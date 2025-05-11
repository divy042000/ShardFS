// cmd/write.go
package cmd

import (
	"context"
	"fmt"
	"gfs-client/chunking"
	"gfs-client/client"
	"gfs-client/metadata"
	"gfs-client/worker"
	"log"
	"os"
	"sync"
	"time"
    pb"gfs-client/proto"
	"github.com/spf13/cobra"
)

var writeCmd = &cobra.Command{
	Use:   "write <file_path>",
	Short: "Writes a file to the distributed file system",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		fmt.Printf("[write.go][DEBUG] Checking file: %s\n", filePath)
		if _, err := os.Stat(filePath); err != nil {
			log.Printf("[write.go][ERROR] Failed to stat file: %v", err)
			return
		}

		clientID := "gfs-client"
		masterAddr, _ := cmd.Flags().GetString("master")
		if envAddr := os.Getenv("MASTER_ADDRESS"); envAddr != "" {
			log.Printf("[write.go][DEBUG] MASTER_ADDRESS env var detected: %s", envAddr)
			masterAddr = envAddr
		}
		if masterAddr == "" || masterAddr == "localhost:50052" {
			log.Printf("[write.go][DEBUG] No master flag or default used, falling back to container address")
			masterAddr = "master_server_container:50052"
		}
		if masterAddr == "" {
			log.Printf("[write.go][ERROR] Master address is not specified or invalid")
			return
		}
		log.Printf("[write.go][INFO] Master address: %s", masterAddr)

		log.Printf("[write.go][DEBUG] Initializing DFS client")
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Printf("[write.go][ERROR] Failed to initialize client: %v", err)
			return
		}
		defer cl.Close()

		// Initialize worker pool
		workerPool := worker.NewWorkerPool(cl, 10) // 10 workers
		defer workerPool.Close()

		log.Printf("[write.go][DEBUG] Chunking file: %s", filePath)
		chunks, err := chunking.ChunkFile(filePath)
		if err != nil {
			log.Printf("[write.go][ERROR] Failed to chunk file: %v", err)
			return
		}
		log.Printf("[write.go][DEBUG] Total chunks created: %d", len(chunks))

		// Prepare metadata
		var chunkSizes []int64
		var chunkHashes []string
		for _, chunk := range chunks {
			chunkSizes = append(chunkSizes, chunk.Size)
			chunkHashes = append(chunkHashes, chunk.Hash)
			log.Printf("[write.go][DEBUG] Chunk hash: %s, size: %d", chunk.Hash, chunk.Size)
		}
		totalSize := int64(0)
		for _, size := range chunkSizes {
			totalSize += size
		}
		log.Printf("[write.go][DEBUG] Total file size: %d bytes", totalSize)

		meta := metadata.NewFileMetadata(filePath, totalSize, chunkSizes, chunkHashes, clientID)
		log.Printf("[write.go][DEBUG] Generated file metadata")

		// Register file with master
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		log.Printf("[write.go][DEBUG] Registering file with master")
		respChan := make(chan *pb.RegisterFileResponse, 1)
		workerPool.Submit(&worker.RegisterFileTask{
			Ctx:        ctx, // Pass the context
			Request:    meta.ToProto(),
			ResultChan: respChan,
		})
		resp := <-respChan
		if resp == nil || !resp.Success {
			log.Printf("❌ [write.go] Failed to register file: %v", err)
			return
		}
		log.Printf("📦 [write.go] File registered: FileID=%s", resp.FileId)

		// Upload chunks using worker pool
		errChan := make(chan error, len(chunks))
		var wg sync.WaitGroup
		for i, chunk := range chunks {
			wg.Add(1)
			go func(index int, chunk chunking.Chunk) {
				defer wg.Done()

				log.Printf("⏳ [write.go] Preparing to upload chunk %d", index)
				chunkServer := resp.ChunkAssignments[int32(index)]
				leaderAddr := chunkServer.Leader
				if leaderAddr == "" {
					errChan <- fmt.Errorf("❌ [write.go] No leader server available for chunk %d", index)
					return
				}
				log.Printf("🎯 [write.go] Leader server for chunk %d: %s", index, leaderAddr)

				var follower1, follower2 string
				if len(chunkServer.Replicas) > 0 {
					follower1 = chunkServer.Replicas[0]
				}
				if len(chunkServer.Replicas) > 1 {
					follower2 = chunkServer.Replicas[1]
				}
				log.Printf("🔄 [write.go] Replication targets for chunk %d: %s, %s", index, follower1, follower2)

				chunkHash := resp.ChunkAssignments[int32(index)].ChunkHash
				if chunkHash == "" {
					errChan <- fmt.Errorf("❌ [write.go] Missing chunk hash for chunk %d", index)
					return
				}
				log.Printf("📋 [write.go] Chunk hash for chunk %d: %s", index, chunkHash)
				chunkIndex := resp.ChunkAssignments[int32(index)].ChunkIndex
				log.Printf("📦 [write.go] Chunk index for chunk %d: %d", index, chunkIndex)

				workerPool.Submit(&worker.UploadChunkTask{
					FileID:           resp.FileId,
					ChunkServerAddr:  leaderAddr,
					ChunkIndex:       chunkIndex,
					Data:             chunk.Data,
					Follower1:        follower1,
					Follower2:        follower2,
					ChunkHash:        chunkHash,
					Retries:          3,
				})
			}(i, chunk)
		}

		// Wait for all uploads and handle errors
		wg.Wait()
		close(errChan)

		hasErrors := false
		for err := range errChan {
			if err != nil {
				log.Printf("❌ [write.go] Upload error: %v", err)
				hasErrors = true
			}
		}

		if hasErrors {
			log.Printf("❌ [write.go] File upload encountered errors")
			return
		}

		log.Printf("🎉 [write.go] File write completed successfully")
	},
}

func init() {
	rootCmd.AddCommand(writeCmd)
}