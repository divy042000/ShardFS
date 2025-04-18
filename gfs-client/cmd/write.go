package cmd

import (
	"context"
	"fmt"
	"gfs-client/chunking"
	"gfs-client/client"
	"gfs-client/metadata"
	"log"
	"os"
	"sync"
	"time"

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
		resp, err := cl.RegisterFile(ctx, meta.ToProto())
		if err != nil {
			log.Printf("[write.go][ERROR] Failed to register file: %v", err)
			return
		}

		fmt.Printf("[write.go][INFO] File registered: FileID=%s\n", resp.FileId)

		// Upload chunks in parallel
		var wg sync.WaitGroup
		errChan := make(chan error, len(chunks))

		for i, chunk := range chunks {
			wg.Add(1)
			go func(index int, chunk chunking.Chunk) {
				defer wg.Done()
				log.Printf("[write.go][DEBUG] Preparing to upload chunk %d", index)

				chunkID := fmt.Sprintf("%v", resp.ChunkAssignments[int32(index)])
				log.Printf("[write.go][DEBUG] Assigned chunkID: %s", chunkID)

				leaderServers, ok := resp.ChunkAssignments[int32(index)]
				if !ok {
					errChan <- fmt.Errorf("no chunk server assigned for chunk %d", index)
					log.Printf("[write.go][ERROR] No chunk server assigned for chunk %d", index)
					return
				}
				if len(leaderServers.Servers) == 0 {
					errChan <- fmt.Errorf("no leader server available for chunk %d", index)
					log.Printf("[write.go][ERROR] Leader server list is empty for chunk %d", index)
					return
				}
				leaderAddr := leaderServers.Servers[0]
				log.Printf("[write.go][DEBUG] Leader server for chunk %d: %s", index, leaderAddr)

				// Get replica servers
				var follower1, follower2 string
				if replicas, ok := resp.ReplicationMap[int32(index)]; ok {
					if len(replicas.Servers) > 0 {
						follower1 = replicas.Servers[0]
					}
					if len(replicas.Servers) > 1 {
						follower2 = replicas.Servers[1]
					}
					log.Printf("[write.go][DEBUG] Replication targets for chunk %d: %s, %s", index, follower1, follower2)
				} else {
					log.Printf("[write.go][DEBUG] No replication info for chunk %d", index)
				}

				log.Printf("[write.go][DEBUG] Uploading chunk %d to leader %s", index, leaderAddr)
				err := cl.UploadChunk(resp.FileId, leaderAddr, chunkID, chunk.Data, follower1, follower2, 3)
				if err != nil {
					errChan <- fmt.Errorf("failed to upload chunk %d: %v", index, err)
					log.Printf("[write.go][ERROR] Chunk %d upload failed: %v", index, err)
					return
				}

				log.Printf("[write.go][INFO] Successfully uploaded chunk %d to leader %s", index, leaderAddr)
			}(i, chunk)
		}

		// Wait for uploads and check for errors
		wg.Wait()
		close(errChan)
		hasErrors := false
		for err := range errChan {
			if err != nil {
				log.Printf("[write.go][ERROR] Upload error: %v", err)
				hasErrors = true
			}
		}
		if hasErrors {
			log.Printf("[write.go][ERROR] File upload encountered one or more errors")
			return
		}

		fmt.Println("[write.go][INFO] File write completed successfully")
	},
}

func init() {
	rootCmd.AddCommand(writeCmd)
}
