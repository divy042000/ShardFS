// cmd/append.go
package cmd

import (
	"context"
	"fmt"
	"gfs-client/chunking"
	"gfs-client/client"
	"gfs-client/worker"
	"log"
	"sync"
	"time"

	pb "gfs-client/proto"

	"github.com/spf13/cobra"
)

var appendCmd = &cobra.Command{
	Use:   "append <file_name> <data_path>",
	Short: "Appends a file to the distributed file system",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		dataPath := args[1]
		masterAddr, _ := cmd.Flags().GetString("master")
		clientID := "client-123"

		// Initialize gRPC client
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("Failed to initialize client: %v", err)
		}
		defer cl.Close()

		// Initialize worker pool
		workerPool := worker.NewWorkerPool(cl, 10) // 10 workers
		defer workerPool.Close()

		// Chunk the data to append
		chunks, err := chunking.ChunkFile(dataPath)
		if err != nil {
			log.Fatalf("Failed to chunk file: %v", err)
		}

		// Prepare an append request
		var chunkSizes []int64
		var chunkHashes []string
		for _, chunk := range chunks {
			chunkSizes = append(chunkSizes, chunk.Size)
			chunkHashes = append(chunkHashes, chunk.Hash)
		}
		totalSize := int64(0)
		for _, size := range chunkSizes {
			totalSize += size
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		respChan := make(chan *pb.AppendFileResponse, 1)
		workerPool.Submit(&worker.AppendFileTask{
			Ctx: ctx, // Pass the context
			Request: &pb.AppendFileRequest{
				FileName:    fileName,
				TotalSize:   totalSize,
				ChunkCount:  int32(len(chunks)),
				ChunkSizes:  chunkSizes,
				ChunkHashes: chunkHashes,
				ClientId:    clientID,
			},
			ResultChan: respChan,
		})
		resp := <-respChan
		if resp == nil || !resp.Success {
			log.Fatalf("failed to append file: %s", resp.Message)
		}

		// Upload new chunks in parallel
		var wg sync.WaitGroup
		errChan := make(chan error, len(chunks))
		for i, chunk := range chunks {
			wg.Add(1)
			go func(index int, chunk chunking.Chunk) {
				defer wg.Done()
				serverAddr, ok := resp.ChunkAssignments[int32(index)]
				if !ok {
					errChan <- fmt.Errorf("no chunk server assigned for chunk %d", index)
					return
				}
				workerPool.Submit(&worker.UploadChunkTask{
					FileID:           resp.FileId,
					ChunkServerAddr:  serverAddr,
					ChunkIndex:       int32(index),
					Data:             chunk.Data,
					Follower1:        "",
					Follower2:        "",
					ChunkHash:        chunk.Hash,
					Retries:          3,
				})
				fmt.Printf("Uploaded chunk %d to %s\n", index, serverAddr)
			}(i, chunk)
		}
		wg.Wait()
		close(errChan)

		for err := range errChan {
			if err != nil {
				log.Fatalf("Error occurred during chunk upload: %v", err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(appendCmd)
}