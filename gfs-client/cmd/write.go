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
		fmt.Printf("Checking file: %s\n", filePath)
		if _, err := os.Stat(filePath); err != nil {
			log.Printf("Failed to stat file: %v", err)
			return
		}
		
		clientID := "gfs-client"
		masterAddr, _ := cmd.Flags().GetString("master") // Get flag first
		if envAddr := os.Getenv("MASTER_ADDRESS"); envAddr != "" {
			masterAddr = envAddr // Override with env if set
		}
		if masterAddr == "" || masterAddr == "localhost:50052" { // Avoid localhost default
			masterAddr = "master_server_container:50052" // Hardcode as last resort
		}

		log.Printf("Master address: %s", masterAddr)
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Printf("Failed to initialize client: %v", err)
			return
		}
		defer cl.Close()

		// Chunk the file
		chunks, err := chunking.ChunkFile(filePath)
		if err != nil {
			log.Printf("Failed to chunk file: %v", err)
			return
		}

		// Prepare metadata
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
		meta := metadata.NewFileMetadata(filePath, totalSize, chunkSizes, chunkHashes, clientID)

		// Register file with master
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := cl.RegisterFile(ctx, meta.ToProto())
		if err != nil {
			log.Printf("Failed to register file: %v", err)
			return
		}

		fmt.Printf("File registered: FileID=%s, LeaderServer=%s\n", resp.FileId, resp.LeaderServer)

		// Upload chunks in parallel
		var wg sync.WaitGroup
		errChan := make(chan error, len(chunks))

		for i, chunk := range chunks {
			wg.Add(1)
			go func(index int, chunk chunking.Chunk) {
				defer wg.Done()
				chunkID := fmt.Sprintf("%s_%d", resp.FileId, index)
				serverAddr, ok := resp.ChunkAssignments[int32(index)]
				if !ok {
					errChan <- fmt.Errorf("no chunk server assigned for chunk %d", index)
					return
				}
				if len(serverAddr.Servers) == 0 {
					errChan <- fmt.Errorf("no servers available for chunk %d", index)
					return
				}
				err := cl.UploadChunk(serverAddr.Servers[0], chunkID, chunk.Data, 3) // 3 retries
				if err != nil {
					errChan <- fmt.Errorf("failed to upload chunk %d: %v", index, err)
					return
				}
				fmt.Printf("Uploaded chunk %d to %s\n", index, serverAddr)
			}(i, chunk)
		}

		// Wait for uploads and check for errors
		wg.Wait()
		close(errChan)
		for err := range errChan {
			if err != nil {
				log.Printf("Upload error: %v", err)
				return
			}
		}

		fmt.Println("File write completed successfully")
	},
}

func init() {
	rootCmd.AddCommand(writeCmd)
}
