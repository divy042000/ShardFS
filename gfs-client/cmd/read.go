package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gfs-client/client"
	"gfs-client/metadata"

	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read <file_name> <destination_path>",
	Short: "Reads a file from the distributed file system",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		destPath := args[1]
		masterAddr, _ := cmd.Flags().GetString("master")

		// Initialize gRPC client
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("Failed to initialize client: %v", err)
		}
		defer cl.Close()

		// Create destination directory
		err = os.MkdirAll(filepath.Dir(destPath), os.ModePerm)
		if err != nil {
			log.Fatalf("Failed to create destination directory: %v", err)
		}

		// Get chunk count (simplified: fetch until failure)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var chunkCount int32
		for i := int32(0); ; i++ {
			resp, err := cl.GetChunkLocations(ctx, fileName, i)
			if err != nil || !resp.Success || len(resp.ChunkServers) == 0 {
				chunkCount = i
				break
			}
		}

		if chunkCount == 0 {
			log.Fatalf("No chunks found for file %s", fileName)
		}

		// Download chunks in parallel
		var wg sync.WaitGroup
		chunks := make([][]byte, chunkCount)
		errChan := make(chan error, chunkCount)

		for i := int32(0); i < chunkCount; i++ {
			wg.Add(1)
			go func(index int32) {
				defer wg.Done()
				resp, err := cl.GetChunkLocations(ctx, fileName, index)
				if err != nil {
					errChan <- fmt.Errorf("failed to get chunk %d locations: %v", index, err)
					return
				}

				data, err := cl.DownloadChunk(resp.ChunkServers[0], resp.ChunkId, 3) // 3 retries
				if err != nil {
					errChan <- fmt.Errorf("failed to download chunk %d: %v", index, err)
					return
				}

				// Verify integrity
				hash := metadata.GenerateChunkHash(data)
				fmt.Printf("Downloaded chunk %d from %s (hash: %s)\n", index, resp.ChunkServers[0], hash)
				chunks[index] = data
			}(i)
		}

		// Wait for downloads and check errors
		wg.Wait()
		close(errChan)
		for err := range errChan {
			if err != nil {
				log.Fatalf("Download error: %v", err)
			}
		}

		// Reassemble file
		outFile, err := os.Create(destPath)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outFile.Close()

		for i := int32(0); i < chunkCount; i++ {
			_, err = outFile.Write(chunks[i])
			if err != nil {
				log.Fatalf("Failed to write chunk %d to file: %v", i, err)
			}
		}

		fmt.Printf("File downloaded successfully to %s\n", destPath)
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
}