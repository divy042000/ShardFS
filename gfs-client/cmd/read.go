// read.go (CMD)
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

		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("❌ Failed to initialize client: %v", err)
		}
		defer cl.Close()

		err = os.MkdirAll(filepath.Dir(destPath), os.ModePerm)
		if err != nil {
			log.Fatalf("❌ Failed to create destination directory: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		metaResp, err := cl.GetFileMetadata(ctx, fileName, "gfs-client")
		if err != nil {
			log.Fatalf("❌ Failed to retrieve metadata: %v", err)
		}

		chunkCount := metaResp.ChunkCount
		if chunkCount == 0 {
			log.Fatalf("⚠️ No chunks found for file %s", fileName)
		}

		fmt.Printf("📦 File has %d chunks. Format: %s | Total size: %d bytes\n", chunkCount, metaResp.FileFormat, metaResp.TotalSize)

		chunks := make([][]byte, chunkCount)
		errChan := make(chan error, chunkCount)
		var wg sync.WaitGroup
		failureMap := make(map[string]bool)

		for i := int32(0); i < chunkCount; i++ {
			wg.Add(1)
			go func(index int32) {
				defer wg.Done()

				replicas := metaResp.ChunkAssignments[index].Replicas
				chunkHash := metaResp.ChunkAssignments[index].ChunkHash
				chunkIndex := metaResp.ChunkAssignments[index].ChunkIndex
				leader := metaResp.ChunkAssignments[index].Leader
				chunkName := fmt.Sprintf("%s_%d", chunkHash, chunkIndex)

				var success bool

				if !failureMap[leader] {
					log.Printf("📡 [Chunk %d] Trying leader %s...", index, leader)
					data, err := cl.DownloadChunk(leader, chunkHash, chunkIndex, 1)
					if err != nil {
						log.Printf("⚠️ [Chunk %d] Leader %s failed: %v", index, leader, err)
						failureMap[leader] = true
					} else {
						if metadata.GenerateChunkHash(data) == chunkHash {
							log.Printf("✅ [Chunk %d] Leader success", index)
							success = true
							chunks[index] = data
						} else {
							log.Printf("❌ [Chunk %d] Hash mismatch from leader", index)
						}
					}
				}

				if !success {
					for _, replica := range replicas {
						if failureMap[replica] {
							log.Printf("🚫 [Chunk %d] Skipping failed replica %s", index, replica)
							continue
						}
						log.Printf("🔁 [Chunk %d] Trying replica %s...", index, replica)
						data, err := cl.DownloadChunk(replica, chunkHash, chunkIndex, 2)
						if err != nil {
							log.Printf("⚠️ [Chunk %d] Replica %s failed: %v", index, replica, err)
							failureMap[replica] = true
							continue
						}
						if metadata.GenerateChunkHash(data) == chunkHash {
							log.Printf("✅ [Chunk %d] Replica %s success", index, replica)
							chunks[index] = data
							success = true
							break
						} else {
							log.Printf("❌ [Chunk %d] Hash mismatch from replica %s", index, replica)
						}
					}
				}

				if !success {
					errChan <- fmt.Errorf("❌ All attempts failed for chunk %d (%s)", index, chunkName)
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		for err := range errChan {
			if err != nil {
				log.Fatalf("💥 Download failed: %v", err)
			}
		}

		// 🧩 Assemble full file
		outFile, err := os.Create(destPath)
		if err != nil {
			log.Fatalf("❌ Cannot create output file: %v", err)
		}
		defer outFile.Close()

		for i := int32(0); i < chunkCount; i++ {
			_, err := outFile.Write(chunks[i])
			if err != nil {
				log.Fatalf("❌ Write error: %v", err)
			}
		}

		fmt.Printf("🎉 File downloaded to %s\n", destPath)
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
}
