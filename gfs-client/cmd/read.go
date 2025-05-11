package cmd

import (
	"context"
	"fmt"
	"gfs-client/client"
	"gfs-client/worker"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read <file_name> <destination_path>",
	Short: "Reads a file from the distributed file system",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		destPath := args[1]

		masterAddr := os.Getenv("MASTER_ADDRESS")
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("❌ Failed to initialize client: %v", err)
		}
		defer cl.Close()

		// Initialize worker pool
		workerPool := worker.NewWorkerPool(cl, 10)
		defer workerPool.Close()

		err = os.MkdirAll(filepath.Dir(destPath), os.ModePerm)
		if err != nil {
			log.Fatalf("❌ Failed to create destination directory: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		log.Printf("[read.go][DEBUG] Initializing DFS client")
		metaResp, err := cl.GetFileMetadata(ctx, fileName, "gfs-client")
		if err != nil {
			log.Fatalf("❌ Failed to retrieve metadata: %v", err)
		}

		log.Printf("📥 Received File Metadata for: %s", fileName)
		log.Printf("👤 Client ID: %s", metaResp.ClientId)
		log.Printf("📝 Format: %s", metaResp.FileFormat)
		log.Printf("📦 Chunk Count: %d", metaResp.ChunkCount)
		log.Printf("📐 Total Size: %d bytes", metaResp.TotalSize)

		chunkCount := metaResp.ChunkCount
		if chunkCount == 0 {
			log.Fatalf("⚠️ No chunks found for file %s", fileName)
		}

		chunks := make([][]byte, chunkCount)
		errChan := make(chan error, chunkCount)
		dataChan := make(chan struct {
			index int32
			data  []byte
		}, chunkCount)

		var wg sync.WaitGroup
		failureMap := sync.Map{}

		for i := int32(0); i < chunkCount; i++ {
			wg.Add(1)
			go func(index int32) {
				defer wg.Done()

				assignment := metaResp.ChunkAssignments[index]
				chunkIndex := assignment.ChunkIndex
				chunkHash := metaResp.ChunkHashes[chunkIndex]
				leader := assignment.Leader
				replicas := assignment.Replicas

				log.Printf("📦 [Chunk %d] Assignment Info:", index)
				log.Printf("    ├─ Chunk Index   : %d", chunkIndex)
				log.Printf("    ├─ Chunk Hash    : %s", chunkHash)
				log.Printf("    ├─ Leader Server : %s", leader)
				if len(replicas) > 0 {
					log.Printf("    └─ Replicas      : %v", replicas)
				} else {
					log.Printf("    └─ Replicas      : (none)")
				}

				var success bool

				// Try leader first
				if _, failed := failureMap.Load(leader); !failed {
					log.Printf("📡 [Chunk %d] Attempting to download from leader: %s", index, leader)
					resultChan := make(chan []byte, 1)

					workerPool.Submit(&worker.DownloadChunkTask{
						ChunkServerAddr: leader,
						ChunkHash:       chunkHash,
						ChunkIndex:      chunkIndex,
						Retries:         1,
						ResultChan:      resultChan,
					})

					data := <-resultChan
					if data != nil {
						log.Printf("✅ [Chunk %d] Successfully downloaded from leader: %s | Size: %d bytes", index, leader, len(data))
						dataChan <- struct {
							index int32
							data  []byte
						}{index, data}
						success = true
					} else {
						log.Printf("❌ [Chunk %d] Leader download failed: %s. Marking as failed.", index, leader)
						failureMap.Store(leader, true)
					}
				} else {
					log.Printf("⚠️ [Chunk %d] Leader %s already marked as failed. Skipping...", index, leader)
				}

				// Try replicas if leader failed
				if !success {
					log.Printf("🔄 [Chunk %d] Attempting download from replicas...", index)
					for _, replica := range replicas {
						if _, failed := failureMap.Load(replica); failed {
							log.Printf("🚫 [Chunk %d] Skipping failed replica: %s", index, replica)
							continue
						}

						log.Printf("🔁 [Chunk %d] Attempting to download from replica: %s", index, replica)
						resultChan := make(chan []byte, 1)

						workerPool.Submit(&worker.DownloadChunkTask{
							ChunkServerAddr: replica,
							ChunkHash:       chunkHash,
							ChunkIndex:      chunkIndex,
							Retries:         2,
							ResultChan:      resultChan,
						})

						data := <-resultChan
						if data != nil {
							log.Printf("✅ [Chunk %d] Successfully downloaded from replica: %s | Size: %d bytes", index, replica, len(data))
							dataChan <- struct {
								index int32
								data  []byte
							}{index, data}
							success = true
							break
						} else {
							log.Printf("❌ [Chunk %d] Replica download failed: %s. Marking as failed.", index, replica)
							failureMap.Store(replica, true)
						}
					}

					if !success {
						log.Printf("🚨 [Chunk %d] All attempts failed. Could not download chunk.", index)
					}
				}
			}(i)
		}

		// Collect results and errors
		go func() {
			wg.Wait()
			close(dataChan)
			close(errChan)
		}()

		for result := range dataChan {
			chunks[result.index] = result.data
		}

		for err := range errChan {
			if err != nil {
				log.Fatalf("💥 Download failed: %v", err)
			}
		}

		// Assemble the full file
		outFile, err := os.Create(destPath)
		if err != nil {
			log.Fatalf("❌ Cannot create output file: %v", err)
		}
		defer outFile.Close()

		for i := int32(0); i < chunkCount; i++ {
			if chunks[i] == nil {
				log.Fatalf("❌ Missing chunk %d", i)
			}
			_, err := outFile.Write(chunks[i])
			if err != nil {
				log.Fatalf("❌ Write error: %v", err)
			}
		}

		fmt.Printf("🎉 File successfully downloaded to %s\n", destPath)
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
}
