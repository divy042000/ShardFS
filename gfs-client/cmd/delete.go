// cmd/delete.go
package cmd

import (
	"context"
	"fmt"
	"gfs-client/client"
	pb "gfs-client/proto"
	"gfs-client/worker"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <file_name>",
	Short: "Deletes a file from the distributed file system",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		masterAddr := os.Getenv("MASTER_ADDRESS")
		clientID := "gfs-client"

		log.Printf("📡 Connecting to master at: %s", masterAddr)
		log.Printf("🗑️ Request to delete file: %s", fileName)

		// Initialize gRPC client
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("❌ Unable to connect to master server: %v", err)
		}
		defer cl.Close()

		// Initialize worker pool
		workerPool := worker.NewWorkerPool(cl, 1) // Single worker for simplicity
		defer workerPool.Close()

		// Prepare context
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create the request
		req := &pb.DeleteFileRequest{
			FileName: fileName,
			ClientId: clientID,
		}

		errChan := make(chan error, 1)
		retries := 3
		for i := 0; i < retries; i++ {
			workerPool.Submit(&worker.DeleteFileTask{
				Ctx:     ctx, // Pass the context
				Request: req,
			})

			select {
			case err := <-errChan:
				if err == nil {
					log.Printf("✅ File '%s' deleted successfully.", fileName)
					fmt.Printf("🧹 File '%s' deleted successfully from the system.\n", fileName)
					return
				}
				log.Printf("🔁 Retry %d/%d: Error deleting file: %v", i+1, retries, err)
				time.Sleep(2 * time.Second)
			case <-ctx.Done():
				log.Printf("❌ Deletion timed out")
				fmt.Println("🚫 Could not delete file due to timeout.")
				return
			}
		}

		log.Printf("❌ Failed to delete file after %d retries", retries)
		fmt.Println("🚫 Could not delete file due to network or server error.")
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
