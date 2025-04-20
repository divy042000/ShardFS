package cmd
import (
     "context"
	 "fmt"
	 "log"
	 "time"
	 "gfs-client/client"
	 "github.com/spf13/cobra"
	 pb "gfs-client/proto"
)
var deleteCmd = &cobra.Command{
	Use:   "delete <file_name>",
	Short: "Deletes a file from the distributed file system",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		masterAddr, _ := cmd.Flags().GetString("master")
		clientID := "gfs-client" // Static client ID for now

		log.Printf("📡 Connecting to master at: %s", masterAddr)
		log.Printf("🗑️  Request to delete file: %s", fileName)

		// Initialize gRPC client
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("❌ Unable to connect to master server: %v", err)
		}
		defer cl.Close()

		// Prepare context
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create the request
		req := &pb.DeleteFileRequest{
			FileName: fileName,
			ClientId: clientID,
		}

		var resp *pb.DeleteFileResponse

		retries := 3
		for i := 0; i < retries; i++ {
			resp, err = cl.DeleteFile(ctx, req)
			if err == nil {
				break
			}
			log.Printf("🔁 Retry %d/%d: Error deleting file: %v", i+1, retries, err)
			time.Sleep(2 * time.Second)
		}
		
		resp, err = cl.DeleteFile(ctx, req)

		if err != nil {
			log.Printf("❌ Failed to communicate with master: %v", err)
			fmt.Println("🚫 Could not delete file due to network or server error.")
			return
		}
		if !resp.Success {
			log.Printf("⚠️ Deletion rejected by master: %s", resp.Message)
			fmt.Printf("⚠️ File '%s' could not be deleted: %s\n", fileName, resp.Message)
			return
		}

		log.Printf("✅ File '%s' deleted successfully.", fileName)
		fmt.Printf("🧹 File '%s' deleted successfully from the system.\n", fileName)
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
