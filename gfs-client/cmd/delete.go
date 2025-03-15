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
	Use:  "delete <file_name>",
	Short: "Deletes a file from the distributed file system",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		masterAddr, _ := cmd.Flags().GetString("master")
        clientID := "client-123" // Hardcoded for now

		// Initialize gRPC client
		cl, err := client.NewClient(masterAddr)
		if err != nil {
			log.Fatalf("Failed to initialize client: %v", err)
		}
		defer cl.Close()

		// Prepare delete request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
        
		// Assuming Delete File is added to client/client.go
    
		resp, err := cl.DeleteFile(ctx, &pb.DeleteFileRequest{
			FileName: fileName,
			ClientId: clientID,
		})
		
		if err != nil {
			log.Fatalf("Failed to delete file: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Failed to delete file: %s", resp.Message)
		}
		fmt.Printf("File %s deleted successfully\n", fileName)
	},
}

func init(){
	rootCmd.AddCommand(deleteCmd)
}