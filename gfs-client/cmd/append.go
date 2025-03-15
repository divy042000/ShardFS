package cmd
import(
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	pb "gfs-client/proto"
	"gfs-client/chunking"
	"gfs-client/client"
	"github.com/spf13/cobra"
)

var appendCmd = &cobra.Command{
	Use: "append <file_path>",
	Short: "Appends a file to the distributed file system",
	Args : cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string){
		fileName := args[0]
		dataPath :=args[1]
		masterAddr, _ := cmd.Flags().GetString("master")
		clientID := "client-123"


		// Intialize gRPC client
		cl,err := client.NewClient(masterAddr)
		if err!=nil{
			log.Fatalf("Failed to Inititalize client: %v" ,err)
		}
		defer cl.Close()

		// Chunk the data to append
		chunks , err := chunking.ChunkFile(dataPath)
		if err != nil{
			log.Fatalf("Failed to chunk file: %v", err)
		}

		// Preparing an append request for purpose of appending data
		var chunkSizes []int64
		var chunkHashes []string
		for _, chunk := range chunks{	
			chunkSizes = append(chunkSizes, chunk.Size)
			chunkHashes = append(chunkHashes, chunk.Hash)
		}
		totalSize := int64(0)
		for _, size := range chunkSizes{
			totalSize += size
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := cl.AppendFile(ctx, &pb.AppendFileRequest{
			FileName: fileName,
			TotalSize : totalSize,
			ChunkCount: int32(len(chunks)),
			ChunkSizes: chunkSizes,
			ChunkHashes: chunkHashes,
			ClientId: clientID,
		})
		if err != nil {
			log.Fatalf("failed to append file: %v", err)
		}
		if !resp.Success {
			log.Fatalf("server rejected append operation: %s", resp.Message)
		}

		// Upload new chunks in parallel 
		var wg sync.WaitGroup
		errChan := make(chan error, len(chunks))
		for i, chunk := range chunks {
			wg.Add(1)
			go func(index int, chunk chunking.Chunk) {
				defer wg.Done()
				chunkID := fmt.Sprintf("%s_%d", fileName, index)
				serverAddr, ok := resp.ChunkAssignments[int32(index)]
				if !ok {
					errChan <- fmt.Errorf("no chunk server assigned for chunk %d", index)
					return
				}
				err := cl.UploadChunk(serverAddr, chunkID, chunk.Data, 3) // 3 retries
				if err != nil {
					errChan <- fmt.Errorf("failed to upload chunk %d: %v", index, err)
					return
				}
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


func init(){
	rootCmd.AddCommand(appendCmd)
}


