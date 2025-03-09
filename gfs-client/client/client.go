package client
import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "gfs-client/proto"

	"google.golang.org/grpc"
)

// client encapsulates grpc connections and operations
type Client struct{
   masterConn *grpc.ClientConn
   master  pb.MasterServiceClient
}

// New Client initializes a new gRPC client

func NewClient(masterAddr string) (*Client,error){
   conn , err := grpc.Dial(masterAddr,grpc.WithInsecure())
   if(err!=nil){
      return nil , fmt.Errorf("failed to connect to master server : %v" , err)
   }
   return &Client{
      masterConn:conn,
      master: pb.NewMasterServiceClient(conn),
   },nil
}

// close closes the grpc connection 

func (c *Client) Close(){
    c.masterConn.Close()
}

// register file registers a file with master server 

func (c *Client) RegisterFile(ctx context.Context , req *pb.RegisterFileRequest) (*pb.RegisterFileResponse,error) {
    resp , err  := c.masterRegisterFile(ctx,req)
        if err != nil {
		return nil, fmt.Errorf("failed to register file: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("master rejected file registration: %s", resp.Message)
	}
    return resp,nil
}

//  Getchunklocations retrieves chunk server locations for a file 

func (c* Client) GetChunkLocations(ctx context.Context , fileName string chunkIndex int32) (*pb.GetChunkRespons , error) {
    resp , err := c.master.GetChunkLocations(ctx , &pb.GetChunkRequest{
       FileName: fileName,
       ChunkIndex : chunkIndex,
       })
       if err != nil {
		return nil, fmt.Errorf("failed to get chunk locations: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("master failed to locate chunk: %s", resp.Message)
	}
}

func (c *Client) UploadChunk(chunkServerAddr , chunkID string , data byte , retries int) error {
   const maxRetries = 3
   for attempt := 0;attempt<=retries && attempt<=maxRetries; attempt++ {
   err := c.uploadChunkAttempt(chunkServerAddr, chunkID, data)
   if err == nil {
	return nil
    }
   log.Printf("Upload attempt %d/%d failed for chunk %s to %s: %v", attempt+1, maxRetries, chunkID, chunkServerAddr, err)
   time.Sleep(time.Second * time.Duration(attempt+1))
   }
  return fmt.Errorf("failed to upload chunk %s to %s after %d retries", chunkID, chunkServerAddr, maxRetries)
}

func (c *Client) uploadChunkAttempt(chunkServerAddr, chunkID string, data []byte) error {
   conn, err := grpc.Dial(chunkServerAddr, grpc.WithInsecure())
   if err != nil {
   return fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
   }
   defer conn.Close()
   client := pb.NewChunkServiceClient(conn)
   ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
   defer cancel()
   stream, err := client.UploadChunk(ctx)
   if err != nil {
		return fmt.Errorf("failed to open upload stream: %v", err)
   }
   const bufferSize = 1024 * 1024
   for i := 0; i < len(data); i += bufferSize {
		end := i + bufferSize
		if end > len(data) {
			end = len(data)
   }
