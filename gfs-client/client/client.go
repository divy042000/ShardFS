package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "gfs-client/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	masterConn *grpc.ClientConn
	master     pb.MasterServiceClient
}


func NewClient(masterAddr string) (*Client, error) {
	fmt.Printf("Connecting to master server at %s\n", masterAddr)
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master server: %v", err)
	}
	return &Client{
		masterConn: conn,
		master:     pb.NewMasterServiceClient(conn),
	}, nil
}

func (c *Client) Close() {
	c.masterConn.Close()
}


func (c *Client) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	fmt.Printf("Registering file %s with master\n", req.FileName)
	resp, err := c.master.RegisterFile(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to register file: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("master rejected file registration: %s", resp.Message)
	}
	return resp, nil
}



func (c *Client) GetFileMetadata(ctx context.Context, fileName, clientID string) (*pb.GetFileMetadataResponse, error) {
	req := &pb.GetFileMetadataRequest{
		FileName: fileName,
		ClientId: clientID,
	}

	resp, err := c.master.GetFileMetadata(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get file metadata: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("master responded with error: %s", resp.Message)
	}
	return resp, nil
}







func (c *Client) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	resp, err := c.master.DeleteFile(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to delete file: %v", err)
	}
	return resp, nil
}

// Append File : Appends a file to the distributed file system by sending a request to the master server.
func (c *Client) AppendFile(ctx context.Context, req *pb.AppendFileRequest) (*pb.AppendFileResponse, error) {
	resp, err := c.master.AppendFile(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to append file: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("server rejected append operation: %s", resp.Message)
	}
	return resp, nil
}


func (c *Client) UploadChunk(
	FileId string,
	chunkServerAddr string,
	chunk_index int32,
	data []byte,
	follower1, follower2 string,
	chunkHash string,
	retries int,
) error {
	log.Printf("🔍 Starting upload for chunk %s to %s with up to %d retries", chunkHash, chunkServerAddr, retries)

	var lastErr error

	for attempt := 0; attempt < retries; attempt++ {
		log.Printf("🔄 Attempt %d/%d to upload chunk %s to %s", attempt+1, retries, chunkHash, chunkServerAddr)

		err := c.uploadChunkOnce(FileId, chunkHash, chunk_index,chunkServerAddr, data, follower1, follower2)
		if err == nil {
			log.Printf("✅ Chunk %s uploaded successfully to %s", chunkHash, chunkServerAddr)
			return nil
		}

		log.Printf("⚠️ Attempt %d failed for chunk %s: %v", attempt+1, chunkHash, err)
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
	}

	log.Printf("❌ All %d attempts failed for chunk %s to %s", retries, chunkHash, chunkServerAddr)
	return fmt.Errorf("upload failed after %d retries for chunk %s: %v", retries, chunkHash, lastErr)
}

func (c *Client) uploadChunkOnce(
	FileId string,
	chunkHash string,
	chunk_index int32,
	chunkServerAddr string,
	data []byte,
	follower1, follower2 string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.Dial(chunkServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)
	stream, err := client.UploadChunk(ctx)
	if err != nil {
		return fmt.Errorf("failed to open upload stream to %s: %v", chunkServerAddr, err)
	}

	const bufferSize = 1024 * 1024 // 1MB
	// totalParts := (len(data) + bufferSize - 1) / bufferSize

	for i := 0; i < len(data); i += bufferSize {
		end := i + bufferSize
		if end > len(data) {
			end = len(data)
		}
		part := data[i:end]

		req := &pb.ChunkUploadRequest{
			FileId:    FileId,
			ChunkHash: chunkHash,
			ChunkIndex: chunk_index,
			Data:      part,
			Leader:    chunkServerAddr,
			Follower1: follower1,
			Follower2: follower2,
		}

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("failed to send chunk part to %s: %v", chunkServerAddr, err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream and receive response: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("chunk upload rejected: %s", resp.Message)
	}

	return nil
}

// client/download.go
func (c *Client) DownloadChunk(chunkServerAddr, chunkHash string, chunkIndex int32, retries int) ([]byte, error) {
	const maxRetries = 3
	chunkID := fmt.Sprintf("%s_%d", chunkHash, chunkIndex)

	for attempt := 0; attempt <= retries && attempt <= maxRetries; attempt++ {
		data, err := c.downloadChunkAttempt(chunkServerAddr, chunkHash, chunkIndex)
		if err == nil {
			return data, nil
		}
		log.Printf("⚠️ Download attempt %d/%d failed for chunk %s from %s: %v", attempt+1, maxRetries, chunkID, chunkServerAddr, err)
		time.Sleep(time.Second * time.Duration(attempt+1))
	}
	return nil, fmt.Errorf("❌ Failed to download chunk %s from %s after retries", chunkID, chunkServerAddr)
}


func (c *Client) downloadChunkAttempt(chunkServerAddr, chunkHash string, chunkIndex int32) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.Dial(chunkServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)
	stream, err := client.DownloadChunk(ctx, &pb.ChunkRequest{
		ChunkHash:  chunkHash,
		ChunkIndex: chunkIndex,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open download stream: %v", err)
	}

	var data []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive chunk data: %v", err)
		}
		data = append(data, chunk.Data...)
	}
	return data, nil
}
