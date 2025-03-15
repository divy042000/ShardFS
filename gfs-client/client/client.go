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

// Client encapsulates gRPC connections and operations
type Client struct {
	masterConn *grpc.ClientConn
	master     pb.MasterServiceClient
}

// NewClient initializes a new gRPC client
func NewClient(masterAddr string) (*Client, error) {
	// Establish a connection to the master server
	conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}

	return &Client{
		masterConn: conn,
		master:     pb.NewMasterServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *Client) Close() {
	c.masterConn.Close()
}

// RegisterFile registers a file with the master server
func (c *Client) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	resp, err := c.master.RegisterFile(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to register file: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("master rejected file registration: %s", resp.Message)
	}
	return resp, nil
}

// DeleteFile : Deletes a file from the distributed file system by sending a request to the master server.
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

// GetChunkLocations retrieves chunk server locations for a file
func (c *Client) GetChunkLocations(ctx context.Context, fileName string, chunkIndex int32) (*pb.GetChunkResponse, error) {
	resp, err := c.master.GetChunkLocations(ctx, &pb.GetChunkRequest{
		FileName:   fileName,
		ChunkIndex: chunkIndex,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk locations: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("master failed to locate chunk: %s", resp.Message)
	}
	return resp, nil
}

// UploadChunk uploads a chunk to a chunk server with retries
func (c *Client) UploadChunk(chunkServerAddr string, chunkID string, data []byte, retries int) error {
	const maxRetries = 3
	for attempt := 0; attempt <= retries && attempt <= maxRetries; attempt++ {
		err := c.uploadChunkAttempt(chunkServerAddr, chunkID, data)
		if err == nil {
			return nil
		}
		log.Printf("Upload attempt %d/%d failed for chunk %s to %s: %v", attempt+1, maxRetries, chunkID, chunkServerAddr, err)
		time.Sleep(time.Second * time.Duration(attempt+1)) // Exponential backoff
	}
	return fmt.Errorf("failed to upload chunk %s to %s after %d retries", chunkID, chunkServerAddr, maxRetries)
}

func (c *Client) uploadChunkAttempt(chunkServerAddr, chunkID string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Establish a connection to the chunk server
	conn, err := grpc.Dial(chunkServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)
	stream, err := client.UploadChunk(ctx)
	if err != nil {
		return fmt.Errorf("failed to open upload stream: %v", err)
	}

	// Send data in 1MB chunks
	const bufferSize = 1024 * 1024
	for i := 0; i < len(data); i += bufferSize {
		end := i + bufferSize
		if end > len(data) {
			end = len(data)
		}
		err = stream.Send(&pb.ChunkData{
			ChunkId: chunkID,
			Data:    data[i:end],
		})
		if err != nil {
			return fmt.Errorf("failed to send chunk data: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close upload stream: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("chunk upload failed: %s", resp.Message)
	}
	return nil
}

// DownloadChunk downloads a chunk from a chunk server with retries
func (c *Client) DownloadChunk(chunkServerAddr, chunkID string, retries int) ([]byte, error) {
	const maxRetries = 3
	for attempt := 0; attempt <= retries && attempt <= maxRetries; attempt++ {
		data, err := c.downloadChunkAttempt(chunkServerAddr, chunkID)
		if err == nil {
			return data, nil
		}
		log.Printf("Download attempt %d/%d failed for chunk %s from %s: %v", attempt+1, maxRetries, chunkID, chunkServerAddr, err)
		time.Sleep(time.Second * time.Duration(attempt+1)) // Exponential backoff
	}
	return nil, fmt.Errorf("failed to download chunk %s from %s after %d retries", chunkID, chunkServerAddr, maxRetries)
}

func (c *Client) downloadChunkAttempt(chunkServerAddr, chunkID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Establish a connection to the chunk server
	conn, err := grpc.Dial(chunkServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)
	stream, err := client.DownloadChunk(ctx, &pb.ChunkRequest{ChunkId: chunkID})
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
