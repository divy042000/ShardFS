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

// Close closes the gRPC connection
func (c *Client) Close() {
	c.masterConn.Close()
}

// RegisterFile registers a file with the master server
func (c *Client) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	fmt.Printf("Registering file %s with master\n", req.FileName)
	resp, err := c.master.RegisterFile(ctx, req)
	fmt.Println("Registering approach")
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


func (c *Client) UploadChunk(FileId string,chunkServerAddr string, chunkID string, data []byte, follower1, follower2 string, retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Printf("🔍 Starting upload attempt for chunk %s to %s with %d retries", chunkID, chunkServerAddr, retries)
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		log.Printf("🔄 Attempt %d/%d to upload chunk %s to %s", attempt+1, retries, chunkID, chunkServerAddr)

		conn, err := grpc.Dial(chunkServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("❌ Connection failed to chunk server %s: %v", chunkServerAddr, err)
			lastErr = fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
			time.Sleep(time.Duration(attempt+1) * time.Second) 
			continue
		}
		defer func() {
			log.Printf("🔌 Closing connection to chunk server %s", chunkServerAddr)
			conn.Close()
		}()

		client := pb.NewChunkServiceClient(conn)
		log.Printf("📡 Opening upload stream for chunk %s to %s", chunkID, chunkServerAddr)
		stream, err := client.UploadChunk(ctx)
		if err != nil {
			log.Printf("❌ Failed to open upload stream for chunk %s: %v", chunkID, err)
			lastErr = fmt.Errorf("failed to open upload stream: %v", err)
			time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
			continue
		}

		req := &pb.ChunkUploadRequest{
			FileId: FileId,
			ChunkId:   chunkID,
			Data:      data,
			Leader:    chunkServerAddr,
			Follower1: follower1,
			Follower2: follower2,
		}
		log.Printf("📤 Sending chunk %s data to %s", chunkID, chunkServerAddr)
		if err := stream.Send(req); err != nil {
			log.Printf("❌ Failed to send chunk %s to %s: %v", chunkID, chunkServerAddr, err)
			lastErr = fmt.Errorf("failed to send chunk: %v", err)
			time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
			continue
		}

		log.Printf("📨 Receiving response for chunk %s from %s", chunkID, chunkServerAddr)
		resp, err := stream.CloseAndRecv()
		if err != nil {
			log.Printf("❌ Failed to receive response for chunk %s from %s: %v", chunkID, chunkServerAddr, err)
			lastErr = fmt.Errorf("failed to receive response: %v", err)
			time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
			continue
		}
		if !resp.Success {
			log.Printf("⚠️ Upload failed for chunk %s to %s: %s", chunkID, chunkServerAddr, resp.Message)
			lastErr = fmt.Errorf("upload failed: %s", resp.Message)
			time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
			continue
		}

		log.Printf("✅ Chunk %s uploaded successfully to %s", chunkID, chunkServerAddr)
		return nil
	}

	log.Printf("❌ All %d upload attempts failed for chunk %s to %s: %v", retries, chunkID, chunkServerAddr, lastErr)
	return fmt.Errorf("all %d upload attempts failed for chunk %s: %v", retries, chunkID, lastErr)
}

func (c *Client) uploadChunkAttempt(chunkServerAddr, chunkID string, data []byte, follower1, follower2 string, retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("🔍 Starting upload attempt for chunk %s to %s", chunkID, chunkServerAddr)
	conn, err := grpc.Dial(chunkServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("❌ Connection failed to chunk server %s: %v", chunkServerAddr, err)
		return fmt.Errorf("failed to connect to chunk server %s: %v", chunkServerAddr, err)
	}
	defer func() {
		log.Printf("🔌 Closing connection to chunk server %s", chunkServerAddr)
		conn.Close()
	}()

	client := pb.NewChunkServiceClient(conn)
	log.Printf("📡 Opening upload stream for chunk %s to %s", chunkID, chunkServerAddr)
	stream, err := client.UploadChunk(ctx)
	if err != nil {
		log.Printf("❌ Failed to open upload stream for chunk %s: %v", chunkID, err)
		return fmt.Errorf("failed to open upload stream: %v", err)
	}

	const bufferSize = 1024 * 1024 // 1MB
	totalChunks := (len(data) + bufferSize - 1) / bufferSize
	log.Printf("📦 Starting to send chunk %s in %d parts to %s", chunkID, totalChunks, chunkServerAddr)

	for i := 0; i < len(data); i += bufferSize {
		end := i + bufferSize
		if end > len(data) {
			end = len(data)
		}
		partNumber := (i / bufferSize) + 1
		log.Printf("📤 Sending part %d/%d for chunk %s to %s", partNumber, totalChunks, chunkID, chunkServerAddr)
		req := &pb.ChunkUploadRequest{
			ChunkId:   chunkID,
			Data:      data[i:end],
			Leader:    chunkServerAddr,
			Follower1: follower1,
			Follower2: follower2,
		}
		if err := stream.Send(req); err != nil {
			log.Printf("❌ Failed to send part %d for chunk %s to %s: %v", partNumber, chunkID, chunkServerAddr, err)
			return fmt.Errorf("failed to send chunk data: %v", err)
		}
		log.Printf("✅ Part %d/%d sent successfully for chunk %s", partNumber, totalChunks, chunkID)
	}

	log.Printf("📨 Finalizing upload for chunk %s to %s", chunkID, chunkServerAddr)
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Printf("❌ Failed to receive response for chunk %s from %s: %v", chunkID, chunkServerAddr, err)
		return fmt.Errorf("failed to close upload stream: %v", err)
	}
	if !resp.Success {
		log.Printf("⚠️ Chunk upload failed for %s to %s: %s", chunkID, chunkServerAddr, resp.Message)
		return fmt.Errorf("chunk upload failed: %s", resp.Message)
	}

	log.Printf("✅ Chunk %s uploaded successfully to %s", chunkID, chunkServerAddr)
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
