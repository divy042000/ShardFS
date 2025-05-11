// worker/task.go
package worker

import (
	"context"
	"gfs-client/client"
	pb "gfs-client/proto"
	"log"
)

// Task represents a generic task to be executed by the worker pool.
type Task interface {
	Execute(ctx context.Context, cl *client.Client) error
}

// UploadChunkTask represents a task to upload a chunk.
type UploadChunkTask struct {
	FileID          string
	ChunkServerAddr string
	ChunkIndex      int32
	Data            []byte
	Follower1       string
	Follower2       string
	ChunkHash       string
	Retries         int
}

func (t *UploadChunkTask) Execute(ctx context.Context, cl *client.Client) error {
	return cl.UploadChunk(t.FileID, t.ChunkServerAddr, t.ChunkIndex, t.Data, t.Follower1, t.Follower2, t.ChunkHash, t.Retries)
}

// DownloadChunkTask represents a task to download a chunk.
type DownloadChunkTask struct {
	ChunkServerAddr string
	ChunkHash       string
	ChunkIndex      int32
	Retries         int
	ResultChan      chan<- []byte
}

func (t *DownloadChunkTask) Execute(ctx context.Context, cl *client.Client) error {
	log.Printf("🚀 [DownloadTask] Starting chunk download | ChunkHash: %s | ChunkIndex: %d | Server: %s | Retries: %d",
		t.ChunkHash, t.ChunkIndex, t.ChunkServerAddr, t.Retries)

	data, err := cl.DownloadChunk(t.ChunkServerAddr, t.ChunkHash, t.ChunkIndex, t.Retries)
	if err != nil {
		log.Printf("❌ [DownloadTask] Failed to download chunk | ChunkHash: %s | ChunkIndex: %d | Server: %s | Error: %v",
			t.ChunkHash, t.ChunkIndex, t.ChunkServerAddr, err)
		return err
	}

	log.Printf("✅ [DownloadTask] Successfully downloaded chunk | ChunkHash: %s | ChunkIndex: %d | Bytes: %d",
		t.ChunkHash, t.ChunkIndex, len(data))

	t.ResultChan <- data
	log.Printf("📬 [DownloadTask] Result sent to channel for chunk | ChunkHash: %s | ChunkIndex: %d", t.ChunkHash, t.ChunkIndex)

	return nil
}

// RegisterFileTask represents a task to register a file.
type RegisterFileTask struct {
	Ctx        context.Context // Add context field
	Request    *pb.RegisterFileRequest
	ResultChan chan<- *pb.RegisterFileResponse
}

func (t *RegisterFileTask) Execute(ctx context.Context, cl *client.Client) error {
	resp, err := cl.RegisterFile(t.Ctx, t.Request) // Use task's context
	if err != nil {
		return err
	}
	t.ResultChan <- resp
	return nil
}

// AppendFileTask represents a task to append a file.
type AppendFileTask struct {
	Ctx        context.Context // Add context field
	Request    *pb.AppendFileRequest
	ResultChan chan<- *pb.AppendFileResponse
}

func (t *AppendFileTask) Execute(ctx context.Context, cl *client.Client) error {
	resp, err := cl.AppendFile(t.Ctx, t.Request) // Use task's context
	if err != nil {
		return err
	}
	t.ResultChan <- resp
	return nil
}

// DeleteFileTask represents a task to delete a file.
type DeleteFileTask struct {
	Ctx     context.Context // Add context field
	Request *pb.DeleteFileRequest
}

func (t *DeleteFileTask) Execute(ctx context.Context, cl *client.Client) error {
	_, err := cl.DeleteFile(t.Ctx, t.Request) // Use task's context
	return err
}
