package server

import (
	"context"
	"log"
	"path/filepath"
	"os"

	pb "chunk_server_1/proto"
)

// ReplicateChunk writes a received chunk to disk
func ReplicateChunk(chunkID string, data []byte, version int) error {
	chunkPath := filepath.Join("/data", chunkID+".chunk")
	err := os.WriteFile(chunkPath, data, 0644)
	if err != nil {
		log.Printf("Replication error: %v", err)
		return err
	}
	log.Printf("Chunk %s replicated successfully", chunkID)
	return nil
}

// SendChunk now submits the replication request to the Worker Pool
func (s *ChunkServer) SendChunk(ctx context.Context, req *pb.ReplicationRequest) (*pb.ReplicationResponse, error) {
	responseChan := make(chan JobResult, 1)

	job := Job{
		Type:     ReplicationJob,
		ChunkID:  req.ChunkId,
		Data:     req.Data,
		Version:  int(req.Version),
		Response: responseChan,
	}

	s.workerPool.SubmitJob(job)

	result := <-responseChan
	if !result.Success {
		log.Printf("SendChunk error: %s", result.Message)
		return &pb.ReplicationResponse{Success: false, Message: result.Message}, nil
	}

	log.Printf("Chunk %s replicated successfully", req.ChunkId)
	return &pb.ReplicationResponse{Success: true, Message: "Chunk replicated"}, nil
}
