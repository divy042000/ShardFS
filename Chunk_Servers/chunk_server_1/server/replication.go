package server

import (
	"context"
	"fmt"
	"log"

	pb "chunk_server_1/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReplicationManager struct {
	chunkServer *ChunkServer
}

func NewReplicationManager(cs *ChunkServer) *ReplicationManager {
	return &ReplicationManager{chunkServer: cs}
}

func (rm *ReplicationManager) StartReplication(req *pb.ReplicationRequest, currentIndex int) (*pb.ReplicationResponse, error) {
	log.Printf("🚀 [StartReplication] Starting replication at index %d for chunk '%s' (file '%s')", currentIndex, req.ChunkId, req.FileId)

	// Base case: all followers have been processed
	if currentIndex >= len(req.Followers) {
		log.Printf("✅ [StartReplication] No more followers to replicate chunk '%s'", req.ChunkId)
		return &pb.ReplicationResponse{
			Success:   true,
			Message:   "Replication chain complete",
			StatusMap: map[string]bool{},
		}, nil
	}

	target := req.Followers[currentIndex]
	if target == "" {
		log.Printf("⚠️ [StartReplication] Skipping empty follower address at index %d", currentIndex)
		return rm.StartReplication(req, currentIndex+1)
	}

	log.Printf("📡 [StartReplication] Attempting to connect to follower '%s' (index %d)", target, currentIndex)

	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("❌ [StartReplication] Failed to connect to follower '%s': %v", target, err)
		return nil, fmt.Errorf("failed to connect to follower %s: %v", target, err)
	}
	defer func() {
		_ = conn.Close()
		log.Printf("🔌 [StartReplication] Closed connection to follower '%s'", target)
	}()

	client := pb.NewChunkServiceClient(conn)

	log.Printf("📤 [StartReplication] Sending chunk '%s' to follower '%s'", req.ChunkId, target)
	resp, err := client.SendChunk(context.Background(), req)
	if err != nil {
		log.Printf("❌ [StartReplication] RPC to follower '%s' failed: %v", target, err)
		return nil, fmt.Errorf("error sending chunk to follower %s: %v", target, err)
	}

	log.Printf("📬 [StartReplication] Received response from follower '%s': success=%v, message='%s'", target, resp.Success, resp.Message)
	return resp, nil
}
