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
	leaderServer *ChunkServer
}

func NewReplicationManager(leaderServer *ChunkServer) *ReplicationManager {
	return &ReplicationManager{leaderServer: leaderServer}
}
func (rm *ReplicationManager) ReplicateChunk(chunkID string, data []byte, followers []string, currentIndex int) error {
	if currentIndex >= len(followers) {
		return nil // All done
	}

	follower := followers[currentIndex]
	if follower == "" {
		return rm.ReplicateChunk(chunkID, data, followers, currentIndex+1)
	}

	conn, err := grpc.Dial(follower, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to follower %s: %v", follower, err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)
	req := &pb.ReplicationRequest{
		ChunkId:   chunkID,
		Data:      data,
		Followers: followers,
	}

	_, err = client.SendChunk(context.Background(), req)
	if err != nil {
		return fmt.Errorf("replication to %s failed: %v", follower, err)
	}

	log.Printf("✅ Replicated chunk %s to %s", chunkID, follower)
	return rm.ReplicateChunk(chunkID, data, followers, currentIndex+1)
}

func (rm *ReplicationManager) sendChunkToFollower(followerAddr, chunkID string, data []byte, version int) {
	conn, err := grpc.Dial(followerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to follower %s: %v", followerAddr, err)
		return
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)
	req := &pb.ReplicationRequest{
		ChunkId:   chunkID,
		Data:      data,
		Followers: []string{},
	}

	resp, err := client.SendChunk(context.Background(), req)
	if err != nil || !resp.Success {
		log.Printf("Replication failed to %s for chunk %s: %v", followerAddr, chunkID, err)
		return
	}
	log.Printf("Successfully replicated chunk %s to follower %s", chunkID, followerAddr)
}
