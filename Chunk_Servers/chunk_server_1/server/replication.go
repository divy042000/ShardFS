package server

import (
    "context"
    "log"
    pb "chunk_server_1/proto"
    "google.golang.org/grpc"
)

type ReplicationManager struct {
    leaderServer *ChunkServer
}

func NewReplicationManager(leaderServer *ChunkServer) *ReplicationManager {
    return &ReplicationManager{leaderServer: leaderServer}
}

func (rm *ReplicationManager) ReplicateChunk(chunkID string, data []byte, version int, followers []string) {
    for _, follower := range followers {
        go rm.sendChunkToFollower(follower, chunkID, data, version)
    }
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
        Version:   int32(version),
        Followers: []string{}, // Empty since this is the last in chain
    }
    
    resp, err := client.SendChunk(context.Background(), req)
    if err != nil || !resp.Success {
        log.Printf("Replication failed to %s for chunk %s: %v", followerAddr, chunkID, err)
        return
    }
    log.Printf("Successfully replicated chunk %s to follower %s", chunkID, followerAddr)
}
