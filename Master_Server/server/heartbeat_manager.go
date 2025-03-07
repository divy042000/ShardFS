package server

import (
	"context"
	"log"
	"sync"
	"time"

	pb "master_server/proto"
)

// ChunkServerInfo stores heartbeat data for each Chunk Server
type ChunkServerInfo struct {
	ServerID      string
	FreeSpace     int64
	TotalSpace    int64
	StoredChunks  []string
	LastHeartbeat time.Time
}

// HeartbeatManager tracks active Chunk Servers
type HeartbeatManager struct {
	pb.UnimplementedHeartbeatServiceServer // ✅ Implements gRPC Interface

	mu           sync.Mutex
	chunkServers map[string]*ChunkServerInfo
}

// NewHeartbeatManager initializes a HeartbeatManager
func NewHeartbeatManager() *HeartbeatManager {
	return &HeartbeatManager{
		chunkServers: make(map[string]*ChunkServerInfo),
	}
}

// SendHeartbeat receives heartbeats from Chunk Servers
func (hm *HeartbeatManager) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Update or create a Chunk Server entry
	hm.chunkServers[req.ServerId] = &ChunkServerInfo{
		ServerID:      req.ServerId,
		FreeSpace:     req.FreeSpace,
		TotalSpace:    req.TotalSpace,
		StoredChunks:  req.ChunkIds,
		LastHeartbeat: time.Now(),
	}
 
       log.Printf("💓 Received heartbeat from %s | Free Space: %d MB Total Space : %d MB| Chunks: %d | CPU: %.2f%%",
           req.ServerId, req.FreeSpace, req.TotalSpace,len(req.ChunkIds), req.CpuUsage)

	return &pb.HeartbeatResponse{
		Success: true,
		Message: "✅ Heartbeat received successfully",
	}, nil
}

// RemoveInactiveServers clears out inactive Chunk Servers (no heartbeat in 30s)
func (hm *HeartbeatManager) RemoveInactiveServers() {
	for {
		time.Sleep(10 * time.Second) // Check every 10s

		hm.mu.Lock()
		for id, server := range hm.chunkServers {
			if time.Since(server.LastHeartbeat) > 30*time.Second {
				log.Printf("⚠️  Chunk Server %s is INACTIVE! Removing from active list.", id)
				delete(hm.chunkServers, id)
			}
		}
		hm.mu.Unlock()
	}
}

// GetActiveChunkServers filters out inactive servers
func (hm *HeartbeatManager) GetActiveChunkServers(servers []string) []string {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	var activeServers []string
	for _, serverID := range servers {
		if _, exists := hm.chunkServers[serverID]; exists {
			activeServers = append(activeServers, serverID)
		}
	}
	return activeServers
}

// IsChunkServerActive checks if a chunk server is active
func (hm *HeartbeatManager) IsChunkServerActive(serverID string) bool {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	_, exists := hm.chunkServers[serverID]
	return exists
}
