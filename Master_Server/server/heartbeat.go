package server

import (
	"context"
	"log"
	"sync"
	"time"

	pb "master_server/proto"
)

// HeartbeatManager tracks active chunk servers
type HeartbeatManager struct {
	mu           sync.Mutex
	chunkServers map[string]time.Time // Stores last heartbeat time
}

// NewHeartbeatManager initializes a heartbeat tracker
func NewHeartbeatManager() *HeartbeatManager {
	return &HeartbeatManager{
		chunkServers: make(map[string]time.Time),
	}
}

// SendHeartbeat updates chunk server status
func (hm *HeartbeatManager) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.chunkServers[req.ServerId] = time.Now()
	log.Printf("Received heartbeat from %s | Free Space: %d MB", req.ServerId, req.FreeSpace)

	return &pb.HeartbeatResponse{Success: true, Message: "Heartbeat received"}, nil
}

