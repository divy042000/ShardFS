package server

import (
	"context"
	"log"
	"net"
	"sync"

	pb "master_server/proto"
	"google.golang.org/grpc"
)

// MasterServer manages file metadata, chunk locations & heartbeats
type MasterServer struct {
	pb.UnimplementedMasterServiceServer
	mu               sync.Mutex
	chunkTable       map[string][]string // Mapping chunk_id → chunk servers
	heartbeatManager *HeartbeatManager   // ✅ Manages active chunk servers
}

// NewMasterServer initializes the Master Server
func NewMasterServer() *MasterServer {
	return &MasterServer{
		chunkTable:       make(map[string][]string),
		heartbeatManager: NewHeartbeatManager(), // ✅ Initialize HeartbeatManager
	}
}

// Start initializes gRPC services and listens for client & chunk server requests
func (ms *MasterServer) Start() {
	listener, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen on port 50052: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServiceServer(grpcServer, ms)
	pb.RegisterHeartbeatServiceServer(grpcServer, ms.heartbeatManager) // ✅ Register Heartbeat Service

	// Start routine to remove inactive servers
	go ms.heartbeatManager.RemoveInactiveServers()

	log.Println("🚀 Master Server running on port 50052")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// GetChunkLocations provides chunk locations for a requested file
func (m *MasterServer) GetChunkLocations(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	chunkID := req.FileName + "_" + string(req.ChunkIndex)
	servers, exists := m.chunkTable[chunkID]
	if !exists {
		log.Printf("⚠️ No chunk locations found for %s", chunkID)
		return &pb.GetChunkResponse{Success: false}, nil
	}

	// ✅ Filter out inactive servers before returning results
	activeServers := m.heartbeatManager.GetActiveChunkServers(servers)

	if len(activeServers) == 0 {
		log.Printf("⚠️ All chunk replicas for %s are unavailable!", chunkID)
		return &pb.GetChunkResponse{Success: false, Message: "No active servers available"}, nil
	}

	return &pb.GetChunkResponse{ChunkId: chunkID, ChunkServers: activeServers, Success: true}, nil
}

// ReportChunk updates chunk metadata when a chunk is created
func (m *MasterServer) ReportChunk(ctx context.Context, req *pb.ChunkReport) (*pb.ChunkResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// ✅ Ensure the reporting chunk server is active before updating chunkTable
	if !m.heartbeatManager.IsChunkServerActive(req.ServerId) {
		log.Printf("⚠️ Ignoring chunk report from inactive server %s", req.ServerId)
		return &pb.ChunkResponse{Success: false, Message: "Inactive server"}, nil
	}

	// ✅ Add the chunk location to metadata
	m.chunkTable[req.ChunkId] = append(m.chunkTable[req.ChunkId], req.ServerId)
	log.Printf("📥 Chunk %s reported by server %s", req.ChunkId, req.ServerId)

	return &pb.ChunkResponse{Success: true, Message: "Chunk reported successfully"}, nil
}
