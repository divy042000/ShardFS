package server

import (
	"context"
	"log"
	"sync"

	pb "master_server/proto"
)

// MasterServer manages file metadata and chunk locations
type MasterServer struct {
	pb.UnimplementedMasterServiceServer
	mu         sync.Mutex
	chunkTable map[string][]string // Mapping chunk_id → chunk servers
}

// NewMasterServer initializes a new master server
func NewMasterServer() *MasterServer {
	return &MasterServer{
		chunkTable: make(map[string][]string),
	}
}

// GetChunkLocations provides chunk locations for a requested file
func (m *MasterServer) GetChunkLocations(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	chunkID := req.FileName + "_" + string(req.ChunkIndex)
	servers, exists := m.chunkTable[chunkID]
	if !exists {
		return &pb.GetChunkResponse{Success: false}, nil
	}

	return &pb.GetChunkResponse{ChunkId: chunkID, ChunkServers: servers, Success: true}, nil
}

// ReportChunk updates chunk metadata when a chunk is created
func (m *MasterServer) ReportChunk(ctx context.Context, req *pb.ChunkReport) (*pb.ChunkResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.chunkTable[req.ChunkId] = append(m.chunkTable[req.ChunkId], req.ServerId)

	log.Printf("Chunk %s reported by server %s", req.ChunkId, req.ServerId)
	return &pb.ChunkResponse{Success: true, Message: "Chunk reported successfully"}, nil
}
