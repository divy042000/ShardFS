package server

import (
	"context"
	"encoding/json"
	"fmt"
	"image/jpeg"
	pb "master_server/proto"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/exp/rand"
)

type masterServer struct {
	pb.UnimplementedMasterServiceServer
	chunkServers []string         // address of chunk servers
	le           *LeaderElector   // Modular Leader Election
	rs           *ReplicaSelector // Modular Replica Selection

	// Granular Locking for each data structure
	serverLoads struct {
		sync.RWMutex
		m map[string]int64
	} // space used
	serverSpaces struct {
		sync.RWMutex
		m map[string]int64
	} // total space
	// fileID -> FileMetadata
	fileMetadata struct {
		sync.RWMutex
		m map[string]*pb.RegisterFileRequest
	}
	// chunkID -> ChunkPacket
	chunkPackets struct {
		sync.RWMutex
		m map[string][]ChunkPacket
	}
	// clientID_fileName -> fileID
	clientFileMap struct {
		sync.RWMutex
		m map[string]string
	}
	storageDir string // Directory for serialized data
}

func (s *masterServer) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	// Step 1 : Create Mapping
	clientFileKey := fmt.Sprintf("%s_%s", req.ClientId, req.FileName)
	fileID := fmt.Sprintf("file_%s_%d", clientFileKey, req.Timestamp)

	s.clientFileMap.Lock()
	// Check if file already exists with the same name
	if existingID, exists := s.clientFileMap.m[clientFileKey]; exists {
		s.clientFileMap.Unlock()
		return nil, fmt.Errorf("file already exists with ID %s", existingID)
	}
	s.clientFileMap.m[clientFileKey] = fileID
	s.clientFileMap.Unlock()

	// Step 2 : Store Metadata
	s.fileMetadata.Lock()
	s.fileMetadata.m[fileID] = req
	s.fileMetadata.Unlock()

	// Step 3 : Elect Leader and Assign Chunks
	chunkAssignments := make(map[int32]*pb.ChunkServers)
	replicationMap := make(map[int32][]string)
	var packets []ChunkPacket

	maxChunks := int(s.serverSpaces.m[req.LeaderServer] / (64 * 1024 * 1024))
	s.serverLoads.Lock()
	s.serverSpaces.RLock()
	remainingSize := req.TotalSize
	remainingChunks := req.ChunkCount
	s.serverLoads.Lock()
	s.serverSpaces.RLock()
	remainingSize := req.TotalSize
	remainingChunks := req.ChunkCount
	for i := int32(0); i < req.ChunkCount; {
		// Elect leader for remaining chunks
		leader := s.le.ElectLeader(remainingSize, remainingChunks, s.chunkServers, s.serverLoads.m, s.serverSpaces.m)
		if leader == "" {
			s.serverLoads.Unlock()
			s.serverSpaces.RUnlock()
			return nil, fmt.Errorf("no suitable leader found at chunk %d", i)
		}

		// Calculate max chunks this leader can handle
		maxChunks := int(s.serverSpaces.m[leader] / (64 * 1024 * 1024)) // 64MB chunks
		if maxChunks == 0 {
			leader = s.le.ElectLeader(remainingSize, remainingChunks, s.chunkServers, s.serverLoads.m, s.serverSpaces.m)
			if leader == "" {
				s.serverLoads.Unlock()
				s.serverSpaces.RUnlock()
				return nil, fmt.Errorf("no leader with enough space at chunk %d", i)
			}
			maxChunks = int(s.serverSpaces.m[leader] / (64 * 1024 * 1024))
			if maxChunks == 0 {
				s.serverLoads.Unlock()
				s.serverSpaces.RUnlock()
				return nil, fmt.Errorf("no server with sufficient space at chunk %d", i)
			}
		}

		// Determine chunks to assign (min of maxChunks or remainingChunks)
		chunksToAssign := maxChunks
		if int32(chunksToAssign) > remainingChunks {
			chunksToAssign = int(remainingChunks)
		}

		// Assign replicas once per batch
		replicas := s.rs.SelectReplicas(leader, 2, s.chunkServers)
		if len(replicas) < 2 {
			s.serverLoads.Unlock()
			s.serverSpaces.RUnlock()
			return nil, fmt.Errorf("not enough replica servers for chunk %d", i)
		}

		// Assign chunks in this batch
		for j := int32(0); j < int32(chunksToAssign); j++ {
			chunkIndex := i + j
			packet := ChunkPacket{
				ChunkName:        fmt.Sprintf("%s_%d", fileID, chunkIndex),
				LeaderAddress:    leader,
				ReplicaAddresses: replicas,
				FileID:           fileID,
				ChunkIndex:       chunkIndex,
				ChunkSize:        req.ChunkSizes[chunkIndex],
				ChunkHash:        req.ChunkHashes[chunkIndex],
			}
			packets = append(packets, packet)
			chunkAssignments[chunkIndex] = packet.ToProtoChunkServers()
			replicationMap[chunkIndex] = packet.ToProtoReplicaServers()

			s.serverLoads.m[leader] += req.ChunkSizes[chunkIndex]
		}

		// Update remaining counts
		remainingChunks -= int32(chunksToAssign)
		remainingSize -= req.TotalSize / int64(req.ChunkCount) * int64(chunksToAssign)
		if remainingChunks <= 0 {
			break
		}
		i += int32(chunksToAssign) // Move index forward
	}
	s.serverLoads.Unlock()
	s.serverSpaces.RUnlock()

}
