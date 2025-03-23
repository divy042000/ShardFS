package server

import (
	"context"
	"encoding/json"
	"fmt"
	pb "master_server/proto"
	"os"
	"path/filepath"
	"sync"

)

type masterServer struct {
	pb.UnimplementedMasterServiceServer
	chunkServers []string         // address of chunk servers
    mu sync.Mutex

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
    s.mu.lock()
    defer s.mu.Unlock()

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

	// maxChunks := int(s.serverSpaces.m[req.LeaderServer] / (64 * 1024 * 1024))


	s.serverLoads.Lock()
	s.serverSpaces.RLock()
	s.serverLoads.Lock()
	s.serverSpaces.RLock()
	remainingSize := req.TotalSize
	remainingChunks := req.ChunkCount
	for i := int32(0); i < req.ChunkCount; {
		// Elect leader for remaining chunks
		leader := s.le.ElectLeader(remainingSize, remainingChunks, s.chunkServers, s.serverLoads.m, s.serverSpaces.m)
		if leader == "" {
			return nil, fmt.Errorf("no suitable leader found at chunk %d", i)
		}

		// Calculate max chunks this leader can handle
		maxChunks := int(s.serverSpaces.m[leader] / (64 * 1024 * 1024)) // 64MB chunks

		if maxChunks == 0 {
			leader = s.le.ElectLeader(remainingSize, remainingChunks, s.chunkServers, s.serverLoads.m, s.serverSpaces.m)
			if leader == "" {
				return nil, fmt.Errorf("no leader with enough space at chunk %d", i)
			}
			maxChunks = int(s.serverSpaces.m[leader] / (64 * 1024 * 1024))
			if maxChunks == 0 {
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
    // Step 4 : Store Chunk Packets
	s.chunkPackets.Lock()
	s.chunkPackets.m[fileID] = packets
	s.chunkPackets.Unlock()

	// Step 5 : Serialize to Disk
    data := struct {
		Metadata *pb.RegisterFileRequest
		Packets []ChunkPacket
	}{
		Metadata: req,
		Packets: packets,
	}
    bytes, err := json.Marshal(data)
	if err != nil{
		return nil, fmt.Errorf("failed to marshal data: %v",err)
	}
	if err := os.WriteFile(filepath.Join(s.storageDir, fileID+".json"),bytes,0644); err!=nil {
		return nil, fmt.Errorf("failed to write to disk %v", err)
	}

	// Step 6: Return Response
    return &pb.RegisterFileResponse{
		FileId: fileID,
		LeaderServer: chunkAssignments[0].Servers[0],
		chunkAssignments: chunkAssignments,
		replicationMap: convertReplicationMap(replicationMap),
		Success: true,
		Message: "File registered successfully",
	}, nil

	// Helper to convert replicationMap to proto format
    func convertReplicationMap(m map[int32][]string) map[int32]*pb.ReplicaServers {
		result := make(map[int32]*pb.ReplicaServers)
		for k, v := range m {
			result[k] = &pb.ReplicaServers{Servers: v}
		}
		return result
	}
}
