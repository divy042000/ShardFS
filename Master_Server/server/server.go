package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "master_server/proto"
)

// ChunkPacket represents metadata for a single chunk
type ChunkPacket struct {
	ChunkName        string   // e.g., "file_client1_test.txt_123456_0"
	LeaderAddress    string   // e.g., "chunk1:50051"
	ReplicaAddresses []string // e.g., ["chunk2:50051", "chunk3:50051"]
	FileID           string   // e.g., "file_client1_test.txt_123456"
	ChunkIndex       int32    // e.g., 0
	ChunkSize        int64    // e.g., 64000000 (bytes)
	ChunkHash        string   // e.g., "hash1"
}

// NewChunkPacket creates a new ChunkPacket
func NewChunkPacket(fileID string, chunkIndex int32, leader string, replicas []string, req *pb.RegisterFileRequest) ChunkPacket {
	return ChunkPacket{
		ChunkName:        fmt.Sprintf("%s_%d", fileID, chunkIndex),
		LeaderAddress:    leader,
		ReplicaAddresses: replicas,
		FileID:           fileID,
		ChunkIndex:       chunkIndex,
		ChunkSize:        req.ChunkSizes[chunkIndex],
		ChunkHash:        req.ChunkHashes[chunkIndex],
	}
}

// ToProtoChunkServers converts to proto format
func (cp ChunkPacket) ToProtoChunkServers() *pb.ChunkServers {
	return &pb.ChunkServers{Servers: []string{cp.LeaderAddress}}
}

// ToProtoReplicaServers returns replica addresses
func (cp ChunkPacket) ToProtoReplicaServers() []string {
	return cp.ReplicaAddresses
}

// ReplicaSelector handles replica server selection
type ReplicaSelector struct{}

// NewReplicaSelector creates a new ReplicaSelector instance
func NewReplicaSelector() *ReplicaSelector {
	return &ReplicaSelector{}
}

// SelectReplicas selects 'count' replica servers excluding the leader
func (rs *ReplicaSelector) SelectReplicas(leader string, count int, servers []string) []string {
	replicas := make([]string, 0, count)
	for _, server := range servers {
		if server != leader && len(replicas) < count {
			replicas = append(replicas, server)
		}
	}
	return replicas
}

// DataManager manages server and file metadata
type DataManager struct {
	mu           sync.Mutex // Added for thread safety of chunkServers
	chunkServers []string
	serverLoads struct {
		sync.RWMutex
		m map[string]int64
	}
	serverSpaces struct {
		sync.RWMutex
		m map[string]int64
	}
	fileMetadata struct {
		sync.RWMutex
		m map[string]*pb.RegisterFileRequest
	}
	clientFileMap struct {
		sync.RWMutex
		m map[string]string
	}
}

// NewDataManager initializes a DataManager
func NewDataManager(chunkServers []string) *DataManager {
	dm := &DataManager{
		mu:           sync.Mutex{}, // Initialize the mutex
		chunkServers: chunkServers,
	}
	dm.serverLoads.m = make(map[string]int64)
	dm.serverSpaces.m = make(map[string]int64)
	dm.fileMetadata.m = make(map[string]*pb.RegisterFileRequest)
	dm.clientFileMap.m = make(map[string]string)
	return dm
}

// RegisterFile registers a file and returns its ID
func (dm *DataManager) RegisterFile(req *pb.RegisterFileRequest) (string, error) {
	clientFileKey := fmt.Sprintf("%s_%s", req.ClientId, req.FileName)
	fileID := fmt.Sprintf("file_%s_%d", clientFileKey, req.Timestamp)

	dm.clientFileMap.Lock()
	if existingID, exists := dm.clientFileMap.m[clientFileKey]; exists {
		dm.clientFileMap.Unlock()
		return "", fmt.Errorf("file already exists with ID %s", existingID)
	}
	dm.clientFileMap.m[clientFileKey] = fileID
	dm.clientFileMap.Unlock()

	dm.fileMetadata.Lock()
	dm.fileMetadata.m[fileID] = req
	dm.fileMetadata.Unlock()

	return fileID, nil
}

// MaxChunksForServer calculates max chunks a server can handle
func (dm *DataManager) MaxChunksForServer(server string) int {
	dm.serverSpaces.RLock()
	defer dm.serverSpaces.RUnlock()
	space := dm.serverSpaces.m[server]
	if space == 0 {
		return 0 // Avoid division by zero
	}
	return int(space / (64 * 1024 * 1024)) // 64MB chunks
}

// UpdateLoad updates the load for a server
func (dm *DataManager) UpdateLoad(server string, size int64) {
	dm.serverLoads.Lock()
	defer dm.serverLoads.Unlock()
	dm.serverLoads.m[server] += size
}

// ChunkManager handles chunk storage and persistence
type ChunkManager struct {
	storageDir   string
	chunkPackets struct {
		sync.RWMutex
		m map[string][]ChunkPacket
	}
}

// NewChunkManager initializes a ChunkManager
func NewChunkManager(storageDir string) *ChunkManager {
	cm := &ChunkManager{storageDir: storageDir}
	cm.chunkPackets.m = make(map[string][]ChunkPacket)
	return cm
}

// StoreAndSerialize saves chunks and persists to disk
func (cm *ChunkManager) StoreAndSerialize(fileID string, req *pb.RegisterFileRequest, packets []ChunkPacket) error {
	cm.chunkPackets.Lock()
	cm.chunkPackets.m[fileID] = packets
	cm.chunkPackets.Unlock()

	data := struct {
		Metadata *pb.RegisterFileRequest
		Packets  []ChunkPacket
	}{
		Metadata: req,
		Packets:  packets,
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cm.storageDir, fileID+".json"), bytes, 0644); err != nil {
		return fmt.Errorf("failed to write to disk: %v", err)
	}
	return nil
}