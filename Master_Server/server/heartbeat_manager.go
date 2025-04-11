package server

import (
	"container/heap"
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
	CPUUsage      float32 // CPU usage percentage
	MemoryUsage   float32
	NetworkUsage  float32 // Use when it is necessary.
	Load          float32 // system load average
	LastHeartbeat time.Time
}

// ServerScore represents the score of a Chunk Server based on its performance
type ServerScore struct {
	ServerID     string
	Score        float64 // Higher is better
	FreeSpace    int64
	CPUUsage     float32
	MemoryUsage  float32
	NetworkUsage float32
	Load         float32
	Index        int
}

// PriorityQueue is a max-heap of ServerScore
type PriorityQueue []*ServerScore

func (pq PriorityQueue) Len() int { return len(pq) } // Length of the queue

// Conversion of min-heap to max-heap
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Score > pq[j].Score // Higher score is better
}

// Swap exchanges two elements in the queue
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push adds an element to the queue
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*ServerScore)
	item.Index = n
	*pq = append(*pq, item)
}

// Pop removes the highest priority element from the queue
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// HeartbeatManager tracks active Chunk Servers with a priority queue
type HeartbeatManager struct {
	pb.UnimplementedHeartbeatServiceServer                             // ✅ Implements gRPC Interface
	mu                                     sync.Mutex                  // Mutex for thread safety
	chunkServers                           map[string]*ChunkServerInfo // Map of Chunk Server IDs to their info
	pq                                     PriorityQueue               // Priority queue for Chunk Servers
	ms                                     *MasterServer               // Master Server reference for leader election
}

// NewHeartbeatManager initializes a HeartbeatManager
func NewHeartbeatManager() *HeartbeatManager {
	pq := make(PriorityQueue, 0) // Initialize the priority queue
	heap.Init(&pq)               // Initialize the heap
	return &HeartbeatManager{
		chunkServers: make(map[string]*ChunkServerInfo),
		pq:           pq,
	}
}

// SendHeartbeat receives heartbeats from Chunk Servers
func (hm *HeartbeatManager) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Update current server info
	info := &ChunkServerInfo{
		ServerID:      req.ServerId,
		FreeSpace:     req.FreeSpace,
		TotalSpace:    req.TotalSpace,
		StoredChunks:  req.ChunkIds,
		CPUUsage:      req.CpuUsage,
		MemoryUsage:   req.MemoryUsage,
		NetworkUsage:  req.NetworkUsage,
		Load:          req.Load,
		LastHeartbeat: time.Now(),
	}
	hm.chunkServers[req.ServerId] = info

	// Compute scheduling score
	score := calculateScore(info)
	hm.updateOrPushServerScore(req.ServerId, score, req)

	// Log the heartbeat nicely
	log.Printf(
		"💓 [%s] Heartbeat received from %s | CPU: %.2f%% | Mem: %.2f%% | Free: %dMB / Total: %dMB | Load: %.2f | Network: %.2fKB/s | Chunks: %d",
		time.Now().Format("15:04:05"),
		req.ServerId,
		req.CpuUsage,
		req.MemoryUsage,
		req.FreeSpace,
		req.TotalSpace,
		req.Load,
		req.NetworkUsage,
		len(req.ChunkIds),
	)

	return &pb.HeartbeatResponse{
		Success: true,
		Message: "✅ Heartbeat received successfully",
	}, nil
}

func (hm *HeartbeatManager) updateOrPushServerScore(serverID string, score float64, req *pb.HeartbeatRequest) {
	for i, item := range hm.pq {
		if item.ServerID == serverID {
			hm.pq[i].Score = score
			hm.pq[i].FreeSpace = req.FreeSpace
			hm.pq[i].CPUUsage = req.CpuUsage
			hm.pq[i].MemoryUsage = req.MemoryUsage
			hm.pq[i].NetworkUsage = req.NetworkUsage
			hm.pq[i].Load = req.Load
			heap.Fix(&hm.pq, i)
			return
		}
	}

	// Not found — push new
	heap.Push(&hm.pq, &ServerScore{
		ServerID:     serverID,
		Score:        score,
		FreeSpace:    req.FreeSpace,
		CPUUsage:     req.CpuUsage,
		MemoryUsage:  req.MemoryUsage,
		NetworkUsage: req.NetworkUsage,
		Load:         req.Load,
	})
}

// calculate score computes a score comprising of available metric score out of heartbeat
func calculateScore(info *ChunkServerInfo) float64 {
	spaceScore := float64(info.FreeSpace) / float64(info.TotalSpace)
	computeScore := (100.0 - float64(info.CPUUsage)) / 100.0
	memoryScore := (100.0 - float64(info.MemoryUsage)) / 100.0
	networkScore := (100.0 - float64(info.NetworkUsage)) / 100.0
	loadScore := 1.0
	if info.Load > 0 {
		loadScore = 1.0 / float64(info.Load)
		if loadScore > 1.0 {
			loadScore = 1.0
		}
	}
	return 0.4*spaceScore + 0.25*computeScore + 0.15*memoryScore + 0.10*networkScore + 0.10*loadScore
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
				// Remove from priority queue
				for i, item := range hm.pq {
					if item.ServerID == id {
						heap.Remove(&hm.pq, i)
						break
					}
				}
			}
			// Update dataManager if linked
			if hm.ms != nil {
				hm.ms.dataManager.serverLoads.Lock()
				delete(hm.ms.dataManager.serverLoads.m, id)
				hm.ms.dataManager.serverLoads.Unlock()
				hm.ms.dataManager.serverSpaces.Lock()
				delete(hm.ms.dataManager.serverSpaces.m, id)
				hm.ms.dataManager.serverSpaces.Unlock()
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
