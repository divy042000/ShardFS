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
	NetworkUsage  float32
	Load float32 // system load average
	LastHeartbeat time.Time
}

// ServerScore represents the score of a Chunk Server based on its performance
type ServerScore struct {
	ServerID    string
	Score 	 float64 // Higher is better
	FreeSpace   int64
	CPUUsage   float32
	MemoryUsage float32
	NetworkUsage float32
	Load float32
	Index int
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
func (pq *PriorityQueue) Push(x interface{}){
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
	pb.UnimplementedHeartbeatServiceServer // ✅ Implements gRPC Interface
	mu           sync.Mutex // Mutex for thread safety
	chunkServers map[string]*ChunkServerInfo // Map of Chunk Server IDs to their info
	pq PriorityQueue // Priority queue for Chunk Servers
}

// NewHeartbeatManager initializes a HeartbeatManager
func NewHeartbeatManager() *HeartbeatManager {
	pq := make(PriorityQueue, 0) // Initialize the priority queue
	heap.Init(&pq) // Initialize the heap
	return &HeartbeatManager{
		chunkServers: make(map[string]*ChunkServerInfo),
		pq: pq,
	}
}



// SendHeartbeat receives heartbeats from Chunk Servers
func (hm *HeartbeatManager) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Update ChunkServerInfo with available metrics 
	info := &ChunkServerInfo{
		ServerID:      req.ServerId,
		FreeSpace:     req.FreeSpace,
		TotalSpace:    req.TotalSpace,
		StoredChunks:  req.ChunkIds,
		CPUUsage: req.CpuUsage,
		MemoryUsage: req.MemoryUsage,
		NetworkUsage: req.NetworkUsage,
		Load: req.Load,
		LastHeartbeat: time.Now(),
	}
    hm.chunkServers[req.ServerId] = info

	// Calculate the score based on available metrics
	score := calculateScore(info)


	// Update or add to priority queue
    found := false
	for i,item := range hm.pq {
		if item.ServerID == req.ServerId{
			hm.pq[i].Score = score
			
		}
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
