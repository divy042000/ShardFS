package server

import (
	"container/heap"
	"context"
	"log"
	pb "master_server/proto"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	pb.UnimplementedHeartbeatServiceServer
	mu           sync.Mutex
	ChunkServers map[string]*ChunkServerInfo
	pq           PriorityQueue
	ms           *MasterServer
	ScoreWeights ScoreWeights
}

type ScoreWeights struct {
	Space   float64
	CPU     float64
	Memory  float64
	Network float64
	Load    float64
}

// NewHeartbeatManager initializes a HeartbeatManager
func NewHeartbeatManager(ms *MasterServer) *HeartbeatManager {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &HeartbeatManager{
		ChunkServers: make(map[string]*ChunkServerInfo),
		pq:           pq,
		ms:           ms,
		ScoreWeights: ScoreWeights{
			Space:   0.4,
			CPU:     0.25,
			Memory:  0.15,
			Network: 0.10,
			Load:    0.10,
		},
	}
}

// SendHeartbeat receives heartbeats from Chunk Servers
func (hm *HeartbeatManager) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Validate Server
	if !hm.ms.dataManager.IsServerRegistered(req.ServerId) {
		log.Printf("❌ Heartbeat from unregistered server %s", req.ServerId)
		return &pb.HeartbeatResponse{
			Success: false,
			Message: "❌ Heartbeat from unregistered server",
		}, nil
	}

	// // Validate chunk_ids
	// if !hm.validateChunkIds(ctx, req.ChunkIds) {
	// 	log.Printf("❌ Heartbeat from %s with invalid chunk IDs: %v", req.ServerId, req.ChunkIds)
	// }

	// Update chunk server info
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
	hm.ChunkServers[req.ServerId] = info

	// Update DataManager

	hm.ms.dataManager.serverSpaces.Lock()
	hm.ms.dataManager.serverSpaces.m[req.ServerId] = req.FreeSpace
	hm.ms.dataManager.serverSpaces.Unlock()
	hm.ms.dataManager.serverLoads.Lock()
	hm.ms.dataManager.serverLoads.m[req.ServerId] = int64(req.Load * 100)
	hm.ms.dataManager.serverLoads.Unlock()

	// Compute and Update Score
	score := hm.calculateScore(info)
	hm.updateOrPushServerScore(req.ServerId, score, req)

	// Update MongoDB
	update := bson.M{
		"$set": bson.M{
			"server_id":      req.ServerId,
			"free_space":     req.FreeSpace,
			"total_space":    req.TotalSpace,
			"cpu_usage":      req.CpuUsage,
			"memory_usage":   req.MemoryUsage,
			"network_usage":  req.NetworkUsage,
			"load":           req.Load,
			"chunk_ids":      req.ChunkIds,
			"last_heartbeat": time.Now().Unix(),
			"active":         true,
			"score":          score,
		},
	}

	_, err := hm.ms.db.Collection("server_status").UpdateOne(
		ctx,
		bson.M{"server_id": req.ServerId},
		update,
		options.Update().SetUpsert(true),
	)
	if err != nil {
		log.Printf("❌ Failed to update server status in MongoDB: %v", err)
	}

	log.Printf(
		"💓 Heartbeat [%s]: CPU=%.2f%%, Mem=%.2f%%, Free=%dB, Total=%dB, Load=%.2f, Net=%.2fKB/s, Chunks=%d, Score=%.3f",
		req.ServerId, req.CpuUsage, req.MemoryUsage, req.FreeSpace, req.TotalSpace, req.Load, req.NetworkUsage, len(req.ChunkIds), score,
	)

	return &pb.HeartbeatResponse{
		Success: true,
		Message: "✅ Heartbeat received successfully",
	}, nil

}

// // validateChunkIds checks if the chunk IDs are valid
// func (hm *HeartbeatManager) validateChunkIds(ctx context.Context, chunkIds []string) bool {
// 	for _, chunkId := range chunkIds {
// 		var metadata FileMetadata
// 		err := hm.ms.db.Collection("file_metadata").FindOne(ctx, bson.M{"chunk_assignments": bson.M{"$exists": true}}).Decode(&metadata)
// 		if err != nil {
// 			continue
// 		}

// 		// Iterate over ChunkAssignments which is a slice of ChunkPacket
// 		for _, packet := range metadata.ChunkAssignments {
// 			// Check if the chunkId matches the FileID or ChunkIndex (based on your logic)
// 			if packet.FileID == chunkId || string(packet.ChunkIndex) == chunkId {
// 				return true
// 			}
// 		}
// 	}
// 	return len(chunkIds) == 0
// }

// calculateScore computes a server score
func (hm *HeartbeatManager) calculateScore(info *ChunkServerInfo) float64 {
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
	w := hm.ScoreWeights
	return w.Space*spaceScore + w.CPU*computeScore + w.Memory*memoryScore + w.Network*networkScore + w.Load*loadScore
}

// updateOrPushServerScore updates or pushes a server score in the priority queue
func (hm *HeartbeatManager) updateOrPushServerScore(serverID string, score float64, req *pb.HeartbeatRequest) {
	log.Printf("📊 Updating score for %s, initial pq size: %d", serverID, len(hm.pq))
	for i, item := range hm.pq {
		if item.ServerID == serverID {
			log.Printf("📊 Updating existing server %s at index %d", serverID, i)
			hm.pq[i].Score = score
			hm.pq[i].FreeSpace = req.FreeSpace
			hm.pq[i].CPUUsage = req.CpuUsage
			hm.pq[i].MemoryUsage = req.MemoryUsage
			hm.pq[i].NetworkUsage = req.NetworkUsage
			hm.pq[i].Load = req.Load
			heap.Fix(&hm.pq, i)
			log.Printf("📊 Fixed heap for %s, new pq size: %d", serverID, len(hm.pq))
			return
		}
	}
	log.Printf("📊 Pushing new server %s to pq", serverID)
	heap.Push(&hm.pq, &ServerScore{
		ServerID:     serverID,
		Score:        score,
		FreeSpace:    req.FreeSpace,
		CPUUsage:     req.CpuUsage,
		MemoryUsage:  req.MemoryUsage,
		NetworkUsage: req.NetworkUsage,
		Load:         req.Load,
	})
	log.Printf("📊 Pushed to heap, new pq size: %d", len(hm.pq))
}

// RemoveInactiveServers removes inactive Chunk Servers (no heartbeat in 30s)
func (hm *HeartbeatManager) RemoveInactiveServers() {
	for {
		time.Sleep(10 * time.Second) // Check every 10s
		hm.mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for id, server := range hm.ChunkServers {
			if time.Since(server.LastHeartbeat) > 30*time.Second {
				log.Printf("⚠️  Chunk Server %s is INACTIVE! Removing from active list.", id)
				delete(hm.ChunkServers, id)
				// Remove from priority queue
				for i, item := range hm.pq {
					if item.ServerID == id {
						heap.Remove(&hm.pq, i)
						break
					}
				}
				// Update MongoDB
				_, err := hm.ms.db.Collection("server_status").UpdateOne(
					ctx,
					bson.M{"server_id": id},
					bson.M{"$set": bson.M{"active": false}},
					options.Update().SetUpsert(true),
				)
				if err != nil {
					log.Printf("❌ Failed to update server status in MongoDB: %v", err)
				}
				// Update dataManager if linked
				hm.ms.dataManager.serverSpaces.Lock()
				delete(hm.ms.dataManager.serverSpaces.m, id)
				hm.ms.dataManager.serverSpaces.Unlock()
				hm.ms.dataManager.serverLoads.Lock()
				delete(hm.ms.dataManager.serverLoads.m, id)
				hm.ms.dataManager.serverLoads.Unlock()
				hm.ms.dataManager.RemoveServer(id)

			}
		}
		hm.mu.Unlock()
	}
}

// GetActiveChunkServers returns active servers
func (hm *HeartbeatManager) GetActiveChunkServers(servers []string) []string {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	var activeServers []string
	for _, serverID := range servers {
		if _, exists := hm.ChunkServers[serverID]; exists {
			activeServers = append(activeServers, serverID)
		}
	}
	return activeServers
}

func (hm *HeartbeatManager) IsChunkServerActive(serverID string) bool {
    hm.mu.Lock()
    defer hm.mu.Unlock()
    info, exists := hm.ChunkServers[serverID]
    if !exists {
        return false
    }
    // Consider server inactive if last heartbeat is older than 30 seconds
    return time.Since(info.LastHeartbeat) < 30*time.Second
}