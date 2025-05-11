package server

import (
	"context"
	"log"
	"sync"
	"time"

	pb "chunk_server_1/proto"
	"chunk_server_1/storage"
	"chunk_server_1/utils"

	"google.golang.org/grpc"
)

// ChunkServerInfo stores information about active chunk servers
type ChunkServerInfo struct {
	ServerID      string
	FreeSpace     int64
	LastHeartbeat time.Time
}

// HeartbeatManager tracks active chunk servers & sends heartbeats to Master Server
type HeartbeatManager struct {
	mu            sync.Mutex
	serverID      string
	masterAddress string
	storagePath   string
	interval      time.Duration
	client        pb.HeartbeatServiceClient
	conn          *grpc.ClientConn
	chunkServers  map[string]*ChunkServerInfo

	// ✅ Embed the unimplemented server to satisfy gRPC interface
	pb.UnimplementedHeartbeatServiceServer
}

// NewHeartbeatManager initializes a HeartbeatManager
func NewHeartbeatManager(serverID, masterAddress, storagePath string, interval time.Duration) *HeartbeatManager {
	hm := &HeartbeatManager{
		serverID:      serverID,
		masterAddress: masterAddress,
		storagePath:   storagePath,
		interval:      interval,
		chunkServers:  make(map[string]*ChunkServerInfo),
	}
	hm.connectToMaster()
	return hm
}

// connectToMaster establishes a persistent gRPC connection to Master Server
func (hm *HeartbeatManager) connectToMaster() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		conn, err := grpc.DialContext(ctx, hm.masterAddress, grpc.WithInsecure(), grpc.WithBlock())
		cancel()

		if err != nil {
			log.Printf("⚠️ Failed to connect to Master Server (%s): %v, retrying in 5s...", hm.masterAddress, err)
			time.Sleep(5 * time.Second)
			continue
		}

		hm.conn = conn
		hm.client = pb.NewHeartbeatServiceClient(conn)
		log.Println("✅ Connected to Master Server for Heartbeats")
		break // exit the loop once connected
	}
}

// StartHeartbeat sends heartbeats to Master Server at regular intervals
func (hm *HeartbeatManager) StartHeartbeat() {
	defer hm.conn.Close() // Close connection on shutdown

	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	for range ticker.C {
		hm.sendHeartbeat()
	}
}

func (hm *HeartbeatManager) sendHeartbeat() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Fetch system metrics
	freeSpace := int64(utils.GetFreeDiskSpace(hm.storagePath))
	totalSpace := int64(utils.GetTotalDiskSpace(hm.storagePath))
	cpuUsage := utils.GetCPUUsage()
	memoryUsage := utils.GetMemoryUsage()
	networkUsage := utils.GetNetworkUsage()
	load := utils.GetSystemLoad()

	// Fetch stored chunks from storage
	chunkIDs, err := storage.ListStoredChunks(hm.storagePath)
	if err != nil {
		log.Printf("⚠️ Failed to list stored chunks: %v", err)
		chunkIDs = []string{}
	}

	// Send heartbeat request
	req := &pb.HeartbeatRequest{
		ServerId:     hm.serverID,
		FreeSpace:    freeSpace,
		CpuUsage:     float32(cpuUsage),
		MemoryUsage:  float32(memoryUsage),
		NetworkUsage: float32(networkUsage),
		Load:         float32(load),
		ChunkIds:     chunkIDs,
		TotalSpace: totalSpace,
	}

	_, err = hm.client.SendHeartbeat(context.Background(), req)
	if err != nil {
		log.Printf("⚠️ Heartbeat failed: %v", err)
		hm.connectToMaster()
	} else {
		log.Printf(
			"💓 [%s] Heartbeat from %s | CPU: %.2f%% | Memory: %.2f%% | Free: %d MB / Total: %d MB | Load: %.2f | Chunks Stored: %d",
			time.Now().Format("15:04:05"),
			hm.serverID,
			cpuUsage,
			memoryUsage,
			freeSpace,
			totalSpace,
			load,
			len(chunkIDs), // Correct chunk count from actual slice
		)
	}
}

// SendHeartbeat (gRPC Method) - Called by Master Server
func (hm *HeartbeatManager) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	log.Printf("💓 Received Heartbeat from %s", req.ServerId)

	// ✅ Store/update active chunk servers
	hm.chunkServers[req.ServerId] = &ChunkServerInfo{
		ServerID:      req.ServerId,
		FreeSpace:     req.FreeSpace,
		LastHeartbeat: time.Now(),
	}

	return &pb.HeartbeatResponse{
		Success: true,
		Message: "✅ Heartbeat received successfully",
	}, nil
}

// RemoveInactiveServers removes chunk servers that have not sent heartbeats for 30+ seconds
func (hm *HeartbeatManager) RemoveInactiveServers() {
	for {
		time.Sleep(10 * time.Second) // Check every 10s

		hm.mu.Lock()
		for id, server := range hm.chunkServers {
			if time.Since(server.LastHeartbeat) > 30*time.Second {
				log.Printf("⚠️ Chunk Server %s is INACTIVE (no heartbeat in 30s)", id)
				delete(hm.chunkServers, id)
			}
		}
		hm.mu.Unlock()
	}
}
