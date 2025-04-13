package server

import (
	"context"
	"log"
	"net"
	"sync"
	"time"
     "os"
	"google.golang.org/grpc/credentials/insecure"
	 "google.golang.org/grpc"
	pb "chunk_server_1/proto"
	"chunk_server_1/storage"
)

// ChunkServer represents a single chunk server
type ChunkServer struct {
	mu               sync.Mutex
	serverID         string
	storagePath      string
	masterAddress    string
	workerPool       *WorkerPool
	heartbeatManager *HeartbeatManager
	chunkTable       map[string][]string
	pb.UnimplementedChunkServiceServer
}

// NewChunkServer initializes a Chunk Server
func NewChunkServer(serverID, storagePath, masterAddress string, workerCount int) *ChunkServer {
	return &ChunkServer{
		serverID:         serverID,
		storagePath:      storagePath,
		masterAddress:    masterAddress,
		workerPool:       NewWorkerPool(workerCount, 100), // Worker pool with queue size of 100
		heartbeatManager: NewHeartbeatManager(serverID, masterAddress, storagePath, 10*time.Second),
		chunkTable:       make(map[string][]string),
	}
}

func (cs *ChunkServer) Start() {
    // Register with master
    masterAddr := "master_server_container:50052"
	ChunkServerAddr := os.Getenv("CHUNK_SERVER_ADDRESS")
    conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatalf("❌ Failed to dial master at %s: %v", masterAddr, err)
    }
    defer conn.Close()
    client := pb.NewMasterServiceClient(conn)
    log.Printf("Attempting to register chunk server %s at %s", cs.serverID, ChunkServerAddr)
    resp, err := client.RegisterChunkServer(context.Background(), &pb.RegisterChunkServerRequest{
        ServerId: cs.serverID, // e.g., "chunk_server_1"
        Address:  ChunkServerAddr,  // e.g., "chunk_server_container:50051"
    })
    if err != nil {
        log.Printf("❌ Failed to register with master: %v", err)
    } else {
        log.Printf("✅ Registered with master: %s", resp.Message)
    }

    // Start gRPC server
    listener, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("❌ Failed to listen on port 50051: %v", err)
    }

    grpcServer := grpc.NewServer()
    rpcServer := NewRPCServer(cs)

    pb.RegisterChunkServiceServer(grpcServer, rpcServer)
    pb.RegisterHeartbeatServiceServer(grpcServer, cs.heartbeatManager)

    go cs.heartbeatManager.StartHeartbeat()

    log.Printf("✅ Chunk Server %s started on port 50051", cs.serverID)
    if err := grpcServer.Serve(listener); err != nil {
        log.Fatalf("❌ Failed to serve gRPC: %v", err)
    }
}

// WriteChunk handles chunk write requests from clients
func (cs *ChunkServer) WriteChunk(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	log.Printf("📥 Receiving chunk %s for storage", req.ChunkId)

	// Submit the write request to the worker pool
	responseChan := make(chan JobResult, 1)
	job := Job{
		Type:     WriteJob,
		ChunkID:  req.ChunkId,
		Data:     req.Data,
		Version:  int(req.Version),
		Response: responseChan,
	}

	cs.workerPool.SubmitJob(job)
	result := <-responseChan
	if !result.Success {
		log.Printf("❌ WriteChunk error: %s", result.Message)
		return &pb.WriteResponse{Success: false, Message: result.Message}, nil
	}

	log.Printf("✅ Chunk %s written successfully", req.ChunkId)
	return &pb.WriteResponse{Success: true, Message: "Chunk written"}, nil
}

// ReadChunk handles chunk read requests from clients
func (cs *ChunkServer) ReadChunk(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("📤 Reading chunk %s", req.ChunkId)

	// Submit the read request to the worker pool
	responseChan := make(chan JobResult, 1)
	job := Job{
		Type:     ReadJob,
		ChunkID:  req.ChunkId,
		Response: responseChan,
	}

	cs.workerPool.SubmitJob(job)
	result := <-responseChan
	if !result.Success {
		log.Printf("❌ ReadChunk error: %s", result.Message)
		return &pb.ReadResponse{Success: false}, nil
	}

	log.Printf("✅ Chunk %s read successfully", req.ChunkId)
	return &pb.ReadResponse{Success: true, Data: result.Data}, nil
}

// ReplicateChunk forwards the chunk to follower chunk servers
func (cs *ChunkServer) ReplicateChunk(chunkID string, data []byte, version int, followers []string) {
	replicationManager := NewReplicationManager(cs)
	replicationManager.ReplicateChunk(chunkID, data, version, followers)
}

// GetStoredChunkIds retrieves the list of stored chunk IDs from the storage directory
func (cs *ChunkServer) GetStoredChunkIds() []string {
	chunkIDs, err := storage.ListStoredChunks(cs.storagePath)
	if err != nil {
		log.Printf("⚠️ Failed to list stored chunks: %v", err)
		return []string{}
	}
	return chunkIDs
}

// UpdateChunkMetadata updates the chunk table to track which server stores each chunk
func (cs *ChunkServer) UpdateChunkMetadata(serverID string, chunkIDs []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// ✅ Ensure chunkTable is initialized before use
	if cs.chunkTable == nil {
		cs.chunkTable = make(map[string][]string)
	}

	for _, chunkID := range chunkIDs {
		cs.chunkTable[chunkID] = append(cs.chunkTable[chunkID], serverID)
	}

	log.Printf("📌 Updated metadata: %d chunks stored by server: %s", len(chunkIDs), serverID)
}
