package server

import (
	"context"
	"log"
	"net"
	"time"

	pb "chunk_server_1/proto"
	"chunk_server_1/storage"
	"chunk_server_1/server/replication"
	"chunk_server_1/server/worker_pool"
	"google.golang.org/grpc"
)

// ChunkServer represents a single chunk server
type ChunkServer struct {
	serverID         string
	storagePath      string
	masterAddress    string
	workerPool       *worker_pool.WorkerPool
	heartbeatManager *HeartbeatManager
	pb.UnimplementedChunkServiceServer
}

// NewChunkServer initializes a Chunk Server
func NewChunkServer(serverID, storagePath, masterAddress string, workerCount int) *ChunkServer {
	return &ChunkServer{
		serverID:         serverID,
		storagePath:      storagePath,
		masterAddress:    masterAddress,
		workerPool:       worker_pool.NewWorkerPool(workerCount, 100), // Worker pool with queue size of 100
		heartbeatManager: NewHeartbeatManager(serverID, masterAddress, storagePath, 10*time.Second),
	}
}

// Start initializes the gRPC server and handles RPCs
func (cs *ChunkServer) Start() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	rpcServer := NewRPCServer(cs)
	pb.RegisterChunkServiceServer(grpcServer, rpcServer)
	pb.RegisterHeartbeatServiceServer(grpcServer, rpcServer)

	// Start Heartbeat routine
	go cs.heartbeatManager.StartHeartbeat()

	log.Printf("Chunk Server %s started on port 50051", cs.serverID)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// WriteChunk submits the request to the worker pool for processing
func (cs *ChunkServer) WriteChunk(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	responseChan := make(chan worker_pool.JobResult, 1)

	job := worker_pool.Job{
		Type:     worker_pool.WriteJob,
		ChunkID:  req.ChunkId,
		Data:     req.Data,
		Version:  int(req.Version),
		Response: responseChan,
	}

	cs.workerPool.SubmitJob(job)

	result := <-responseChan
	if !result.Success {
		log.Printf("WriteChunk error: %s", result.Message)
		return &pb.WriteResponse{Success: false, Message: result.Message}, nil
	}

	log.Printf("Chunk %s written successfully", req.ChunkId)
	return &pb.WriteResponse{Success: true, Message: "Chunk written"}, nil
}

// ReadChunk submits the request to the worker pool for processing
func (cs *ChunkServer) ReadChunk(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	responseChan := make(chan worker_pool.JobResult, 1)

	job := worker_pool.Job{
		Type:     worker_pool.ReadJob,
		ChunkID:  req.ChunkId,
		Response: responseChan,
	}

	cs.workerPool.SubmitJob(job)

	result := <-responseChan
	if !result.Success {
		log.Printf("ReadChunk error: %s", result.Message)
		return &pb.ReadResponse{Success: false}, nil
	}

	log.Printf("Chunk %s read successfully", req.ChunkId)
	return &pb.ReadResponse{Success: true, Data: result.Data}, nil
}

// ReplicateChunk submits the replication request to the worker pool
func (cs *ChunkServer) ReplicateChunk(chunkID string, data []byte, version int, followers []string) {
	replicationManager := replication.NewReplicationManager(cs)
	replicationManager.ReplicateChunk(chunkID, data, version, followers)
}
