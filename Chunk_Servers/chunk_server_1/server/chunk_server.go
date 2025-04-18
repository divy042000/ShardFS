package server

import (
	pb "chunk_server_1/proto"
	"chunk_server_1/storage"
	"context"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ChunkServer represents a single chunk server
type ChunkServer struct {
	mu                 sync.Mutex
	serverID           string
	storagePath        string
	masterAddress      string
	workerPool         *WorkerPool
	heartbeatManager   *HeartbeatManager
	chunkTable         map[string][]string
	replicationManager *ReplicationManager
	pb.UnimplementedChunkServiceServer
}

// NewChunkServer initializes a Chunk Server
func NewChunkServer(serverID, storagePath, masterAddress string, workerCount int) *ChunkServer {
	cs := &ChunkServer{
		serverID:         serverID,
		storagePath:      storagePath,
		masterAddress:    masterAddress,
		workerPool:       NewWorkerPool(workerCount, 100), // Worker pool with queue size of 100
		heartbeatManager: NewHeartbeatManager(serverID, masterAddress, storagePath, 10*time.Second),
		chunkTable:       make(map[string][]string),
	}

	cs.replicationManager = NewReplicationManager(cs)

	return cs
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
		ServerId: cs.serverID,     // e.g., "chunk_server_1"
		Address:  ChunkServerAddr, // e.g., "chunk_server_container:50051"
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
	pb.RegisterChunkServiceServer(grpcServer, cs)
	pb.RegisterHeartbeatServiceServer(grpcServer, cs.heartbeatManager)

	go cs.heartbeatManager.StartHeartbeat()

	log.Printf("✅ Chunk Server %s started on port 50051", cs.serverID)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("❌ Failed to serve gRPC: %v", err)
	}
}

func (cs *ChunkServer) UploadChunk(stream pb.ChunkService_UploadChunkServer) error {
	var chunkID string
	var data []byte
	var leader, follower1, follower2 string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("📥 Received full chunk %s for storage to leader %s", chunkID, leader)

			// Submit storage to worker pool
			storageChan := make(chan JobResult, 1)
			storageJob := Job{
				Type:     WriteJob,
				ChunkID:  chunkID,
				Data:     data,
				Response: storageChan,
			}
			cs.workerPool.SubmitJob(storageJob)

			// Submit replication jobs to worker pool
			replicationChans := make([]chan JobResult, 0)
			followers := []string{follower1, follower2}
			for i := 1; i < len(followers); i++ { // Start from index 1 (skip leader)
				if followers[i] == "" {
					continue
				}
				replicationChan := make(chan JobResult, 1)
				replicationJob := Job{
					Type:         ReplicationJob,
					ChunkID:      chunkID,
					Data:         data,
					Followers:    followers,
					CurrentIndex: i,
					Response:     replicationChan,
				}
				cs.workerPool.SubmitJob(replicationJob)
				replicationChans = append(replicationChans, replicationChan)
			}

			// Wait for storage
			storageResult := <-storageChan
			if !storageResult.Success {
				log.Printf("❌ Storage error for %s: %s", chunkID, storageResult.Message)
				return stream.SendAndClose(&pb.ChunkUploadResponse{Success: false, Message: storageResult.Message})
			}

			// Wait for all replication results
			hasReplicationError := false
			for _, ch := range replicationChans {
				result := <-ch
				if !result.Success {
					log.Printf("⚠️ Replication error for %s: %s", chunkID, result.Message)
					hasReplicationError = true
				}
			}
			if hasReplicationError {
				return stream.SendAndClose(&pb.ChunkUploadResponse{Success: false, Message: "Replication failed"})
			}

			log.Printf("✅ Chunk %s stored and replicated successfully", chunkID)
			return stream.SendAndClose(&pb.ChunkUploadResponse{Success: true, Message: "Chunk uploaded and replicated"})
		}
		if err != nil {
			return err
		}

		chunkID = req.ChunkId
		data = append(data, req.Data...)
		leader = req.Leader
		follower1 = req.Follower1
		follower2 = req.Follower2
	}
}

// SendChunk handles chunk replication requests from the leader
func (cs *ChunkServer) SendChunk(ctx context.Context, req *pb.ReplicationRequest) (*pb.ReplicationResponse, error) {
	log.Printf("📥 Receiving replicated chunk %s from leader", req.ChunkId)

	// Submit storage to worker pool
	responseChan := make(chan JobResult, 1)
	job := Job{
		Type:     WriteJob,
		ChunkID:  req.ChunkId,
		Data:     req.Data,
		Response: responseChan,
	}
	cs.workerPool.SubmitJob(job)
	result := <-responseChan
	if !result.Success {
		log.Printf("❌ Replication error for %s: %s", req.ChunkId, result.Message)
		return &pb.ReplicationResponse{Success: false, Message: result.Message}, nil
	}

	log.Printf("✅ Chunk %s written as follower, initiating next replication", req.ChunkId)
	if err := cs.ReplicateChunk(req.ChunkId, req.Data, req.Followers, 1); err != nil {
		log.Printf("⚠️ Next replication failed for %s: %v", req.ChunkId, err)
		return &pb.ReplicationResponse{Success: false, Message: "Next replication failed"}, nil
	}

	log.Printf("✅ Chunk %s replication chain completed", req.ChunkId)
	return &pb.ReplicationResponse{Success: true, Message: "Replication completed"}, nil
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
func (cs *ChunkServer) ReplicateChunk(chunkID string, data []byte, followers []string, version int) error {
	return cs.replicationManager.ReplicateChunk(chunkID, data, followers, version)
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
