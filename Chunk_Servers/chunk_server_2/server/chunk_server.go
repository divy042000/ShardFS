package server

import (
	pb "chunk_server_2/proto"
	"chunk_server_2/storage"
	"context"
	"io"
	"fmt"
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
	selfAddress        string
	chunkTable         map[string][]string
	replicationManager *ReplicationManager
	pb.UnimplementedChunkServiceServer
}

func NewChunkServer(serverID, storagePath, masterAddress, selfAddress string, workerCount int) *ChunkServer {
	cs := &ChunkServer{
		serverID:      serverID,
		storagePath:   storagePath,
		masterAddress: masterAddress,
		selfAddress:   selfAddress,
		chunkTable:    make(map[string][]string),
	}

	// Step 1: Initialize ReplicationManager first
	cs.replicationManager = NewReplicationManager(cs)

	// Step 2: Pass it to the WorkerPool
	cs.workerPool = NewWorkerPool(workerCount, 100, cs.replicationManager)

	// Step 3: Initialize HeartbeatManager (unrelated to replication)
	cs.heartbeatManager = NewHeartbeatManager(serverID, masterAddress, storagePath, 10*time.Second)

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
	listener, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("❌ Failed to listen on port 50053: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChunkServiceServer(grpcServer, cs)
	pb.RegisterHeartbeatServiceServer(grpcServer, cs.heartbeatManager)

	go cs.heartbeatManager.StartHeartbeat()

	log.Printf("✅ Chunk Server %s started on port 50053", cs.serverID)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("❌ Failed to serve gRPC: %v", err)
	}
}


func (cs *ChunkServer) UploadChunk(stream pb.ChunkService_UploadChunkServer) error {
	var chunkHash, fileID string
	var chunkIndex int32
	var data []byte
	var leader, follower1, follower2 string

	log.Println("🚀 Starting UploadChunk stream handler...")

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("📥 Received complete data stream for chunk (hash: %s, index: %d) of file '%s' at leader '%s'", chunkHash, chunkIndex, fileID, leader)

			// 1. Store chunk locally via worker pool
			log.Printf("💾 Submitting write job for chunk (hash: %s, index: %d) to worker pool", chunkHash, chunkIndex)
			storageChan := make(chan JobResult, 1)
			cs.workerPool.SubmitJob(Job{
				Type:        WriteJob,
				ChunkHash:   chunkHash,
				ChunkIndex:  chunkIndex,
				Data:        data,
				Response:    storageChan,
			})

			storageResult := <-storageChan
			if !storageResult.Success {
				log.Printf("❌ Failed to write chunk (hash: %s, index: %d): %s", chunkHash, chunkIndex, storageResult.Message)
				return stream.SendAndClose(&pb.ChunkUploadResponse{
					Success:    false,
					Message:    storageResult.Message,
					FileId:     fileID,
					ChunkHash:  chunkHash,
				})
			}
			log.Printf("✅ Successfully stored chunk (hash: %s, index: %d) at leader '%s'", chunkHash, chunkIndex, leader)

			// 2. Submit replication job to worker pool
			if follower1 != "" {
				log.Printf("📨 Submitting replication job for chunk (hash: %s, index: %d) to followers: [%s, %s]", chunkHash, chunkIndex, follower1, follower2)
				replicationChan := make(chan JobResult, 1)
				cs.workerPool.SubmitJob(Job{
					Type:         ReplicationJob,
					ChunkHash:    chunkHash,
					ChunkIndex:   chunkIndex,
					Data:         data,
					Followers:    []string{follower1, follower2},
					CurrentIndex: 0,
					Response:     replicationChan,
				})

				replicationResult := <-replicationChan
				if !replicationResult.Success {
					log.Printf("❌ Replication failed for chunk (hash: %s, index: %d): %s", chunkHash, chunkIndex, replicationResult.Message)
					return stream.SendAndClose(&pb.ChunkUploadResponse{
						Success:    false,
						Message:    "Replication failed",
						FileId:     fileID,
						ChunkHash:  chunkHash,
					})
				}
				log.Printf("✅ Replication completed for chunk (hash: %s, index: %d)", chunkHash, chunkIndex)
			} else {
				log.Printf("⚠️ No followers provided for replication of chunk (hash: %s, index: %d)", chunkHash, chunkIndex)
			}

			log.Printf("📦 UploadChunk process complete for chunk (hash: %s, index: %d) of file '%s'", chunkHash, chunkIndex, fileID)
			return stream.SendAndClose(&pb.ChunkUploadResponse{
				Success:    true,
				Message:    "Chunk uploaded and replicated",
				FileId:     fileID,
				ChunkHash:  chunkHash,
			})
		}

		if err != nil {
			log.Printf("❌ Error receiving chunk data stream: %v", err)
			return err
		}

		log.Printf("📡 Receiving data for chunk (hash: %s, index: %d), file '%s'...", req.ChunkHash, req.ChunkIndex, req.FileId)

		// Extract and accumulate data
		chunkHash = req.ChunkHash
		chunkIndex = req.ChunkIndex
		fileID = req.FileId
		data = append(data, req.Data...)
		leader = req.Leader
		follower1 = req.Follower1
		follower2 = req.Follower2
	}
}


func (cs *ChunkServer) SendChunk(ctx context.Context, req *pb.ReplicationRequest) (*pb.ReplicationResponse, error) {
	log.Printf("📥 [RECV] Follower '%s' received chunk (hash: %s, index: %d) of file '%s' for replication",
		cs.selfAddress, req.ChunkHash, req.ChunkIndex, req.FileId)

	// 1. Store chunk locally using worker pool
	log.Printf("💾 [WRITE] Submitting write job for chunk (hash: %s, index: %d) to local disk at '%s'",
		req.ChunkHash, req.ChunkIndex, cs.selfAddress)
	responseChan := make(chan JobResult, 1)
	cs.workerPool.SubmitJob(Job{
		Type:        WriteJob,
		ChunkHash:   req.ChunkHash,
		ChunkIndex:  req.ChunkIndex,
		Data:        req.Data,
		Response:    responseChan,
	})
	result := <-responseChan

	statusMap := map[string]bool{cs.selfAddress: result.Success}
	if !result.Success {
		log.Printf("❌ [FAIL] Failed to write chunk (hash: %s, index: %d) at follower '%s': %s",
			req.ChunkHash, req.ChunkIndex, cs.selfAddress, result.Message)
		return &pb.ReplicationResponse{
			Success:   false,
			Message:   result.Message,
			StatusMap: statusMap,
		}, nil
	}
	log.Printf("✅ [SUCCESS] Chunk (hash: %s, index: %d) successfully written at '%s'",
		req.ChunkHash, req.ChunkIndex, cs.selfAddress)

	// 2. Submit next chained replication job to the worker pool
	nextIdx := findIndex(req.Followers, cs.selfAddress) + 1
	if nextIdx < len(req.Followers) && req.Followers[nextIdx] != "" {
		nextFollower := req.Followers[nextIdx]
		log.Printf("🔗 [CHAIN] Submitting chained replication for chunk (hash: %s, index: %d) to follower '%s'",
			req.ChunkHash, req.ChunkIndex, nextFollower)

		replicationChan := make(chan JobResult, 1)
		cs.workerPool.SubmitJob(Job{
			Type:         ReplicationJob,
			ChunkHash:    req.ChunkHash,
			ChunkIndex:   req.ChunkIndex,
			Data:         req.Data,
			Followers:    req.Followers,
			CurrentIndex: nextIdx,
			Response:     replicationChan,
		})

		replicationResult := <-replicationChan
		if !replicationResult.Success {
			log.Printf("⚠️ [WARN] Replication to next follower '%s' failed for chunk (hash: %s, index: %d): %s",
				nextFollower, req.ChunkHash, req.ChunkIndex, replicationResult.Message)
			return &pb.ReplicationResponse{
				Success:   false,
				Message:   "Next replication failed",
				StatusMap: statusMap,
			}, nil
		}

		log.Printf("✅ [CHAIN] Follower '%s' successfully replicated chunk (hash: %s, index: %d)",
			nextFollower, req.ChunkHash, req.ChunkIndex)
		statusMap[nextFollower] = true
	} else {
		log.Printf("🔚 [END] No next follower. Replication chain ends at '%s'", cs.selfAddress)
	}

	log.Printf("🏁 [DONE] Replication chain complete at '%s' for chunk (hash: %s, index: %d)",
		cs.selfAddress, req.ChunkHash, req.ChunkIndex)
	return &pb.ReplicationResponse{
		Success:   true,
		Message:   "Replication successful",
		StatusMap: statusMap,
	}, nil
}



// ReadChunk handles chunk read requests from clients
func (cs *ChunkServer) DownloadChunk(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	chunkID := fmt.Sprintf("%s_%d", req.ChunkHash, req.ChunkIndex)
	log.Printf("📥 [ChunkServer] Received read request for chunk: %s", chunkID)

	// Submit the read job to the worker pool
	responseChan := make(chan JobResult, 1)
	job := Job{
		Type:       ReadJob,
		ChunkHash:  req.ChunkHash,
		ChunkIndex: req.ChunkIndex,
		Response:   responseChan,
	}

	log.Printf("🚀 [ChunkServer] Submitting read job to worker pool for chunk: %s", chunkID)
	cs.workerPool.SubmitJob(job)

	// Wait for worker response
	result := <-responseChan
	if !result.Success {
		log.Printf("❌ [ChunkServer] Failed to read chunk %s: %s", chunkID, result.Message)
		return &pb.DownloadResponse{
			Success: false,
			Message: result.Message,
		}, nil
	}

	log.Printf("✅ [ChunkServer] Successfully read chunk %s (%d bytes)", chunkID, len(result.Data))
	return &pb.DownloadResponse{
		Success: true,
		Data:    result.Data,
		Message: "Chunk read successfully",
	}, nil
}


func (cs *ChunkServer) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	log.Printf("🧨 Received request to delete chunk '%s'", req.ChunkId)

	var chunkHash string
	var chunkIndex int32
	_, err := fmt.Sscanf(req.ChunkId, "%[^_]_%d", &chunkHash, &chunkIndex)
	if err != nil {
		log.Printf("❌ Failed to parse chunk ID '%s': %v", req.ChunkId, err)
		return &pb.DeleteChunkResponse{
			Success: false,
			Message: "Invalid chunk ID format",
		}, nil
	}

	// Submit delete job to worker pool
	responseChan := make(chan JobResult)
	 job :=  Job{
		Type:       DeleteJob,
		ChunkHash:  chunkHash,
		ChunkIndex: chunkIndex,
		Response:   responseChan,
	}
	log.Printf("🚀 [ChunkServer] Submitting read job to worker pool for chunk: %s", req.ChunkId)
	cs.workerPool.SubmitJob(job)

	// Wait for result
	result := <-responseChan
	if result.Success {
		log.Printf("✅ Successfully deleted chunk '%s'", req.ChunkId)
	} else {
		log.Printf("❌ Deletion failed for chunk '%s': %s", req.ChunkId, result.Message)
	}

	return &pb.DeleteChunkResponse{
		Success: result.Success,
		Message: result.Message,
	}, nil
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

func allSuccess(statusMap map[string]bool) bool {
	for _, success := range statusMap {
		if !success {
			return false
		}
	}
	return true
}

func findIndex(slice []string, target string) int {
	for i, v := range slice {
		if v == target {
			return i
		}
	}
	return -1
}

func mergeStatusMaps(m1, m2 map[string]bool) map[string]bool {
	merged := make(map[string]bool)
	for k, v := range m1 {
		merged[k] = v
	}
	for k, v := range m2 {
		merged[k] = v
	}
	return merged
}
