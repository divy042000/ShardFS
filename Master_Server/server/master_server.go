package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "master_server/proto"
)

type MasterServer struct {
	pb.UnimplementedMasterServiceServer
	dataManager     *DataManager
	hm              *HeartbeatManager
	le              *LeaderElector
	serverAddresses map[string]string
	mongoClient     *mongo.Client
	db              *mongo.Database
	workerPool      *WorkerPool
}

type SafeMap struct {
	mu sync.RWMutex
	m  map[string]interface{}
}

func (sm *SafeMap) Lock() {
	sm.mu.Lock()
}

// Unlock unlocks the SafeMap after write access.
func (sm *SafeMap) Unlock() {
	sm.mu.Unlock()
}

// RLock locks the SafeMap for read access.
func (sm *SafeMap) RLock() {
	sm.mu.RLock()
}

// RUnlock unlocks the SafeMap after read access.
func (sm *SafeMap) RUnlock() {
	sm.mu.RUnlock()
}

type DataManager struct {
	chunkServers     []string
	serverSpaces     *SafeMap
	serverLoads      *SafeMap
	clientFileMap    *SafeMap
	fileMetadata     *SafeMap
	MaxChunksPerFile int
}

type ReplicaDeleteInfo struct {
	ChunkID      string
	ReplicaAddrs []string
}

func NewDataManager() *DataManager {
	return &DataManager{
		chunkServers:     []string{},
		serverSpaces:     &SafeMap{m: make(map[string]interface{})},
		serverLoads:      &SafeMap{m: make(map[string]interface{})},
		clientFileMap:    &SafeMap{m: make(map[string]interface{})},
		fileMetadata:     &SafeMap{m: make(map[string]interface{})},
		MaxChunksPerFile: 1000,
	}
}

// IsServerRegistered checks if a server is registered
func (dm *DataManager) IsServerRegistered(serverID string) bool {
	dm.serverSpaces.mu.RLock()
	defer dm.serverSpaces.mu.RUnlock()
	_, exists := dm.serverSpaces.m[serverID]
	return exists
}

// Remove Server removes a server from the data manager
func (dm *DataManager) RemoveServer(serverID string) {
	dm.serverSpaces.mu.Lock()
	defer dm.serverSpaces.mu.Unlock()
	for i, id := range dm.chunkServers {
		if id == serverID {
			dm.chunkServers = append(dm.chunkServers[:i], dm.chunkServers[i+1:]...)
			break
		}
	}
}

type FileMetadata struct {
	ID               string        `bson:"_id"`
	FileFormat       string        `bson:"file_format"`
	FileName         string        `bson:"file_name"`
	ClientId         string        `bson:"client_id"`
	TotalSize        int64         `bson:"total_size"`
	ChunkCount       int32         `bson:"chunk_count"`
	ChunkSizes       []int64       `bson:"chunk_sizes"`
	ChunkHashes      []string      `bson:"chunk_hashes"`
	Timestamp        int64         `bson:"timestamp"`
	Priority         int32         `bson:"priority"`
	RedundancyLevel  int32         `bson:"redundancy_level"`
	CompressionUsed  bool          `bson:"compression_used"`
	ChunkAssignments []ChunkPacket `bson:"chunk_assignments"`
}

type ClientResponse struct {
	FileID            string             `bson:"file_id"`
	ChunkAssignments  map[int32][]string `bson:"chunk_assignments"`
	ReplicationMap    map[int32][]string `bson:"replication_map"`
	Success           bool               `bson:"success"`
	Message           string             `bson:"message"`
	ResponseTimestamp int64              `bson:"response_timestamp"`
}

type ChunkReport struct {
	ChunkId   string `bson:"chunk_id"`
	ServerId  string `bson:"server_id"`
	Timestamp int64  `bson:"timestamp"`
}

// ServerStatus represents server status in MongoDB
type ServerStatus struct {
	ServerID      string   `bson:"server_id"`
	Address       string   `bson:"address"`
	FreeSpace     int64    `bson:"free_space"`
	TotalSpace    int64    `bson:"total_space"`
	CPUUsage      float32  `bson:"cpu_usage"`
	MemoryUsage   float32  `bson:"memory_usage"`
	NetworkUsage  float32  `bson:"network_usage"`
	Load          float32  `bson:"load"`
	ChunkIds      []string `bson:"chunk_ids"`
	LastHeartbeat int64    `bson:"last_heartbeat"`
	Active        bool     `bson:"active"`
	Score         float64  `bson:"score"`
}

func NewMasterServer() (*MasterServer, error) {
	ms := &MasterServer{
		serverAddresses: make(map[string]string),
	}
	dm := NewDataManager()
	ms.dataManager = dm
	hm := NewHeartbeatManager(ms)
	le := NewLeaderElector(hm)

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://mongodb:27017/gfs_db"
		log.Printf("📋 MONGO_URI not set, using default: %s", mongoURI)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Printf("❌ Failed to connect to MongoDB: %v", err)
		return nil, fmt.Errorf("connect to MongoDB: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Printf("❌ Failed to ping MongoDB: %v", err)
		return nil, fmt.Errorf("ping MongoDB: %v", err)
	}

	db := client.Database("gfs_db")
	log.Printf("✅ Connected to MongoDB: %s", mongoURI)

	executor := func(job Job) interface{} {
		log.Printf("👷 Worker executing job type: %v", job.Type)
		switch job.Type {
		case RegisterChunkServerJob:
			req, ok := job.Data.(*pb.RegisterChunkServerRequest)
			if !ok {
				log.Printf("❌ Invalid data for RegisterChunkServerJob")
				return JobResult{Success: false, Error: fmt.Errorf("invalid data type")}
			}
			log.Printf("📡 Registering server %s at %s", req.ServerId, req.Address)

			ms.dataManager.serverSpaces.Lock()
			log.Printf("🔒 Acquired lock for chunkServers check")
			for _, existing := range ms.dataManager.chunkServers {
				if existing == req.ServerId {
					ms.dataManager.serverSpaces.Unlock()
					log.Printf("⚠️ Server %s already registered, skipping", req.ServerId)
					job.Response <- JobResult{
						Success: true,
						Data:    &pb.RegisterChunkServerResponse{Success: true, Message: "Registered successfully"},
					}
				}
			}
			log.Printf("✅ Server %s not found, proceeding with registration", req.ServerId)
			ms.dataManager.chunkServers = append(ms.dataManager.chunkServers, req.ServerId)
			ms.serverAddresses[req.ServerId] = req.Address
			ms.dataManager.serverSpaces.m[req.ServerId] = int64(0)
			ms.dataManager.serverSpaces.Unlock()
			log.Printf("🔓 Released lock, updated chunkServers and serverAddresses")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			update := bson.M{
				"$set": bson.M{
					"server_id": req.ServerId,
					"address":   req.Address,
					"active":    true,
				},
			}
			log.Printf("📦 Preparing MongoDB update for server_status: %s", req.ServerId)
			result, err := db.Collection("server_status").UpdateOne(
				ctx,
				bson.M{"server_id": req.ServerId},
				update,
				options.Update().SetUpsert(true),
			)
			if err != nil {
				log.Printf("❌ Failed to update server_status for %s: %v", req.ServerId, err)
			} else {
				log.Printf("✅ Updated server_status for %s, modified: %d", req.ServerId, result.ModifiedCount+result.UpsertedCount)
			}

			log.Printf("✅ Registered server %s, total servers: %d", req.ServerId, len(ms.dataManager.chunkServers))
			job.Response <- JobResult{
				Success: true,
				Data:    &pb.RegisterChunkServerResponse{Success: true, Message: "Registered successfully"},
			}

		case RegisterFileJob:
			req, ok := job.Data.(*pb.RegisterFileRequest)
			if !ok {
				log.Printf("❌ Invalid data type for RegisterFileJob: %T", job.Data)
				return JobResult{Success: false, Error: fmt.Errorf("invalid data type for RegisterFileJob")}
			}
			log.Printf("[RegisterFileJob] 🔄 Registering file: %s | Chunks: %d", req.FileName, req.ChunkCount)

			resp, err := ms.processRegisterFileJob(req)
			if err != nil {
				log.Printf("❌ Failed to process RegisterFileJob: %v", err)
				return JobResult{Success: false, Error: err}
			}
			job.Response <- JobResult{
				Success: true,
				Data:    resp,
			}

		case ReportChunkJob:
			req, ok := job.Data.(*pb.ChunkReport)
			if !ok {
				log.Printf("❌ Invalid data for ReportChunkJob")
				return JobResult{Success: false, Error: fmt.Errorf("invalid data type")}
			}
			log.Printf("📦 Chunk %s reported by %s", req.ChunkId, req.ServerId)

			if !ms.dataManager.IsServerRegistered(req.ServerId) {
				log.Printf("⚠️ Unregistered server %s", req.ServerId)
				return JobResult{
					Success: false,
					Data:    &pb.ChunkResponse{Success: false, Message: "Unregistered server"},
					Error:   fmt.Errorf("server %s not registered", req.ServerId),
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			chunkReport := ChunkReport{
				ChunkId:   req.ChunkId,
				ServerId:  req.ServerId,
				Timestamp: time.Now().Unix(),
			}
			log.Printf("📦 Storing chunk report for %s", req.ChunkId)
			_, err := ms.db.Collection("chunk_reports").InsertOne(ctx, chunkReport)
			if err != nil {
				log.Printf("❌ Failed to store chunk report: %v", err)
				return JobResult{
					Success: false,
					Data:    &pb.ChunkResponse{Success: false, Message: "Failed to store report"},
					Error:   err,
				}
			}

			log.Printf("✅ Chunk %s stored", req.ChunkId)
			return JobResult{
				Success: true,
				Data:    &pb.ChunkResponse{Success: true, Message: "Chunk reported"},
			}

		case DeleteFileJob:
			req := job.Data.(*pb.DeleteFileRequest)
			log.Printf("📂 [DeleteFileJob] Starting deletion of '%s' for client '%s'", req.FileName, req.ClientId)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var metadata FileMetadata
			err := ms.db.Collection("file_metadata").FindOne(ctx, bson.M{"file_name": req.FileName}).Decode(&metadata)
			if err != nil {
				log.Printf("❌ [DeleteFileJob] File metadata not found for '%s': %v", req.FileName, err)
				job.Response <- JobResult{
					Success: false,
					Data: &pb.DeleteFileResponse{
						Success:  false,
						ClientId: req.ClientId,
						Message:  "File not found",
					},
					Error: fmt.Errorf("file not found"),
				}
				job.Response <- JobResult{
					Success: false,
					Error:   fmt.Errorf("file not found"),
					Data: &pb.DeleteFileResponse{
						Success:  false,
						ClientId: req.ClientId,
						Message:  "File not found",
					},
				}
			}

			log.Printf("🔍 [DeleteFileJob] Metadata found: %+v", metadata)
          
			// Step 1: Delete chunks from leader
			for index, assignment := range metadata.ChunkAssignments {
				log.Printf("%s_%d",assignment.ChunkHash,assignment.ChunkIndex)
				chunkID := fmt.Sprintf("%s_%d", assignment.ChunkHash, index)
				log.Printf("🗑️ [DeleteFileJob] Deleting chunk %s from leader %s", chunkID, assignment.LeaderAddress)

				go func(chunkID, leader string) {
					err := sendChunkDeleteRequest(leader, chunkID)
					if err != nil {
						log.Printf("⚠️ [DeleteFileJob] Failed to delete chunk %s from leader %s: %v", chunkID, leader, err)
					} else {
						log.Printf("✅ [DeleteFileJob] Chunk %s deleted from leader %s", chunkID, leader)
					}
				}(chunkID, assignment.LeaderAddress)

				replicaResp := make(chan JobResult, 1)
				replicaJob := Job{
					Type: DeleteReplicaJob,
					Data: ReplicaDeleteInfo{
						ChunkID:      chunkID,
						ReplicaAddrs: assignment.ReplicaAddresses,
					},
				}
				log.Printf("📨 [DeleteFileJob] Submitting DeleteReplicaJob for chunk %s", chunkID)
				ms.workerPool.SubmitJob(replicaJob)

				// 🔄 Wait for DeleteReplicaJob to finish
				go func(chunkID string, respChan chan JobResult) {
					result := <-respChan
					if result.Success {
						log.Printf("✅ [DeleteFileJob] All replicas deleted for chunk %s", chunkID)
					} else {
						log.Printf("⚠️ [DeleteFileJob] Replica deletion failed for chunk %s: %v", chunkID, result.Error)
					}
				}(chunkID, replicaResp)
			}

			// Step 3: Remove metadata from MongoDB
			log.Printf("🧹 [DeleteFileJob] Deleting file metadata for '%s' from database", req.FileName)
			_, err = ms.db.Collection("file_metadata").DeleteOne(ctx, bson.M{"file_name": req.FileName})
			if err != nil {
				log.Printf("⚠️ [DeleteFileJob] Failed to delete metadata for '%s': %v", req.FileName, err)
			} else {
				log.Printf("✅ [DeleteFileJob] Metadata deleted for '%s'", req.FileName)
			}

			// Step 4: Respond to client
			return JobResult{
				Success: true,
				Data: &pb.DeleteFileResponse{
					Success:  true,
					ClientId: req.ClientId,
					Message:  "File deletion initiated",
				},
			}

		case DeleteReplicaJob:
			replicaData := job.Data.(ReplicaDeleteInfo)
			chunkID := replicaData.ChunkID

			success := true
			var failedReplicas []string

			for _, replica := range replicaData.ReplicaAddrs {
				log.Printf("🗑️ [DeleteReplicaJob] Deleting replica chunk %s from %s", chunkID, replica)
				err := sendChunkDeleteRequest(replica, chunkID)
				if err != nil {
					success = false
					failedReplicas = append(failedReplicas, replica)
					log.Printf("⚠️ [DeleteReplicaJob] Failed to delete replica %s from %s: %v", chunkID, replica, err)
				} else {
					log.Printf("✅ [DeleteReplicaJob] Replica %s deleted from %s", chunkID, replica)
				}
			}

			if job.Response != nil {
				job.Response <- JobResult{
					Success: success,
					Error:   fmt.Errorf("failed replicas: %v", failedReplicas),
				}
			}

		case GetFileMetadataJob:
			req, ok := job.Data.(*pb.GetFileMetadataRequest)
			if !ok {
				log.Printf("❌ Invalid data type for GetFileMetadataJob: %T", job.Data)
				return JobResult{
					Success: false,
					Data: &pb.GetFileMetadataResponse{
						Success: false,
						Message: "Invalid data type",
					},
					Error: fmt.Errorf("invalid data type for GetFileMetadataJob"),
				}
			}

			log.Printf("🔍 Retrieving file metadata for %s", req.FileName)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var metadata FileMetadata
			err := ms.db.Collection("file_metadata").FindOne(ctx, bson.M{"file_name": req.FileName}).Decode(&metadata)
			if err != nil {
				log.Printf("⚠️ File not found: %s", req.FileName)
				return JobResult{
					Success: false,
					Data: &pb.GetFileMetadataResponse{
						Success: false,
						Message: "File not found",
					},
					Error: fmt.Errorf("file not found: %s", req.FileName),
				}
			}

			log.Printf("📋 File metadata retrieved: %+v", metadata)

			chunkHashes := make([]string, 0, metadata.ChunkCount)
			chunkAssignments := make(map[int32]*pb.ChunkServers)

			for i := int32(0); i < metadata.ChunkCount; i++ {
				packet := metadata.ChunkAssignments[i]
				chunkHashes = append(chunkHashes, packet.ChunkHash)

				chunkAssignments[i] = &pb.ChunkServers{
					Leader:   packet.LeaderAddress,
					Replicas: packet.ReplicaAddresses,
				}

				log.Printf("✅ Chunk %d - Hash: %s, Leader: %s, Replicas: %v", i, packet.ChunkHash, packet.LeaderAddress, packet.ReplicaAddresses)
			}

			return JobResult{
				Success: true,
				Data: &pb.GetFileMetadataResponse{
					FileFormat:       metadata.FileFormat,
					TotalSize:        metadata.TotalSize,
					ChunkCount:       metadata.ChunkCount,
					ChunkHashes:      chunkHashes,
					ChunkAssignments: chunkAssignments,
					ClientId:         req.ClientId,
					Success:          true,
					Message:          "Metadata retrieval successful",
				},
			}

		case HeartbeatJob:
			req, ok := job.Data.(*pb.HeartbeatRequest)
			if !ok {
				log.Printf("❌ Invalid data for HeartbeatJob")
				return JobResult{Success: false, Error: fmt.Errorf("invalid data type")}
			}
			log.Printf("💓 Heartbeat from server %s", req.ServerId)
			resp, err := ms.hm.SendHeartbeat(context.Background(), req)
			if err != nil {
				log.Printf("❌ Heartbeat failed: %v", err)
				return JobResult{
					Success: false,
					Data: &pb.HeartbeatResponse{
						Success: false,
						Message: "Heartbeat failed",
					},
					Error: err,
				}
			}
			log.Printf("✅ Heartbeat successful: %v", resp)
			return JobResult{Success: true, Data: resp}

		default:
			log.Printf("❌ Unknown job type: %v", job.Type)
			return JobResult{Success: false, Error: fmt.Errorf("unknown job type: %d", job.Type)}
		}
		return JobResult{Success: false, Error: fmt.Errorf("unhandled job type")}
	}
	wp := NewWorkerPool(10, 100, executor)
	go hm.RemoveInactiveServers()
	ms.hm = hm
	ms.le = le
	ms.mongoClient = client
	ms.db = db
	ms.workerPool = wp
	log.Println("✅ MasterServer initialized")
	return ms, nil
}

// Starts the gRPC server and listens for incoming connections
// Start runs the gRPC server
func (ms *MasterServer) Start() {
	listener, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("❌ Failed to listen on :50052: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServiceServer(grpcServer, ms)
	pb.RegisterHeartbeatServiceServer(grpcServer, ms.hm) // Register HeartbeatService

	log.Println("🚀 Master Server running on :50052")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("❌ Failed to serve gRPC: %v", err)
	}
}

func (ms *MasterServer) RegisterChunkServer(ctx context.Context, req *pb.RegisterChunkServerRequest) (*pb.RegisterChunkServerResponse, error) {
	log.Printf("📞 Received RegisterChunkServer for %s", req.ServerId)
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     RegisterChunkServerJob,
		Data:     req,
		Response: responseChan,
	}
	log.Printf("📤 Submitting RegisterChunkServerJob for %s", req.ServerId)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		res, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ Invalid result for RegisterChunkServer %s", req.ServerId)
			return nil, fmt.Errorf("invalid job result")
		}
		if !res.Success {
			log.Printf("❌ Register failed for %s: %v", req.ServerId, res.Error)
			return &pb.RegisterChunkServerResponse{Success: false, Message: res.Error.Error()}, res.Error
		}
		log.Printf("✅ RegisterChunkServer completed for %s", req.ServerId)
		return res.Data.(*pb.RegisterChunkServerResponse), nil
	case <-ctx.Done():
		log.Printf("❌ RegisterChunkServer timeout for %s: %v", req.ServerId, ctx.Err())
		return &pb.RegisterChunkServerResponse{Success: false, Message: "Job timeout"}, ctx.Err()
	}
}

func (ms *MasterServer) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	log.Printf("📞 Received RegisterFile for %s", req.FileName)
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     RegisterFileJob,
		Data:     req,
		Response: responseChan,
	}
	log.Printf("📤 Submitting RegisterFileJob for %s", req.FileName)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		res, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ Invalid result for RegisterFileJob %s", req.FileName)
			return nil, status.Errorf(codes.Internal, "invalid job result")
		}
		if !res.Success {
			log.Printf("❌ RegisterFileJob failed: %v", res.Error)
			return &pb.RegisterFileResponse{Success: false, Message: res.Error.Error()}, res.Error
		}
		log.Printf("✅ RegisterFileJob completed for %s", req.FileName)
		return res.Data.(*pb.RegisterFileResponse), nil
	case <-ctx.Done():
		log.Printf("❌ RegisterFileJob timeout: %v", ctx.Err())
		return nil, status.Errorf(codes.DeadlineExceeded, "job timeout")
	}
}

func (ms *MasterServer) ReportChunk(ctx context.Context, req *pb.ChunkReport) (*pb.ChunkResponse, error) {
	log.Printf("📞 Received ReportChunk for %s", req.ChunkId)
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     ReportChunkJob,
		Data:     req,
		Response: responseChan,
	}
	log.Printf("📤 Submitting ReportChunkJob for %s", req.ChunkId)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		res, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ Invalid result for ReportChunkJob %s", req.ChunkId)
			return &pb.ChunkResponse{Success: false, Message: "Invalid job result"}, fmt.Errorf("invalid job result")
		}
		if !res.Success {
			log.Printf("❌ ReportChunkJob failed: %v", res.Error)
			return &pb.ChunkResponse{Success: false, Message: res.Error.Error()}, res.Error
		}
		log.Printf("✅ ReportChunkJob completed for %s", req.ChunkId)
		return res.Data.(*pb.ChunkResponse), nil
	case <-ctx.Done():
		log.Printf("❌ ReportChunkJob timeout: %v", ctx.Err())
		return &pb.ChunkResponse{Success: false, Message: "Job timeout"}, ctx.Err()
	}
}

func (ms *MasterServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	log.Printf("📞 [DeleteFile] Request received | FileName: '%s', ClientID: '%s'", req.FileName, req.ClientId)

	// Create response channel
	responseChan := make(chan interface{}, 1)

	// Create DeleteFileJob
	job := Job{
		Type:     DeleteFileJob,
		Data:     req,
		Response: responseChan,
	}

	log.Printf("📤 [DeleteFile] Submitting DeleteFileJob to worker pool | FileName: '%s'", req.FileName)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		jobResult, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ [DeleteFile] Invalid job result format | FileName: '%s'", req.FileName)
			return &pb.DeleteFileResponse{
				Success:  false,
				ClientId: req.ClientId,
				Message:  "Internal error: invalid job result type",
			}, fmt.Errorf("invalid job result type")
		}

		if !jobResult.Success {
			log.Printf("❌ [DeleteFile] Job failed | FileName: '%s', Error: %v", req.FileName, jobResult.Error)
			return &pb.DeleteFileResponse{
				Success:  false,
				ClientId: req.ClientId,
				Message:  jobResult.Error.Error(),
			}, nil
		}

		log.Printf("✅ [DeleteFile] Job completed successfully | FileName: '%s'", req.FileName)
		return jobResult.Data.(*pb.DeleteFileResponse), nil

	case <-ctx.Done():
		log.Printf("⏳ [DeleteFile] Context timeout while waiting for job result | FileName: '%s', Error: %v", req.FileName, ctx.Err())
		return &pb.DeleteFileResponse{
			Success:  false,
			ClientId: req.ClientId,
			Message:  "Request timed out",
		}, ctx.Err()
	}
}

func sendChunkDeleteRequest(serverAddr, chunkID string) error {
	log.Printf("🌐 Connecting to chunk server at %s for deletion of chunk '%s'", serverAddr, chunkID)

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		log.Printf("🚫 Failed to connect to chunk server %s: %v", serverAddr, err)
		return fmt.Errorf("failed to connect to chunk server: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn) // ✅ Use correct client

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("📤 Sending DeleteChunk request for chunk ID '%s'", chunkID)
	resp, err := client.DeleteChunk(ctx, &pb.DeleteChunkRequest{ChunkId: chunkID})
	if err != nil {
		log.Printf("❌ gRPC error while deleting chunk '%s': %v", chunkID, err)
		return err
	}
	if !resp.Success {
		log.Printf("⚠️ Chunk server responded with failure for chunk '%s': %s", chunkID, resp.Message)
		return fmt.Errorf("server returned failure: %s", resp.Message)
	}

	log.Printf("✅ Successfully deleted chunk '%s' on server %s", chunkID, serverAddr)
	return nil
}


func (ms *MasterServer) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	log.Printf("📞 Received GetFileMetadata for %s from client %s", req.FileName, req.ClientId)

	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     GetFileMetadataJob,
		Data:     req,
		Response: responseChan,
	}

	log.Printf("📤 Submitting GetFileMetadataJob for %s", req.FileName)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		res, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ Invalid result for GetFileMetadataJob %s", req.FileName)
			return &pb.GetFileMetadataResponse{Success: false, Message: "Invalid job result"}, fmt.Errorf("invalid job result")
		}
		if !res.Success {
			log.Printf("❌ GetFileMetadataJob failed: %v", res.Error)
			return &pb.GetFileMetadataResponse{Success: false, Message: res.Error.Error()}, res.Error
		}
		log.Printf("✅ GetFileMetadataJob completed for %s", req.FileName)
		return res.Data.(*pb.GetFileMetadataResponse), nil

	case <-ctx.Done():
		log.Printf("❌ GetFileMetadataJob timeout: %v", ctx.Err())
		return &pb.GetFileMetadataResponse{Success: false, Message: "Job timeout"}, ctx.Err()
	}
}

// SendHeartbeat handles heartbeat requests
func (ms *MasterServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.Printf("📞 Received Heartbeat for %s", req.ServerId)
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     HeartbeatJob,
		Data:     req,
		Response: responseChan,
	}
	log.Printf("📤 Submitting HeartbeatJob for %s", req.ServerId)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		res, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ Invalid result for HeartbeatJob %s", req.ServerId)
			return &pb.HeartbeatResponse{Success: false, Message: "Invalid job result"}, fmt.Errorf("invalid job result")
		}
		if !res.Success {
			log.Printf("❌ HeartbeatJob failed for %s: %v", req.ServerId, res.Error)
			return &pb.HeartbeatResponse{Success: false, Message: res.Error.Error()}, res.Error
		}
		log.Printf("✅ HeartbeatJob completed for %s", req.ServerId)
		return res.Data.(*pb.HeartbeatResponse), nil
	case <-ctx.Done():
		log.Printf("❌ HeartbeatJob timeout for %s: %v", req.ServerId, ctx.Err())
		return &pb.HeartbeatResponse{Success: false, Message: "Job timeout"}, ctx.Err()
	}
}

// processRegisterFileJob processes file registration
func (ms *MasterServer) processRegisterFileJob(req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	log.Printf("📝 Processing file %s, client=%s, size=%d, chunks=%d",
		req.FileName, req.ClientId, req.TotalSize, req.ChunkCount)

	// Validate input
	if req.FileName == "" || req.ClientId == "" {
		log.Printf("❌ Invalid arguments: file_name or client_id empty")
		return nil, status.Errorf(codes.InvalidArgument, "file_name or client_id empty")
	}
	if req.ChunkCount <= 0 || len(req.ChunkSizes) != int(req.ChunkCount) || len(req.ChunkHashes) != int(req.ChunkCount) {
		log.Printf("❌ Invalid chunk data: count=%d, sizes=%d, hashes=%d", req.ChunkCount, len(req.ChunkSizes), len(req.ChunkHashes))
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk count or sizes/hashes")
	}

	// Register file metadata
	log.Printf("📦 Registering file metadata for %s", req.FileName)
	fileID, err := ms.dataManager.RegisterFile(req)
	if err != nil {
		log.Printf("❌ Failed to register file: %v", err)
		return nil, status.Errorf(codes.AlreadyExists, err.Error())
	}

	// Assign chunks to servers
	log.Printf("📦 Assigning chunks for file %s", fileID)
	assignments, err := ms.assignChunks(req, fileID)
	if err != nil {
		log.Printf("❌ Failed to assign chunks: %v", err)
		ms.dataManager.clientFileMap.Lock()
		delete(ms.dataManager.clientFileMap.m, fmt.Sprintf("%s_%s", req.ClientId, req.FileName))
		ms.dataManager.clientFileMap.Unlock()
		ms.dataManager.fileMetadata.Lock()
		delete(ms.dataManager.fileMetadata.m, fileID)
		ms.dataManager.fileMetadata.Unlock()
		return nil, status.Errorf(codes.Internal, "failed to assign chunks: %v", err)
	}

	// Construct the FileMetadata object
	fileMetadata := FileMetadata{
		ID:               fileID,
		FileName:         req.FileName,
		ClientId:         req.ClientId,
		TotalSize:        req.TotalSize,
		ChunkCount:       req.ChunkCount,
		ChunkSizes:       req.ChunkSizes,
		ChunkHashes:      req.ChunkHashes,
		Timestamp:        req.Timestamp,
		Priority:         req.Priority,
		RedundancyLevel:  req.RedundancyLevel,
		CompressionUsed:  req.CompressionUsed,
		ChunkAssignments: assignments.packets,
	}

	// Store in MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("📦 Storing file metadata in MongoDB for %s", fileID)
	_, err = ms.db.Collection("file_metadata").InsertOne(ctx, fileMetadata)
	if err != nil {
		log.Printf("❌ Failed to store file metadata: %v", err)
		ms.dataManager.clientFileMap.Lock()
		delete(ms.dataManager.clientFileMap.m, fmt.Sprintf("%s_%s", req.ClientId, req.FileName))
		ms.dataManager.clientFileMap.Unlock()
		ms.dataManager.fileMetadata.Lock()
		delete(ms.dataManager.fileMetadata.m, fileID)
		ms.dataManager.fileMetadata.Unlock()
		return nil, status.Errorf(codes.Internal, "store file metadata: %v", err)
	}

	// Construct response
	log.Printf("✅ Registered file %s successfully", fileID)
	return &pb.RegisterFileResponse{
		FileId:           fileID,
		ChunkAssignments: assignments.chunkAssignments, // Needed by client
		Success:          true,
		Message:          "File registered successfully",
	}, nil
}

func (ms *MasterServer) assignChunks(req *pb.RegisterFileRequest, fileID string) (*chunkAssignments, error) {
	log.Printf("📦 Assigning chunks for %s, size=%d, chunks=%d", fileID, req.TotalSize, req.ChunkCount)

	if len(req.ChunkSizes) != int(req.ChunkCount) || len(req.ChunkHashes) != int(req.ChunkCount) {
		log.Printf("❌ Mismatched chunk sizes/hashes: %d vs %d", len(req.ChunkSizes), req.ChunkCount)
		return nil, fmt.Errorf("mismatched chunk sizes/hashes: %d vs %d", len(req.ChunkSizes), req.ChunkCount)
	}

	servers := ms.hm.GetActiveChunkServers(ms.dataManager.chunkServers)
	log.Printf("📋 Active servers: %v", servers)
	addresses := make(map[string]string)
	for k, v := range ms.serverAddresses {
		addresses[k] = v
	}

	ms.dataManager.serverLoads.RLock()
	loads := make(map[string]int64)
	for k, v := range ms.dataManager.serverLoads.m {
		loads[k] = v.(int64)
	}
	ms.dataManager.serverLoads.RUnlock()

	ms.dataManager.serverSpaces.RLock()
	spaces := make(map[string]int64)
	for k, v := range ms.dataManager.serverSpaces.m {
		if val, ok := v.(int64); ok {
			spaces[k] = val * 1024 * 1024 // GB to bytes
		} else {
			log.Printf("⚠️ Could not cast free space value for %s", k)
		}
	}
	ms.dataManager.serverSpaces.RUnlock()

	assignments := &chunkAssignments{
		packets:          make([]ChunkPacket, 0, req.ChunkCount),
		chunkAssignments: make(map[int32]*pb.ChunkServers),
	}

	for i := int32(0); i < req.ChunkCount; {
		chunkSize := req.ChunkSizes[i]
		chunkHash := req.ChunkHashes[i]
		log.Printf("⏳ Assigning chunk %d, size=%d bytes", chunkHash, chunkSize)

		if chunkSize <= 0 {
			log.Printf("❌ Invalid chunk size %d for chunk %d", chunkSize, i)
			return nil, fmt.Errorf("invalid chunk size %d for chunk %d", chunkSize, i)
		}

		leaderID := ms.le.ElectLeader(chunkSize, servers, loads, spaces)
		if leaderID == "" {
			log.Printf("❌ No leader for chunk %d", i)
			return nil, fmt.Errorf("no leader for chunk %d", i)
		}

		leaderAddr, exists := addresses[leaderID]
		log.Printf("📦 Leader %s for chunk %d, address=%s", leaderID, i, leaderAddr)
		if !exists {
			log.Printf("❌ No address for leader %s", leaderID)
			return nil, fmt.Errorf("no address for leader %s", leaderID)
		}

		remainingChunks := req.ChunkSizes[i:]
		maxChunks := ms.dataManager.MaxChunksForServer(ms, spaces, leaderID, remainingChunks)
		if maxChunks == 0 {
			log.Printf("❌ No space for chunk %d on %s", i, leaderID)
			return nil, fmt.Errorf("no space for chunk %d on %s", i, leaderID)
		}

		chunksToAssign := maxChunks
		if int32(chunksToAssign) > req.ChunkCount-i {
			chunksToAssign = int(req.ChunkCount - i)
		}

		for j := int32(0); j < int32(chunksToAssign); j++ {
			chunkIndex := i + j
			if chunkIndex >= req.ChunkCount {
				break
			}
			chunkSize = req.ChunkSizes[chunkIndex]
			chunkHash := req.ChunkHashes[chunkIndex]

			log.Printf("📋 Selecting replicas for chunk %d, hash=%s", chunkIndex, chunkHash)
			replicas := ms.le.SelectReplicas(leaderID, 2, servers, chunkSize, spaces)
			if len(replicas) < 2 {
				log.Printf("❌ Not enough replicas for chunk %d", chunkIndex)
				return nil, fmt.Errorf("not enough replicas for chunk %d", chunkIndex)
			}

			replicaAddrs := make([]string, 0, len(replicas))
			for _, replicaID := range replicas {
				addr, exists := addresses[replicaID]
				if !exists {
					log.Printf("❌ No address for replica %s", replicaID)
					return nil, fmt.Errorf("no address for replica %s", replicaID)
				}
				replicaAddrs = append(replicaAddrs, addr)
			}

			packet := NewChunkPacket(fileID, chunkIndex, leaderAddr, replicaAddrs, chunkHash)
			assignments.packets = append(assignments.packets, packet)
			assignments.chunkAssignments[chunkIndex] = packet.ToProtoChunkServers()

			log.Printf("✅ Assigned chunk %d to leader %s, replicas %v", chunkIndex, leaderID, replicas)

			ms.dataManager.UpdateLoad(leaderID, chunkSize)
			for _, replicaID := range replicas {
				ms.dataManager.UpdateLoad(replicaID, chunkSize)
			}
		}

		i += int32(chunksToAssign)
	}

	log.Printf("🎉 Assigned %d chunks for %s", len(assignments.chunkAssignments), fileID)
	return assignments, nil
}

func (dm *DataManager) MaxChunksForServer(ms *MasterServer, spaces map[string]int64, serverID string, chunkSizes []int64) int {
	// Check if server is active (has sent a heartbeat)
	if !ms.hm.IsChunkServerActive(serverID) {
		log.Printf("⚠️ Server %s is not active (no heartbeat)", serverID)
		return 0
	}

	dm.serverSpaces.RLock()
	defer dm.serverSpaces.RUnlock()

	freeSpace, exists := spaces[serverID]
	if !exists {
		log.Printf("⚠️ No space data for %s (awaiting heartbeat)", serverID)
		return 0
	}

	fs := freeSpace
	count := 0
	for i, size := range chunkSizes {
		if size <= 0 {
			log.Printf("⚠️ Invalid chunk size %d at index %d", size, i)
			continue
		}
		if fs >= size {
			fs -= size
			count++
			log.Printf("✅ Server %s can store chunk %d: size=%d, remaining=%d", serverID, i, size, fs)
		} else {
			log.Printf("⚠️ Server %s out of space: needed=%d, available=%d", serverID, size, fs)
			break
		}
	}

	log.Printf("✅ Server %s can handle %d chunks", serverID, count)
	return count
}

// chunkAssignments holds chunk assignment data
type chunkAssignments struct {
	packets          []ChunkPacket
	chunkAssignments map[int32]*pb.ChunkServers
}
