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

// convertToReplicaServers converts a map[int32]*pb.ChunkServers to map[int32]*pb.ReplicaServers
func convertToReplicaServers(chunkServersMap map[int32]*pb.ChunkServers) map[int32]*pb.ReplicaServers {
	replicaServersMap := make(map[int32]*pb.ReplicaServers)
	for key, chunkServers := range chunkServersMap {
		replicaServersMap[key] = &pb.ReplicaServers{Servers: chunkServers.Servers}
	}
	return replicaServersMap
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
	ID               string             `bson:"_id"`
	FileName         string             `bson:"file_name"`
	ClientId         string             `bson:"client_id"`
	TotalSize        int64              `bson:"total_size"`
	ChunkCount       int32              `bson:"chunk_count"`
	ChunkSizes       []int64            `bson:"chunk_sizes"`
	ChunkHashes      []string           `bson:"chunk_hashes"`
	Timestamp        int64              `bson:"timestamp"`
	Priority         int32              `bson:"priority"`
	RedundancyLevel  int32              `bson:"redundancy_level"`
	CompressionUsed  bool               `bson:"compression_used"`
	ChunkAssignments []ChunkPacket      `bson:"chunk_assignments"`
	ReplicationMap   map[int32][]string `bson:"replication_map"`
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
					return JobResult{
						Success: true,
						Data:    &pb.RegisterChunkServerResponse{Success: true, Message: "Already registered"},
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
			return JobResult{
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
			return JobResult{
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

		case GetChunkLocationsJob:
			req, ok := job.Data.(*pb.GetChunkRequest)
			if !ok {
				log.Printf("❌ Invalid data type for GetChunkLocationsJob: %T", job.Data)
				return JobResult{
					Success: false,
					Data: &pb.GetChunkResponse{
						Success: false,
						Message: "Invalid data type",
					},
					Error: fmt.Errorf("invalid data type for GetChunkLocationsJob"),
				}
			}
			log.Printf("[GetChunkLocationsJob] 🔍 Getting chunk locations for %s", req.FileName)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var metadata FileMetadata
			log.Printf("📦 Querying file_metadata for %s", req.FileName)
			err := ms.db.Collection("file_metadata").FindOne(ctx, bson.M{"file_name": req.FileName}).Decode(&metadata)
			if err != nil {
				log.Printf("⚠️ No file metadata found for %s: %v", req.FileName, err)
				return JobResult{
					Success: false,
					Data: &pb.GetChunkResponse{
						Success: false,
						Message: "File not found",
					},
					Error: fmt.Errorf("file not found: %s", req.FileName),
				}
			}
			log.Printf("[GetChunkLocationsJob] 📋 File metadata found: %v", metadata)

			chunkLocations := make([]*pb.ChunkLocation, 0, metadata.ChunkCount)
			for i := int32(0); i < metadata.ChunkCount; i++ {
				packet := metadata.ChunkAssignments[i]

				activeServers := make([]string, 0, len(packet.ReplicaAddresses)+1)
				if ms.dataManager.IsServerRegistered(packet.LeaderAddress) {
					activeServers = append(activeServers, packet.LeaderAddress)
				}
				for _, replica := range packet.ReplicaAddresses {
					if ms.dataManager.IsServerRegistered(replica) {
						activeServers = append(activeServers, replica)
					}
				}

				if len(activeServers) == 0 {
					log.Printf("⚠️ No active servers for chunk %s_%d", req.FileName, i)
					continue
				}

				chunkID := packet.ChunkName
				chunkSize := packet.ChunkSize
				chunkHash := packet.ChunkHash // Use chunkHash
				chunkLocations = append(chunkLocations, &pb.ChunkLocation{
					ChunkId:   chunkID,
					Servers:   activeServers,
					ChunkHash: chunkHash,
					ChunkSize: chunkSize,
				})
				log.Printf("✅ Added chunk %s: %d bytes, servers %v", chunkID, chunkSize, activeServers)
			}
			if len(chunkLocations) == 0 {
				log.Printf("⚠️ No chunk locations found for %s", req.FileName)
				return JobResult{
					Success: false,
					Data: &pb.GetChunkResponse{
						Success: false,
						Message: "No chunk locations found",
					},
					Error: fmt.Errorf("no chunk locations found for %s", req.FileName),
				}
			}

			log.Printf("[GetChunkLocationsJob] ✅ Found chunk locations: %v", chunkLocations)
			return JobResult{
				Success: true,
				Data: &pb.GetChunkResponse{
					FileId:         metadata.ID,
					ChunkLocations: chunkLocations,
					Success:        true,
					Message:        "Chunk locations retrieved successfully",
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

// GetChunkLocations retrieves chunk locations
func (ms *MasterServer) GetChunkLocations(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	log.Printf("📞 Received GetChunkLocations for %s", req.FileName)
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     GetChunkLocationsJob,
		Data:     req,
		Response: responseChan,
	}
	log.Printf("📤 Submitting GetChunkLocationsJob for %s", req.FileName)
	ms.workerPool.SubmitJob(job)

	select {
	case result := <-responseChan:
		res, ok := result.(JobResult)
		if !ok {
			log.Printf("❌ Invalid result for GetChunkLocationsJob %s", req.FileName)
			return &pb.GetChunkResponse{Success: false, Message: "Invalid job result"}, fmt.Errorf("invalid job result")
		}
		if !res.Success {
			log.Printf("❌ GetChunkLocationsJob failed: %v", res.Error)
			return &pb.GetChunkResponse{Success: false, Message: res.Error.Error()}, res.Error
		}
		log.Printf("✅ GetChunkLocationsJob completed for %s", req.FileName)
		return res.Data.(*pb.GetChunkResponse), nil
	case <-ctx.Done():
		log.Printf("❌ GetChunkLocationsJob timeout: %v", ctx.Err())
		return &pb.GetChunkResponse{Success: false, Message: "Job timeout"}, ctx.Err()
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

	if req.FileName == "" || req.ClientId == "" {
		log.Printf("❌ Invalid arguments: file_name or client_id empty")
		return nil, status.Errorf(codes.InvalidArgument, "file_name or client_id empty")
	}
	if req.ChunkCount <= 0 || len(req.ChunkSizes) != int(req.ChunkCount) || len(req.ChunkHashes) != int(req.ChunkCount) {
		log.Printf("❌ Invalid chunk data: count=%d, sizes=%d, hashes=%d", req.ChunkCount, len(req.ChunkSizes), len(req.ChunkHashes))
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk count or sizes/hashes")
	}

	log.Printf("📦 Registering file metadata for %s", req.FileName)
	fileID, err := ms.dataManager.RegisterFile(req)
	if err != nil {
		log.Printf("❌ Failed to register file: %v", err)
		return nil, status.Errorf(codes.AlreadyExists, err.Error())
	}

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

	chunkAssignments := make(map[int32][]string)
	replicationMap := make(map[int32][]string)
	for idx, servers := range assignments.chunkAssignments {
		chunkAssignments[idx] = servers.Servers
	}
	for idx, servers := range assignments.replicationMap {
		replicationMap[idx] = servers.Servers
	}

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
		ReplicationMap:   replicationMap,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log.Printf("📦 Storing file metadata for %s", fileID)
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

	// Removed the unused response variable to fix the compile error
	log.Printf("📦 Storing client response for %s", fileID)
	// Removed the misplaced line causing the error
	if err != nil {
		log.Printf("⚠️ Failed to store client response: %v", err)
	}

	log.Printf("✅ Registered file %s", fileID)
	return &pb.RegisterFileResponse{
		FileId:           fileID,
		ChunkAssignments: assignments.chunkAssignments,
		ReplicationMap:   convertToReplicaServers(assignments.replicationMap),
		Success:          true,
		Message:          "File registered successfully",
	}, nil
}

// assignChunks assigns chunks to servers
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
		replicationMap:   make(map[int32]*pb.ChunkServers),
	}

	for i := int32(0); i < req.ChunkCount; {
		chunkSize := req.ChunkSizes[i]
		log.Printf("⏳ Assigning chunk %d, size=%d bytes", i, chunkSize)

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
		maxChunks := ms.dataManager.MaxChunksForServer(ms, leaderID, remainingChunks)
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

			log.Printf("📋 Selecting replicas for chunk %d", chunkIndex, chunkHash)
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

			packet := NewChunkPacket(fileID, chunkIndex, leaderAddr, replicaAddrs, req)
			assignments.packets = append(assignments.packets, packet)
			assignments.chunkAssignments[chunkIndex] = packet.ToProtoChunkServers()
			assignments.replicationMap[chunkIndex] = &pb.ChunkServers{Servers: packet.ToProtoReplicaServers()}

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

// MaxChunksForServer calculates max chunks a server can handle
func (dm *DataManager) MaxChunksForServer(ms *MasterServer, serverID string, chunkSizes []int64) int {
	// Check if server is active (has sent a heartbeat)
	if !ms.hm.IsChunkServerActive(serverID) {
		log.Printf("⚠️ Server %s is not active (no heartbeat)", serverID)
		return 0
	}

	dm.serverSpaces.RLock()
	defer dm.serverSpaces.RUnlock()

	freeSpace, exists := dm.serverSpaces.m[serverID]
	if !exists {
		log.Printf("⚠️ No space data for %s (awaiting heartbeat)", serverID)
		return 0
	}

	fs := freeSpace.(int64)
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
	replicationMap   map[int32]*pb.ChunkServers
}
