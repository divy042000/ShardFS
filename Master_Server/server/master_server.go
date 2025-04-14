package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
    "time"
	"string"
	"google.golang.org/grpc"
    "go.mongodb.org/mongo-driver/bson"	
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status" 
	
	pb "master_server/proto"

)

type MasterServer struct {
	pb.UnimplementedMasterServiceServer
	mu               sync.Mutex
	chunkTable       map[string][]string
	heartbeatManager *HeartbeatManager
	le               *LeaderElector
	rs               *ReplicaSelector
	dataManager      *DataManager
	chunkManager     *ChunkManager
	workerPool       *WorkerPool
	serverAddresses  map[string]string // Added to store server addresses
}

// NewMasterServer initializes the Master Server with optional chunk servers
func NewMasterServer(storageDir string) *MasterServer {
	hm := NewHeartbeatManager()
	dm := NewDataManager([]string{}) // Start with empty chunkServers
	cm := NewChunkManager(storageDir)

	ms := &MasterServer{
		chunkTable:       make(map[string][]string),
		heartbeatManager: hm,
		le:               NewLeaderElector(hm),
		rs:               NewReplicaSelector(),
		dataManager:      dm,
		chunkManager:     cm,
		serverAddresses:  make(map[string]string),
	}

	// 🧠 WorkerPool Executor
	executor := func(job Job) interface{} {
		log.Printf("👷 Worker executing job type: %v", job.Type)

		switch job.Type {
		// ──────────────── Register Chunk Server ────────────────
		case RegisterChunkServerJob:
			req := job.Data.(*pb.RegisterChunkServerRequest)
			log.Printf("[RegisterChunkServerJob] ServerId=%s, Address=%s", req.ServerId, req.Address)

			ms.mu.Lock()
			defer ms.mu.Unlock()

			ms.dataManager.mu.Lock()
			for _, existing := range ms.dataManager.chunkServers {
				if existing == req.ServerId {
					ms.dataManager.mu.Unlock()
					log.Printf("⚠️ Chunk server %s already registered", req.ServerId)
					return JobResult{Success: true, Data: &pb.RegisterChunkServerResponse{Success: true, Message: "Already registered"}}
				}
			}
			ms.dataManager.chunkServers = append(ms.dataManager.chunkServers, req.ServerId)
			ms.serverAddresses[req.ServerId] = req.Address
			ms.dataManager.mu.Unlock()

			log.Printf("✅ Registered chunk server %s at %s | Total registered: %d", req.ServerId, req.Address, len(ms.dataManager.chunkServers))

			return JobResult{Success: true, Data: &pb.RegisterChunkServerResponse{
				Success: true,
				Message: "Registered successfully",
			}}

		// ──────────────── Register File ────────────────
		case RegisterFileJob:
			req := job.Data.(*pb.RegisterFileRequest)
			log.Printf("[RegisterFileJob] 🔄 Registering file: %s | Chunks: %d", req.FileName, req.ChunkCount)

			ms.mu.Lock()
			defer ms.mu.Unlock()

			log.Printf("[RegisterFileJob] 📋 Known servers: %v", ms.dataManager.chunkServers)

			activeServers := ms.heartbeatManager.GetActiveChunkServers(ms.dataManager.chunkServers)
			log.Printf("[RegisterFileJob] ✅ Active chunk servers: %v", activeServers)

			fileID, err := ms.dataManager.RegisterFile(req)
			if err != nil {
				log.Printf("❌ Failed to register file in DataManager: %v", err)
				return JobResult{Success: false, Error: err}
			}
			log.Printf("[RegisterFileJob] 🆔 File ID generated: %s", fileID)

			log.Println("[RegisterFileJob] 🚀 Calling assignChunks()...")
			assignments, err := ms.assignChunks(req, fileID)
			if err != nil {
				log.Printf("❌ assignChunks failed: %v", err)
				return JobResult{Success: false, Error: err}
			}
			log.Println("[RegisterFileJob] ✅ assignChunks completed")

			err = ms.chunkManager.StoreAndSerialize(fileID, req, assignments.packets)
			if err != nil {
				log.Printf("❌ Failed to serialize chunk metadata: %v", err)
				return JobResult{Success: false, Error: err}
			}
			log.Printf("[RegisterFileJob] 💾 Chunk metadata serialized for fileID: %s", fileID)

			for _, packet := range assignments.packets {
				chunkID := packet.ChunkName
				servers := append([]string{packet.LeaderAddress}, packet.ReplicaAddresses...)
				ms.chunkTable[chunkID] = servers
				log.Printf("[RegisterFileJob] 📌 Chunk %s assigned to %v", chunkID, servers)
			}

			return JobResult{
				Success: true,
				Data: &pb.RegisterFileResponse{
					FileId:           fileID,
					LeaderServer:     assignments.chunkAssignments[0].Servers[0],
					ChunkAssignments: assignments.chunkAssignments,
					ReplicationMap:   convertReplicationMap(assignments.replicationMap),
					Success:          true,
					Message:          "File registered successfully",
				},
			}

		// ──────────────── Report Chunk ────────────────
		case ReportChunkJob:
			req := job.Data.(*pb.ChunkReport)
			ms.mu.Lock()
			defer ms.mu.Unlock()

			if !ms.heartbeatManager.IsChunkServerActive(req.ServerId) {
				log.Printf("⚠️ Ignoring chunk report from inactive server %s", req.ServerId)
				return JobResult{Success: false, Data: &pb.ChunkResponse{Success: false, Message: "Inactive server"}}
			}

			ms.chunkTable[req.ChunkId] = append(ms.chunkTable[req.ChunkId], req.ServerId)
			log.Printf("📥 Chunk %s reported by server %s", req.ChunkId, req.ServerId)

			return JobResult{Success: true, Data: &pb.ChunkResponse{Success: true, Message: "Chunk reported successfully"}}

		// ──────────────── Get Chunk Locations ────────────────
		case GetChunkLocationsJob:
			req := job.Data.(*pb.GetChunkRequest)
			ms.mu.Lock()
			defer ms.mu.Unlock()

			chunkID := req.FileName + "_" + fmt.Sprint(req.ChunkIndex)
			servers, exists := ms.chunkTable[chunkID]
			if !exists {
				log.Printf("⚠️ No chunk locations found for %s", chunkID)
				return JobResult{Success: false, Data: &pb.GetChunkResponse{Success: false}}
			}

			active := ms.heartbeatManager.GetActiveChunkServers(servers)
			if len(active) == 0 {
				log.Printf("⚠️ All replicas for chunk %s are inactive", chunkID)
				return JobResult{Success: false, Data: &pb.GetChunkResponse{Success: false, Message: "No active servers"}}
			}

			return JobResult{
				Success: true,
				Data: &pb.GetChunkResponse{
					ChunkId:      chunkID,
					ChunkServers: active,
					Success:      true,
				},
			}

		default:
			log.Printf("❌ Unknown job type: %v", job.Type)
			return JobResult{Success: false, Error: fmt.Errorf("unknown job type: %d", job.Type)}
		}
	}

	// ✅ Initialize the WorkerPool and inject executor
	ms.workerPool = NewWorkerPool(4, 100, executor)

	// Link back to master in HeartbeatManager
	hm.ms = ms

	log.Println("✅ MasterServer initialized")
	return ms
}

func (ms *MasterServer) Start() {
	listener, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen on port 50052: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServiceServer(grpcServer, ms)
	pb.RegisterHeartbeatServiceServer(grpcServer, ms.heartbeatManager)

	go ms.heartbeatManager.RemoveInactiveServers()

	log.Println("🚀 Master Server running on port 50052")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// Updated gRPC methods to use WorkerPool
// RegisterChunkServer handles chunk server registration
func (ms *MasterServer) RegisterChunkServer(ctx context.Context, req *pb.RegisterChunkServerRequest) (*pb.RegisterChunkServerResponse, error) {
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     RegisterChunkServerJob,
		Data:     req,
		Response: responseChan,
	}
	ms.workerPool.SubmitJob(job)
	result := <-responseChan
	res := result.(JobResult)
	if !res.Success {
		log.Printf("Registration failed for ServerId=%s: %v", req.ServerId, res.Error)
		return &pb.RegisterChunkServerResponse{Success: false, Message: res.Error.Error()}, res.Error
	}
	return res.Data.(*pb.RegisterChunkServerResponse), nil
}

func (ms *MasterServer) GetChunkLocations(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     GetChunkLocationsJob,
		Data:     req,
		Response: responseChan,
	}
	ms.workerPool.SubmitJob(job)
	result := <-responseChan
	res := result.(JobResult)
	if !res.Success {
		return res.Data.(*pb.GetChunkResponse), res.Error
	}
	return res.Data.(*pb.GetChunkResponse), nil
}

func (ms *MasterServer) ReportChunk(ctx context.Context, req *pb.ChunkReport) (*pb.ChunkResponse, error) {
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     ReportChunkJob,
		Data:     req,
		Response: responseChan,
	}
	ms.workerPool.SubmitJob(job)
	result := <-responseChan
	res := result.(JobResult)
	if !res.Success {
		return res.Data.(*pb.ChunkResponse), res.Error
	}
	return res.Data.(*pb.ChunkResponse), nil
}

func (ms *MasterServer) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	responseChan := make(chan interface{}, 1)
	job := Job{
		Type:     RegisterFileJob,
		Data:     req,
		Response: responseChan,
	}
	ms.workerPool.SubmitJob(job)
	result := <-responseChan
	res := result.(JobResult)
	if !res.Success {
		return &pb.RegisterFileResponse{Success: false, Message: res.Error.Error()}, res.Error
	}
	return res.Data.(*pb.RegisterFileResponse), nil
}

func (ms *MasterServer) assignChunks(req *pb.RegisterFileRequest, fileID string) (*chunkAssignments, error) {
	log.Printf("📦 Starting chunk assignment for file: %s, TotalSize: %d, ChunkCount: %d", fileID, req.TotalSize, req.ChunkCount)

	// Validate inputs
	if len(req.ChunkSizes) != int(req.ChunkCount) {
		log.Printf("❌ Invalid ChunkSizes: length %d, expected %d", len(req.ChunkSizes), req.ChunkCount)
		return nil, fmt.Errorf("ChunkSizes length %d does not match chunk_count %d", len(req.ChunkSizes), req.ChunkCount)
	}
	if len(req.ChunkHashes) != int(req.ChunkCount) {
		log.Printf("❌ Invalid ChunkHashes: length %d, expected %d", len(req.ChunkHashes), req.ChunkCount)
		return nil, fmt.Errorf("ChunkHashes length %d does not match chunk_count %d", len(req.ChunkHashes), req.ChunkCount)
	}

	// Copy servers and addresses
	servers := append([]string{}, ms.dataManager.chunkServers...)
	addresses := make(map[string]string)
	for k, v := range ms.serverAddresses {
		addresses[k] = v
	}

	// Copy loads and spaces
	ms.dataManager.serverLoads.RLock()
	log.Println("🔒 Acquired RLock on serverLoads")
	loads := make(map[string]int64)
	for k, v := range ms.dataManager.serverLoads.m {
		loads[k] = v
	}
	ms.dataManager.serverLoads.RUnlock()
	log.Println("🔓 Released RLock on serverLoads")

	ms.dataManager.serverSpaces.RLock()
	log.Println("🔒 Acquired RLock on serverSpaces")
	spaces := make(map[string]int64)
	for k, v := range ms.dataManager.serverSpaces.m {
		spaces[k] = v * 1024 * 1024 // Convert MB to bytes
	}
	ms.dataManager.serverSpaces.RUnlock()
	log.Println("🔓 Released RLock on serverSpaces")

	log.Printf("🧩 Current servers: %v", servers)
	log.Printf("📊 Current server loads: %v", loads)
	log.Printf("📦 Current server spaces: %v", spaces)
	log.Printf("🌐 Server addresses: %v", addresses)

	assignments := &chunkAssignments{
		packets:          make([]ChunkPacket, 0, req.ChunkCount),
		chunkAssignments: make(map[int32]*pb.ChunkServers),
		replicationMap:   make(map[int32][]string),
	}

	for i := int32(0); i < req.ChunkCount; {
		chunkSize := req.ChunkSizes[i]
		log.Printf("⏳ Assigning chunk %d (Size: %d bytes, %.2f MB, Hash: %s)", i, chunkSize, float64(chunkSize)/1024/1024, req.ChunkHashes[i])

		if chunkSize <= 0 {
			log.Printf("❌ Invalid chunk size %d for chunk %d", chunkSize, i)
			return nil, fmt.Errorf("invalid chunk size %d for chunk %d", chunkSize, i)
		}

		// Elect leader
		leaderID := ms.le.ElectLeader(chunkSize, servers, loads, spaces)
		log.Printf("👑 Elected leader: %s for chunk %d", leaderID, i)
		if leaderID == "" {
			log.Printf("❌ No leader elected for chunk %d. Size: %d, Servers: %v, Loads: %v, Spaces: %v", i, chunkSize, servers, loads, spaces)
			return nil, fmt.Errorf("no suitable leader found for chunk %d", i)
		}

		leaderAddr, exists := addresses[leaderID]
		if !exists {
			log.Printf("❌ No address found for leader %s for chunk %d", leaderID, i)
			return nil, fmt.Errorf("no address for leader %s for chunk %d", leaderID, i)
		}
		log.Printf("📍 Leader %s has address %s", leaderID, leaderAddr)

		// Compute max chunks for remaining chunk sizes
		remainingChunks := req.ChunkSizes[i:]
		maxChunks := ms.dataManager.MaxChunksForServer(leaderID, remainingChunks)
		log.Printf("📦 Max chunks leader %s can take: %d", leaderID, maxChunks)
		if maxChunks == 0 {
			log.Printf("❌ Leader %s has no space for chunk %d. ServerSpaces: %v", leaderID, i, spaces)
			return nil, fmt.Errorf("no server with sufficient space for chunk %d", i)
		}

		// Limit to remaining chunks
		chunksToAssign := maxChunks
		if int32(chunksToAssign) > req.ChunkCount-i {
			chunksToAssign = int(req.ChunkCount - i)
		}
		log.Printf("📌 Chunks to assign to leader %s: %d", leaderID, chunksToAssign)

		// Assign chunks
		for j := int32(0); j < int32(chunksToAssign); j++ {
			chunkIndex := i + j
			if chunkIndex >= req.ChunkCount {
				break
			}
			chunkSize = req.ChunkSizes[chunkIndex]
			chunkHash := req.ChunkHashes[chunkIndex]

			// Select replicas with sufficient space
			replicas := ms.rs.SelectReplicas(leaderID, 2, servers, chunkSize, spaces)
			log.Printf("📋 Selected replicas for chunk %d: %v", chunkIndex, replicas)
			if len(replicas) < 2 {
				log.Printf("❌ Not enough replicas for chunk %d (leader %s)", chunkIndex, leaderID)
				return nil, fmt.Errorf("not enough replica servers for chunk %d", chunkIndex)
			}

			// Get replica addresses
			replicaAddrs := make([]string, 0, len(replicas))
			for _, replicaID := range replicas {
				addr, exists := addresses[replicaID]
				if !exists {
					log.Printf("❌ No address for replica %s for chunk %d", replicaID, chunkIndex)
					return nil, fmt.Errorf("no address for replica %s for chunk %d", replicaID, chunkIndex)
				}
				replicaAddrs = append(replicaAddrs, addr)
			}
			log.Printf("📡 Replica addresses for chunk %d: %v", chunkIndex, replicaAddrs)

			// Create packet
			packet := NewChunkPacket(fileID, chunkIndex, leaderAddr, replicaAddrs, req)
			assignments.packets = append(assignments.packets, packet)
			assignments.chunkAssignments[chunkIndex] = packet.ToProtoChunkServers()
			assignments.replicationMap[chunkIndex] = packet.ToProtoReplicaServers()

			log.Printf("✅ Assigned chunk %d (Hash: %s) to leader %s with replicas %v", chunkIndex, chunkHash, leaderAddr, replicaAddrs)

			// Update load
			ms.dataManager.UpdateLoad(leaderID, chunkSize)
			for _, replicaID := range replicas {
				ms.dataManager.UpdateLoad(replicaID, chunkSize)
			}
			log.Printf("📈 Updated load for leader %s and replicas %v after chunk %d", leaderID, replicas, chunkIndex)
		}

		i += int32(chunksToAssign)
	}

	log.Printf("🎉 Completed chunk assignment for file %s. Assignments: %v", fileID, assignments.chunkAssignments)
	return assignments, nil
}

type chunkAssignments struct {
	packets          []ChunkPacket
	chunkAssignments map[int32]*pb.ChunkServers
	replicationMap   map[int32][]string
}

func convertReplicationMap(m map[int32][]string) map[int32]*pb.ReplicaServers {
	result := make(map[int32]*pb.ReplicaServers)
	for k, v := range m {
		result[k] = &pb.ReplicaServers{Servers: v}
	}
	return result
}
