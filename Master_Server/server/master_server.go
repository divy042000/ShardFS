package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "master_server/proto"

	"google.golang.org/grpc"
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
}

// NewMasterServer initializes the Master Server with optional chunk servers
func NewMasterServer(storageDir string) *MasterServer { // Removed chunkServers argument
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
	}

	executor := func(job Job) interface{} {
		switch job.Type {
		case RegisterChunkServerJob:
			req := job.Data.(*pb.RegisterRequest)
			ms.mu.Lock()
			defer ms.mu.Unlock()
			ms.dataManager.mu.Lock()
			for _, addr := range ms.dataManager.chunkServers {
				if addr == req.Address {
					ms.dataManager.mu.Unlock()
					return JobResult{Success: true, Data: &pb.RegisterResponse{Success: true, Message: "Already registered"}}
				}
			}
			ms.dataManager.chunkServers = append(ms.dataManager.chunkServers, req.Address)
			ms.dataManager.mu.Unlock()
			log.Printf("✅ Chunk Server %s registered at %s", req.ServerId, req.Address)
			return JobResult{Success: true, Data: &pb.RegisterResponse{Success: true, Message: "Registered successfully"}}
		case RegisterFileJob:
			req := job.Data.(*pb.RegisterFileRequest)
			ms.mu.Lock()
			defer ms.mu.Unlock()
			fileID, err := ms.dataManager.RegisterFile(req)
			if err != nil {
				return JobResult{Success: false, Error: err}
			}
			assignments, err := ms.assignChunks(req, fileID)
			if err != nil {
				return JobResult{Success: false, Error: err}
			}
			if err := ms.chunkManager.StoreAndSerialize(fileID, req, assignments.packets); err != nil {
				return JobResult{Success: false, Error: err}
			}
			for _, packet := range assignments.packets {
				chunkID := packet.ChunkName
				servers := append([]string{packet.LeaderAddress}, packet.ReplicaAddresses...)
				ms.chunkTable[chunkID] = servers
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
		case GetChunkLocationsJob:
			req := job.Data.(*pb.GetChunkRequest)
			ms.mu.Lock()
			defer ms.mu.Unlock()
			chunkID := req.FileName + "_" + string(req.ChunkIndex)
			servers, exists := ms.chunkTable[chunkID]
			if !exists {
				log.Printf("⚠️ No chunk locations found for %s", chunkID)
				return JobResult{Success: false, Data: &pb.GetChunkResponse{Success: false}}
			}
			activeServers := ms.heartbeatManager.GetActiveChunkServers(servers)
			if len(activeServers) == 0 {
				log.Printf("⚠️ All chunk replicas for %s are unavailable!", chunkID)
				return JobResult{Success: false, Data: &pb.GetChunkResponse{Success: false, Message: "No active servers available"}}
			}
			return JobResult{
				Success: true,
				Data: &pb.GetChunkResponse{
					ChunkId:      chunkID,
					ChunkServers: activeServers,
					Success:      true,
				},
			}
		default:
			return JobResult{Success: false, Error: fmt.Errorf("unknown job type: %d", job.Type)}
		}
	}

	ms.workerPool = NewWorkerPool(4, 100, executor)
	hm.ms = ms
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
func (ms *MasterServer) RegisterChunkServer(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
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
		return &pb.RegisterResponse{Success: false, Message: res.Error.Error()}, res.Error
	}
	return res.Data.(*pb.RegisterResponse), nil
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

// assignChunks remains as a helper function, called by RegisterFileJob
func (ms *MasterServer) assignChunks(req *pb.RegisterFileRequest, fileID string) (*chunkAssignments, error) {
	ms.dataManager.serverLoads.Lock()
	ms.dataManager.serverSpaces.RLock()
	defer ms.dataManager.serverLoads.Unlock()
	defer ms.dataManager.serverSpaces.RUnlock()

	assignments := &chunkAssignments{
		packets:          make([]ChunkPacket, 0, req.ChunkCount),
		chunkAssignments: make(map[int32]*pb.ChunkServers),
		replicationMap:   make(map[int32][]string),
	}

	remainingSize := req.TotalSize
	remainingChunks := req.ChunkCount
	for i := int32(0); i < req.ChunkCount; {
		leader := ms.le.ElectLeader(remainingSize, remainingChunks, ms.dataManager.chunkServers,
			ms.dataManager.serverLoads.m, ms.dataManager.serverSpaces.m)
		if leader == "" {
			return nil, fmt.Errorf("no suitable leader found at chunk %d", i)
		}

		maxChunks := ms.dataManager.MaxChunksForServer(leader)
		if maxChunks == 0 {
			return nil, fmt.Errorf("no server with sufficient space at chunk %d", i)
		}

		chunksToAssign := maxChunks
		if int32(chunksToAssign) > remainingChunks {
			chunksToAssign = int(remainingChunks)
		}

		replicas := ms.rs.SelectReplicas(leader, 2, ms.dataManager.chunkServers)
		if len(replicas) < 2 {
			return nil, fmt.Errorf("not enough replica servers for chunk %d", i)
		}

		for j := int32(0); j < int32(chunksToAssign); j++ {
			chunkIndex := i + j
			packet := NewChunkPacket(fileID, chunkIndex, leader, replicas, req)
			assignments.packets = append(assignments.packets, packet)
			assignments.chunkAssignments[chunkIndex] = packet.ToProtoChunkServers()
			assignments.replicationMap[chunkIndex] = packet.ToProtoReplicaServers()
			ms.dataManager.UpdateLoad(leader, req.ChunkSizes[chunkIndex])
		}

		remainingChunks -= int32(chunksToAssign)
		remainingSize -= req.TotalSize / int64(req.ChunkCount) * int64(chunksToAssign)
		i += int32(chunksToAssign)
	}
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
