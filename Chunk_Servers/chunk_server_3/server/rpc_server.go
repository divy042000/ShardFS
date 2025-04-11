package server

import (
    "context"
    "log"
    pb "chunk_server_1/proto"
    "chunk_server_1/storage"
)

type RPCServer struct {
    ChunkServer *ChunkServer
    pb.UnimplementedChunkServiceServer
     
}

func NewRPCServer(cs *ChunkServer) *RPCServer {
    return &RPCServer{ChunkServer: cs}
}

func (s *RPCServer) WriteChunk(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
    responseChan := make(chan JobResult, 1)
    job := Job{
        Type:     WriteJob,
        ChunkID:  req.ChunkId,
        Data:     req.Data,
        Version:  int(req.Version),
        Response: responseChan,
    }
    s.ChunkServer.workerPool.SubmitJob(job)
    result := <-responseChan
    if !result.Success {
        log.Printf("WriteChunk error: %s", result.Message)
        return &pb.WriteResponse{Success: false, Message: result.Message}, nil
    }
    log.Printf("Chunk %s written successfully", req.ChunkId)
    return &pb.WriteResponse{Success: true, Message: "Chunk written"}, nil
}

func (s *RPCServer) ReadChunk(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
    responseChan := make(chan JobResult, 1)
    job := Job{
        Type:     ReadJob,
        ChunkID:  req.ChunkId,
        Response: responseChan,
    }
    s.ChunkServer.workerPool.SubmitJob(job)
    result := <-responseChan
    if !result.Success {
        log.Printf("ReadChunk error: %s", result.Message)
        return &pb.ReadResponse{Success: false}, nil
    }
    log.Printf("Chunk %s read successfully", req.ChunkId)
    return &pb.ReadResponse{Success: true, Data: result.Data}, nil
}

func (s *RPCServer) SendChunk(ctx context.Context, req *pb.ReplicationRequest) (*pb.ReplicationResponse, error) {
    err := storage.WriteChunk("/data", req.ChunkId, req.Data, int(req.Version))
    if err != nil {
        log.Printf("Follower failed to store chunk %s: %v", req.ChunkId, err)
        return &pb.ReplicationResponse{Success: false, Message: "Replication failed"}, nil
    }
    if len(req.Followers) > 0 {
        log.Printf("Forwarding chunk %s to next follower %s", req.ChunkId, req.Followers[0])
        manager := NewReplicationManager(s.ChunkServer)
        manager.ReplicateChunk(req.ChunkId, req.Data, int(req.Version), req.Followers)
    }
    return &pb.ReplicationResponse{Success: true, Message: "Chunk replicated"}, nil
}

func (s *RPCServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Log heartbeat received
	log.Printf("💓 Heartbeat received from %s | Free Space: %dMB | Chunks: %d", req.ServerId, req.FreeSpace, len(req.ChunkIds))

	// ✅ Update Master Server's metadata with received chunk IDs
	s.ChunkServer.UpdateChunkMetadata(req.ServerId, req.ChunkIds)

	return &pb.HeartbeatResponse{
		Success: true,
		Message: "✅ Heartbeat received successfully",
	},nil
}
