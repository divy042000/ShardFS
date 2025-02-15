package main

import (
	"fmt"
	"log"
	"net"

	pb "chunk_server_1/proto"
	"chunk_server_1/server"
	"google.golang.org/grpc"
)

func main() {
	port := ":50051"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	chunkServer := server.NewChunkServer("/data", 5) // 5 concurrent workers

	pb.RegisterChunkServiceServer(grpcServer, chunkServer)

	fmt.Printf("Chunk Server is running on port %s...\n", port)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
}
