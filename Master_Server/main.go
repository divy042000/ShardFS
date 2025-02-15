package main

import (
	"log"
	"net"

	pb "master_server/proto"
	"master_server/server"
	"google.golang.org/grpc"
)

func main() {
	listener, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	master := server.NewMasterServer()
	heartbeatManager := server.NewHeartbeatManager()

	pb.RegisterMasterServiceServer(grpcServer, master)
	pb.RegisterHeartbeatServiceServer(grpcServer, heartbeatManager)

	log.Println("Master Server is running on port 50052...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
}
