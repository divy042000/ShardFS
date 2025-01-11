		package main

		import (
			"fmt"
			"io"
			"log"

			pb "path/to/chunking/protobuf/generated"
			"google.golang.org/grpc"
			"google.golang.org/grpc/reflection"
			"net"
		)

		const chunkSize = 64 * 1024 * 1024 // 64 MB	

		type server struct {
			pb.UnimplementedChunkServiceServer
		}

		// UploadChunk handles the streamed file chunks from the Node.js client
		func (s *server) UploadChunk(stream pb.ChunkService_UploadChunkServer) error {
			chunkIndex := 0
			totalBytes := int64(0)

			for {
				// Receive the next chunk from the client
				req, err := stream.Recv()
				if err == io.EOF {
					// End of stream, send response
					response := &pb.ChunkResponse{
						Message: fmt.Sprintf("All chunks processed successfully. Total bytes: %d", totalBytes),
						Success: true,
					}
					return stream.SendAndClose(response)
				}
				if err != nil {
					return fmt.Errorf("error receiving chunk: %w", err)
				}

				// Process the chunk
				log.Printf("Processing chunk %d (offset: %d, size: %d bytes)\n",
					req.ChunkIndex, req.Offset, len(req.ChunkData))

				// Simulate chunk storage or processing
				totalBytes += int64(len(req.ChunkData))
				chunkIndex++
			}
		}

		func main() {
			listener, err := net.Listen("tcp", ":50051")
			if err != nil {
				log.Fatalf("Failed to listen: %v", err)
			}

			grpcServer := grpc.NewServer()
			pb.RegisterChunkServiceServer(grpcServer, &server{})
			reflection.Register(grpcServer)

			log.Println("Go Chunking Service running on port 50051")
			if err := grpcServer.Serve(listener); err != nil {
				log.Fatalf("Failed to serve: %v", err)
			}
		}
