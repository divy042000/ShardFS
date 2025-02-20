package main

import (
	"log"
	"os"

	"chunk_server_1/server"
)

func main() {
	// Define server configurations
	serverID := "chunk_server_1"
	storagePath := "/data/chunks" // ✅ Ensure this is correctly set
	masterAddress := "master-server:50052"
	workerCount := 5 // Number of concurrent worker threads

	// Ensure storage directory exists
	if err := os.MkdirAll(storagePath, os.ModePerm); err != nil {
		log.Fatalf("❌ Failed to create storage directory: %v", err)
	}

	// Initialize and start the Chunk Server
	chunkServer := server.NewChunkServer(serverID, storagePath, masterAddress, workerCount)
	chunkServer.Start()

	// Keep the server running
	select {}
}
