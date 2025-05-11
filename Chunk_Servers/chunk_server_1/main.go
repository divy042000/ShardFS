package main

import (
    "log"
    "os"
    "chunk_server_1/server"
)

func main() {
    serverID := os.Getenv("SERVER_ID")
    if serverID == "" {
        serverID = "chunk_server_1"
    }

    storagePath := os.Getenv("STORAGE_PATH")
    if storagePath == "" {
        storagePath = "/data/chunks"
    }

    selfAddress := os.Getenv("CHUNK_SERVER_ADDRESS")
	if selfAddress == "" {
		log.Fatal("❌ CHUNK_SERVER_ADDRESS not set")
	}

    masterAddress := os.Getenv("MASTER_ADDRESS")
    if masterAddress == "" {
        masterAddress = "master-server:50052"
    }

    workerCount := 5

    if err := os.MkdirAll(storagePath, os.ModePerm); err != nil {
        log.Fatalf("❌ Failed to create storage directory: %v", err)
    }

    chunkServer := server.NewChunkServer(serverID, storagePath, masterAddress, selfAddress ,workerCount)
    chunkServer.Start()

    select {}
}
