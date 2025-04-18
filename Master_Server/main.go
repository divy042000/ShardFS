package main

import (
    "log"
    "master_server/server"
)

func main() {
    ms, err := server.NewMasterServer()
    if err != nil {
        log.Fatalf("Failed to create MasterServer: %v", err)
    }
    go ms.Start()

    log.Println("Master Server started")
    select {} // Keep main running
}