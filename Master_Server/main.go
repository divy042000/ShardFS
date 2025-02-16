package main

import (
	"log"
	"master_server/server"
)

func main() {
	log.Println("🚀 Starting Master Server...")

	// Initialize and start the Master Server (which includes HeartbeatManager)
	master := server.NewMasterServer()
	master.Start()
}

