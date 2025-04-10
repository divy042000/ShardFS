package main

import (
	"log"
	"master_server/server" 
)


func main() {
	ms := server.NewMasterServer("./storage") // Only storageDir needed
	go ms.Start()

	log.Println("Master Server started")
	select {} // Keep main running
}