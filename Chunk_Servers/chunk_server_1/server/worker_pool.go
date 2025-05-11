package server

import (
	pb "chunk_server_1/proto"
	"chunk_server_1/storage"
	"log"
	"fmt"
	"os"
	"sync"
)

type JobType int

const (
	WriteJob JobType = iota
	ReadJob
	ReplicationJob
	DeleteJob
)

type Job struct {
	Type         JobType
	ChunkHash   string
	ChunkIndex   int32
	FileId string
	Data         []byte
	Followers    []string
	CurrentIndex int
	Response     chan JobResult
}

type JobResult struct {
	Success bool
	Message string
	Data    []byte
}

type WorkerPool struct {
	workerCount        int
	jobQueue           chan Job
	wg                 sync.WaitGroup
	replicationManager *ReplicationManager
}

func NewWorkerPool(workerCount int, queueSize int, rm *ReplicationManager) *WorkerPool {
	wp := &WorkerPool{
		workerCount:        workerCount,
		jobQueue:           make(chan Job, queueSize),
		replicationManager: rm,
	}
	wp.startWorkers()
	return wp
}

func (wp *WorkerPool) startWorkers() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

func (wp *WorkerPool) worker(workerID int) {
	defer wp.wg.Done()

	for job := range wp.jobQueue {
		log.Printf("👷 [Worker %d] Processing job: %v for chunk '%s'", workerID, job.Type)
		var result JobResult

		switch job.Type {
		case WriteJob:
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data"
				log.Printf("⚠️ [Worker %d] STORAGE_PATH not set, using default: %s", workerID, storagePath)
			}
		
			// Use ChunkHash and ChunkIndex instead of ChunkID
			err := storage.WriteChunk(storagePath, job.ChunkHash, job.ChunkIndex, job.Data)
			if err != nil {
				log.Printf("❌ [Worker %d] Failed to write chunk (Hash: '%s', Index: %d): %v", workerID, job.ChunkHash, job.ChunkIndex, err)
				result = JobResult{Success: false, Message: err.Error()}
			} else {
				log.Printf("✅ [Worker %d] Chunk (Hash: '%s', Index: %d) written successfully", workerID, job.ChunkHash, job.ChunkIndex)
				result = JobResult{Success: true, Message: "Write complete"}
			}
			job.Response <- result
		
		case DeleteJob:
			log.Printf("🗑️ [Worker %d] Received delete job | ChunkHash: %s | ChunkIndex: %d", workerID, job.ChunkHash, job.ChunkIndex)
		
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data"
				log.Printf("⚠️ [Worker %d] STORAGE_PATH not set. Using default: %s", workerID, storagePath)
			} else {
				log.Printf("📦 [Worker %d] Using storage path: %s", workerID, storagePath)
			}
		
			err := storage.DeleteChunkFromDisk(job.ChunkHash, job.ChunkIndex, storagePath)
			if err != nil {
				log.Printf("❌ [Worker %d] Failed to delete chunk '%s_%d': %v", workerID, job.ChunkHash, job.ChunkIndex, err)
				job.Response <- JobResult{
					Success: false,
					Message: fmt.Sprintf("Delete failed: %v", err),
				}
			} else {
				log.Printf("✅ [Worker %d] Successfully deleted chunk '%s_%d'", workerID, job.ChunkHash, job.ChunkIndex)
				job.Response <- JobResult{
					Success: true,
					Message: "Delete complete",
				}
			}
		

		case ReadJob:
			log.Printf("🧑‍🏭 [Worker %d] Received read job | ChunkHash: %s | ChunkIndex: %d", workerID, job.ChunkHash, job.ChunkIndex)
		
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data"
				log.Printf("⚠️ [Worker %d] STORAGE_PATH not set. Using default: %s", workerID, storagePath)
			} else {
				log.Printf("📦 [Worker %d] Using storage path: %s", workerID, storagePath)
			}
		
			log.Printf("📄 [Worker %d] Attempting to read chunk from sanitized name: %s_%d", workerID, job.ChunkHash, job.ChunkIndex)
		
			// Perform the actual chunk read
			data, err := storage.ReadChunk(storagePath, job.ChunkHash, job.ChunkIndex)
			if err != nil {
				log.Printf("❌ [Worker %d] Failed to read chunk '%s_%d': %v", workerID, job.ChunkHash, job.ChunkIndex, err)
				result = JobResult{
					Success: false,
					Data:    nil,
					Message: fmt.Sprintf("Read failed: %v", err),
				}
			} else {
				log.Printf("✅ [Worker %d] Successfully read chunk '%s_%d' (%d bytes)", workerID, job.ChunkHash, job.ChunkIndex, len(data))
				result = JobResult{
					Success: true,
					Data:    data,
					Message: "Read complete",
				}
			}
		
			log.Printf("📬 [Worker %d] Sending read result back for chunk '%s_%d'", workerID, job.ChunkHash, job.ChunkIndex)
			job.Response <- result
				

		case ReplicationJob:
			log.Printf("🔁 [Worker %d] Starting replication for chunk (hash: %s, index: %d) at replication index %d", 
				workerID, job.ChunkHash, job.ChunkIndex, job.CurrentIndex)
		
			replicationReq := &pb.ReplicationRequest{
				FileId:     job.FileId,
				ChunkHash:  job.ChunkHash,
				ChunkIndex: job.ChunkIndex,
				Data:       job.Data,
				Followers:  job.Followers,
			}
		
			resp, err := wp.replicationManager.StartReplication(replicationReq, job.CurrentIndex)
			if err != nil {
				log.Printf("❌ [Worker %d] Replication error for chunk (hash: %s, index: %d): %v", 
					workerID, job.ChunkHash, job.ChunkIndex, err)
				result = JobResult{Success: false, Message: err.Error()}
			} else if !resp.Success {
				log.Printf("⚠️ [Worker %d] Replication failed for chunk (hash: %s, index: %d): %s", 
					workerID, job.ChunkHash, job.ChunkIndex, resp.Message)
				result = JobResult{Success: false, Message: resp.Message}
			} else {
				log.Printf("✅ [Worker %d] Replication successful for chunk (hash: %s, index: %d)", 
					workerID, job.ChunkHash, job.ChunkIndex)
				result = JobResult{Success: true, Message: "Replication successful"}
			}
		
			job.Response <- result
		}
	}
}

func (wp *WorkerPool) SubmitJob(job Job) {
	log.Printf("📨 [Pool] Job submitted: %v for chunk '%s'", job.Type)
	wp.jobQueue <- job
}

func (wp *WorkerPool) Shutdown() {
	close(wp.jobQueue)
	wp.wg.Wait()
}
