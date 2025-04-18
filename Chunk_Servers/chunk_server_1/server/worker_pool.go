package server

import (
	"chunk_server_1/storage"
	"log"
	"os"
	"sync"
)

type JobType int

const (
	WriteJob JobType = iota
	ReadJob
	ReplicationJob
)

type Job struct {
	Type         JobType
	ChunkID      string
	Data         []byte
	CurrentIndex int
	Followers    []string
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

func NewWorkerPool(workerCount int, queueSize int) *WorkerPool {
	wp := &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan Job, queueSize),
	}
	wp.startWorkers()
	return wp
}

func (wp *WorkerPool) ReplicateChunk(chunkID string, data []byte, followers []string, version int) error {
	return wp.replicationManager.ReplicateChunk(chunkID, data, followers, version)
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
		log.Printf("[Worker %d] Processing job: %v for chunk %s", workerID, job.Type, job.ChunkID)
		var result JobResult

		switch job.Type {
		case WriteJob:
			// Read STORAGE_PATH from environment, fallback to "/data" if not set
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data" // Default fallback
				log.Printf("⚠️ STORAGE_PATH not set, using default: %s", storagePath)
			} else {
				log.Printf("📂 Using STORAGE_PATH: %s", storagePath)
			}

			// Attempt to write the chunk
			err := storage.WriteChunk(storagePath, job.ChunkID, job.Data)
			if err != nil {
				log.Printf("❌ Failed to write chunk %s to %s: %v", job.ChunkID, storagePath, err)
				result = JobResult{Success: false, Message: err.Error()}
			} else {
				log.Printf("✅ Chunk %s written successfully to %s", job.ChunkID, storagePath)
				result = JobResult{Success: true, Message: "Write operation complete"}
			}
			job.Response <- result
		case ReadJob:
			// Read STORAGE_PATH from environment, fallback to "/data" if not set
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data" // Default fallback
				log.Printf("⚠️ STORAGE_PATH not set, using default: %s", storagePath)
			} else {
				log.Printf("📂 Using STORAGE_PATH: %s", storagePath)
			}

			// Attempt to read the chunk
			data, err := storage.ReadChunk(storagePath, job.ChunkID)
			if err != nil {
				log.Printf("❌ Failed to read chunk %s from %s: %v", job.ChunkID, storagePath, err)
				result = JobResult{Success: false, Data: nil, Message: err.Error()}
			} else {
				log.Printf("✅ Chunk %s read successfully from %s", job.ChunkID, storagePath)
				result = JobResult{Success: true, Data: data, Message: "Read operation complete"}
			}
			job.Response <- result
		case ReplicationJob:
			err := wp.replicationManager.ReplicateChunk(job.ChunkID, job.Data, job.Followers, job.CurrentIndex)
			if err != nil {
				log.Printf("❌ Replication failed for chunk %s to follower at index %d: %v", job.ChunkID, job.CurrentIndex, err)
				result = JobResult{Success: false, Message: err.Error()}
			} else {
				log.Printf("✅ Replicated chunk %s to follower at index %d", job.ChunkID, job.CurrentIndex)
				result = JobResult{Success: true, Message: "Replication complete"}
			}
		}
		job.Response <- result
	}
}

func (wp *WorkerPool) SubmitJob(job Job) {
	wp.jobQueue <- job
}

func (wp *WorkerPool) Shutdown() {
	close(wp.jobQueue)
	wp.wg.Wait()
}
