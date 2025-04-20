package server

import (
	pb "chunk_server_1/proto"
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
	FileId       string
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
		log.Printf("👷 [Worker %d] Processing job: %v for chunk '%s'", workerID, job.Type, job.ChunkID)
		var result JobResult

		switch job.Type {
		case WriteJob:
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data"
				log.Printf("⚠️ [Worker %d] STORAGE_PATH not set, using default: %s", workerID, storagePath)
			}

			err := storage.WriteChunk(storagePath, job.ChunkID, job.Data)
			if err != nil {
				log.Printf("❌ [Worker %d] Failed to write chunk '%s': %v", workerID, job.ChunkID, err)
				result = JobResult{Success: false, Message: err.Error()}
			} else {
				log.Printf("✅ [Worker %d] Chunk '%s' written successfully", workerID, job.ChunkID)
				result = JobResult{Success: true, Message: "Write complete"}
			}
			job.Response <- result

		case ReadJob:
			storagePath := os.Getenv("STORAGE_PATH")
			if storagePath == "" {
				storagePath = "/data"
				log.Printf("⚠️ [Worker %d] STORAGE_PATH not set, using default: %s", workerID, storagePath)
			}

			data, err := storage.ReadChunk(storagePath, job.ChunkID)
			if err != nil {
				log.Printf("❌ [Worker %d] Failed to read chunk '%s': %v", workerID, job.ChunkID, err)
				result = JobResult{Success: false, Data: nil, Message: err.Error()}
			} else {
				log.Printf("✅ [Worker %d] Chunk '%s' read successfully", workerID, job.ChunkID)
				result = JobResult{Success: true, Data: data, Message: "Read complete"}
			}
			job.Response <- result

		case ReplicationJob:
			log.Printf("🔁 [Worker %d] Starting replication for chunk '%s' at index %d", workerID, job.ChunkID, job.CurrentIndex)

			replicationReq := &pb.ReplicationRequest{
				FileId:    job.FileId,
				ChunkId:   job.ChunkID,
				Data:      job.Data,
				Followers: job.Followers,
			}

			resp, err := wp.replicationManager.StartReplication(replicationReq, job.CurrentIndex)
			if err != nil {
				log.Printf("❌ [Worker %d] Replication error for chunk '%s': %v", workerID, job.ChunkID, err)
				result = JobResult{Success: false, Message: err.Error()}
			} else if !resp.Success {
				log.Printf("⚠️ [Worker %d] Replication failed for chunk '%s': %s", workerID, job.ChunkID, resp.Message)
				result = JobResult{Success: false, Message: resp.Message}
			} else {
				log.Printf("✅ [Worker %d] Replication successful for chunk '%s'", workerID, job.ChunkID)
				result = JobResult{Success: true, Message: "Replication successful"}
			}
			job.Response <- result
		}
	}
}

func (wp *WorkerPool) SubmitJob(job Job) {
	log.Printf("📨 [Pool] Job submitted: %v for chunk '%s'", job.Type, job.ChunkID)
	wp.jobQueue <- job
}

func (wp *WorkerPool) Shutdown() {
	close(wp.jobQueue)
	wp.wg.Wait()
}
