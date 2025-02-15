package server

import(
"log"
"sync"
)

type JobType int 

const (
  WriteJob JobType = iota
  ReadJob
  ReplicationJob
)

// Job represents a task for the worker pool
type Job struct {
   Type   JobType
   ChunkID string
   Data   []byte
   Version int
   Response chan JobResult
}

// JobResult contains the result of a job execution
type JobResult struct {
	Success bool
	Message string
	Data    []byte
}

// WorkerPool manages concurrent chunk operations
type WorkerPool struct {
	workerCount int
	jobQueue    chan Job
	wg          sync.WaitGroup
}

// NewWorkerPool initializes a worker pool with `workerCount` workers
func NewWorkerPool(workerCount int, queueSize int) *WorkerPool {
	wp := &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan Job, queueSize),
	}
	wp.startWorkers()
	return wp
}

// startWorkers launches the worker goroutines
func (wp *WorkerPool) startWorkers() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker processes chunk operations based on job type
func (wp *WorkerPool) worker(workerID int) {
	defer wp.wg.Done()
	for job := range wp.jobQueue {
		log.Printf("[Worker %d] Processing job: %v for chunk %s", workerID, job.Type, job.ChunkID)

		var result JobResult
		switch job.Type {
		case WriteJob:
			err := WriteChunk("/data", job.ChunkID, job.Data, job.Version)
			result = JobResult{Success: err == nil, Message: "Write operation complete"}
		case ReadJob:
			data, err := ReadChunk("/data", job.ChunkID)
			result = JobResult{Success: err == nil, Data: data, Message: "Read operation complete"}
		case ReplicationJob:
			err := ReplicateChunk(job.ChunkID, job.Data, job.Version)
			result = JobResult{Success: err == nil, Message: "Replication complete"}
		}

		job.Response <- result
	}
}



// SubmitJob adds a job to the queue
func (wp *WorkerPool) SubmitJob(job Job) {
	wp.jobQueue <- job
}

// Shutdown waits for all workers to finish
func (wp *WorkerPool) Shutdown() {
	close(wp.jobQueue)
	wp.wg.Wait()
}

