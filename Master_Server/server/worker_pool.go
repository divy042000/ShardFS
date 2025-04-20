package server

import(
	"sync"
)

// JobType defines the type of job to execute
type JobType int 

const (
	RegisterChunkServerJob JobType = iota
	RegisterFileJob
	AssignChunksJob
	ReportChunkJob
	GetFileMetadataJob
	HeartbeatJob
	DeleteFileJob
	DeleteReplicaJob
)

// Job represents a task to be executed by a worker pool

	type Job struct {
		Type JobType	
		Data interface{} // Data can be any type, depending on the job type
		Response chan interface{} // Channel to send the response back to the caller		
	}

	// JobResult holds the result of a job
	type JobResult struct {
	Success bool
	Data interface{} // Result data
	Error error
	}


type WorkerPool struct{
	workers int 
	jobQueue chan Job
	wg       sync.WaitGroup
	shutdown chan struct{} 
	executor func(Job) interface{} 
}

// NewWorkerPool creates a new worker pool with the specified number of workers and job queue size
func NewWorkerPool(workers, queueSize int,executor func(Job) interface{}) *WorkerPool {
	wp :=  &WorkerPool{
		workers:  workers,
		jobQueue: make(chan Job, queueSize),
		shutdown: make(chan struct{}),
		executor: executor,
	}
	wp.start()
	return wp
}

// start initializes the worker pool and starts the workers
func (wp *WorkerPool) start(){
	for i := 0; i < wp.workers; i++{
		wp.wg.Add(1)
		go func(){
			defer wp.wg.Done()
			for {
				select {
				case job := <- wp.jobQueue: // Receive a job from the queue
					result := wp.executor(job) // Execute the job using the provided executor function
					job.Response <- result // Send the result back to the caller 
				case <- wp.shutdown :
					return 
				}
			}
		}()
	}
}

func(wp *WorkerPool) SubmitJob(job Job){
	wp.jobQueue <- job
}

// Shutdown gracefully stops the worker pool and waits for all workers to finish

func (wp *WorkerPool) Shutdown(){
	close(wp.shutdown)
	wp.wg.Wait()
}

