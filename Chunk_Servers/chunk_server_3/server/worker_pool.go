package server

import (
    "log"
    "sync"
    "chunk_server_1/storage"
)

type JobType int

const (
    WriteJob JobType = iota
    ReadJob
    ReplicationJob
)

type Job struct {
    Type      JobType
    ChunkID   string
    Data      []byte
    Version   int
    Followers []string
    Response  chan JobResult
}

type JobResult struct {
    Success bool
    Message string
    Data    []byte
}

type WorkerPool struct {
    workerCount int
    jobQueue    chan Job
    wg          sync.WaitGroup
}

func NewWorkerPool(workerCount int, queueSize int) *WorkerPool {
    wp := &WorkerPool{
        workerCount: workerCount,
        jobQueue:    make(chan Job, queueSize),
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
        log.Printf("[Worker %d] Processing job: %v for chunk %s", workerID, job.Type, job.ChunkID)
        var result JobResult

        switch job.Type {
        case WriteJob:
            err := storage.WriteChunk("/data", job.ChunkID, job.Data, job.Version)
            result = JobResult{Success: err == nil, Message: "Write operation complete"}
        case ReadJob:
            data, err := storage.ReadChunk("/data", job.ChunkID)
            result = JobResult{Success: err == nil, Data: data, Message: "Read operation complete"}
        case ReplicationJob:
            // Handle replication job
            result = JobResult{Success: true, Message: "Replication complete"}
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
