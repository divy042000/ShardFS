// worker/pool.go
package worker

import (
	"context"
	"gfs-client/client"
	"log"
	"sync"
)

// WorkerPool manages a pool of workers to process tasks.
type WorkerPool struct {
	client    *client.Client
	taskQueue chan Task
	wg        sync.WaitGroup
}

// NewWorkerPool creates a new worker pool with the specified number of workers.
func NewWorkerPool(cl *client.Client, workerCount int) *WorkerPool {
	pool := &WorkerPool{
		client:    cl,
		taskQueue: make(chan Task, 100), // Buffered channel for tasks
	}
	pool.startWorkers(workerCount)
	return pool
}

// startWorkers starts the specified number of worker goroutines.
func (p *WorkerPool) startWorkers(workerCount int) {
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			for task := range p.taskQueue {
				if err := task.Execute(context.Background(), p.client); err != nil {
					log.Printf("Worker %d: Task failed: %v", workerID, err)
				}
			}
		}(i)
	}
}

// Submit submits a task to the worker pool.
func (p *WorkerPool) Submit(task Task) {
	p.taskQueue <- task
}

// Close shuts down the worker pool.
func (p *WorkerPool) Close() {
	close(p.taskQueue)
	p.wg.Wait()
}