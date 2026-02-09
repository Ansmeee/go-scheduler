package service

import (
	"context"
	"fmt"
	"go-scheduler/internal/dao"
	"go-scheduler/internal/service/queue"
	"sync"
)

const workerNum = 32

type WorkerPool struct {
	mu        sync.RWMutex
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	workers   map[string]*Worker
	taskQueue TaskQueue
	taskChan  chan uint64
	stopOnce  sync.Once
}

func NewWorkerPool(ctx context.Context) *WorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	workerPool := &WorkerPool{
		ctx:       ctx,
		cancel:    cancel,
		wg:        sync.WaitGroup{},
		workers:   make(map[string]*Worker),
		taskChan:  make(chan uint64, workerNum),
		taskQueue: &queue.RedisQueue{},
	}

	for i := 0; i < workerNum; i++ {
		workerPool.registerWorker(fmt.Sprintf("worker-%d", i+1))
	}

	return workerPool
}

func (wp *WorkerPool) Start() {
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.dispatchLoop()
	}()
}

func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		wp.cancel()
		wp.wg.Wait()
		close(wp.taskChan)
	})
}

func (wp *WorkerPool) registerWorker(id string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if _, ok := wp.workers[id]; !ok {
		wp.workers[id] = &Worker{id: id, pool: wp}
		go wp.workers[id].Run(wp.ctx, wp.taskChan)
	}
}

func (wp *WorkerPool) dispatchLoop() {
	for {
		if wp.ctx.Err() != nil {
			return
		}

		task, err := wp.taskQueue.BlockDequeue(wp.ctx)
		if err != nil || task == nil {
			continue
		}

		wp.taskChan <- task
	}
}
