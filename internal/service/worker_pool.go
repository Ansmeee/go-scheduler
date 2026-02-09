package service

import (
	"context"
	"encoding/json"
	"fmt"
	"go-scheduler/internal/client"
	"go-scheduler/internal/dao"
	"go-scheduler/internal/logger"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const workerNum = 8
const taskNum = workerNum * workerNum

var workerPool *WorkerPool

type WorkerPool struct {
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	workers    map[string]*Worker
	tasksQueue chan *dao.SchedulerTask
	tasks      map[uint64]*dao.SchedulerTask
	stopOnce   sync.Once
}

func NewWorkerPool(ctx context.Context) *WorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	workerPool = &WorkerPool{
		ctx:        ctx,
		cancel:     cancel,
		workers:    make(map[string]*Worker),
		tasks:      make(map[uint64]*dao.SchedulerTask),
		tasksQueue: make(chan *dao.SchedulerTask, taskNum),
	}

	for i := 0; i < workerNum; i++ {
		workerPool.registerWorker(fmt.Sprintf("worker-%d", i+1))
	}

	return workerPool
}

func (wp *WorkerPool) Start() {
	go wp.dispatchLoop()
}

func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		wp.cancel()
		close(wp.tasksQueue)

		wp.mu.Lock()
		defer wp.mu.Unlock()

		for _, w := range wp.workers {
			close(w.TaskChan)
		}
	})
}

func (wp *WorkerPool) AddTask(task *dao.SchedulerTask) {
	wp.mu.Lock()
	wp.tasks[task.ID] = task
	wp.mu.Unlock()

	select {
	case wp.tasksQueue <- task:
	default:
		logger.Error(wp.ctx, "ask queue is full, task is dropped:", zap.Uint64("id", task.ID))
	}
}

func (wp *WorkerPool) registerWorker(id string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if _, ok := wp.workers[id]; !ok {
		wp.workers[id] = &Worker{
			ID:       id,
			Status:   WorkerStatusActive,
			TaskChan: make(chan *dao.SchedulerTask, 50),
		}

		go wp.workers[id].Run(wp.ctx)
	}
}

func (wp *WorkerPool) selectWorker() *Worker {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	var bestWorker *Worker
	var minLoad int64 = math.MaxInt64

	for _, worker := range wp.workers {
		if worker.Status != WorkerStatusActive {
			continue
		}

		load := atomic.LoadInt64(&worker.Load)
		if load < minLoad {
			bestWorker = worker
			minLoad = load
		}
	}

	return bestWorker
}

func (wp *WorkerPool) dispatchLoop() {
	for {
		select {
		case <-wp.ctx.Done():
			return
		default:
			res, err := client.RedisSlaver.LPop(wp.ctx, dao.TaskScheduleQueue).Result()
			if err != nil {
				logger.Error(wp.ctx, "task queue is not available", zap.Error(err))
				time.Sleep(time.Millisecond * 100)
				continue
			}

			task := &dao.SchedulerTask{}
			if err = json.Unmarshal([]byte(res), task); err != nil {
				logger.Error(wp.ctx, "task is invalid", zap.Error(err))
				continue
			}

			worker := wp.selectWorker()
			if worker == nil {
				time.Sleep(5 * time.Millisecond)
				logger.Error(wp.ctx, "no available worker, retried:", zap.Uint64("id", task.ID))
				wp.AddTask(task)
				continue
			}

			select {
			case worker.TaskChan <- task:
			default:
				time.Sleep(5 * time.Millisecond)
				logger.Error(wp.ctx, "task queue is full, retried", zap.Uint64("id", task.ID))
			}
		}
	}
}
