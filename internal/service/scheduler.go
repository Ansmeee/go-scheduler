package service

import (
	"context"
	"fmt"
	"go-scheduler/internal/dao"
	"go-scheduler/internal/logger"
	"go-scheduler/internal/service/queue"
	"time"

	"go.uber.org/zap"
)

type Scheduler struct {
	ctx       context.Context
	cancel    context.CancelFunc
	taskQueue TaskQueue
}

func NewScheduler(ctx context.Context) *Scheduler {
	ctx, cancel := context.WithCancel(ctx)
	return &Scheduler{ctx: ctx, cancel: cancel, taskQueue: &queue.RedisQueue{}}
}

func (s *Scheduler) Start() {
	go s.startScheduler()
	go s.startMonitor()
}

func (s *Scheduler) startScheduler() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			fmt.Println("scheduler tick is running")
			s.schedOnce()
		}
	}
}

func (s *Scheduler) startMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.autoFallback()
		}
	}
}

func (s *Scheduler) schedOnce() {
	// claim task
	taskIds, err := s.claimTaskIds()
	if err != nil || len(taskIds) == 0 {
		return
	}

	// push to queue
	if err = s.taskQueue.BatchEnqueue(s.ctx, taskIds); err != nil {
		return
	}
}

func (s *Scheduler) claimTaskIds() ([]uint64, error) {
	td := &dao.SchedulerTask{}

	tasks, err := td.ClaimTasks(s.ctx)
	if err != nil {
		logger.Error(s.ctx, "scheduler claim task error", zap.Error(err))
		return nil, err
	}

	return td.BatchSetCacheData(s.ctx, tasks)
}

func (s *Scheduler) autoFallback() {
	td := &dao.SchedulerTask{}
	rows, err := td.AutoFallback(s.ctx, nil)
	if err != nil {
		logger.Error(s.ctx, "scheduler auto fallback error", zap.Error(err))
		return
	}

	if rows > 0 {
		logger.Info(s.ctx, "scheduler auto fallback task", zap.Int64("num", rows))
	}
}
