package service

import (
	"context"
	"encoding/json"
	"fmt"
	"go-scheduler/internal/client"
	"go-scheduler/internal/dao"
	"go-scheduler/internal/logger"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewScheduler(ctx context.Context) *Scheduler {
	ctx, cancel := context.WithCancel(ctx)
	return &Scheduler{ctx: ctx, cancel: cancel}
}

func (s *Scheduler) Start() {
	go s.startScheduler()
	go s.startMonitor()
	logger.Info(s.ctx, "Scheduler started")
}

func (s *Scheduler) startScheduler() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			fmt.Println("scheduler has been stopped")
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
			fmt.Println("scheduler has been stopped")
			return
		case <-ticker.C:
			fmt.Println("scheduler monitor running")
			s.autoFallback()
		}
	}
}

func (s *Scheduler) schedOnce() {
	// claim task
	tasks, err := s.claimTasks()
	if err != nil || len(tasks) == 0 {
		return
	}

	// push to queue
	if err = s.push2Queue(tasks); err != nil {
		return
	}
}

func (s *Scheduler) claimTasks() ([]*dao.SchedulerTask, error) {
	var err error
	var tasks []*dao.SchedulerTask

	td := &dao.SchedulerTask{}
	tasks, err = td.ClaimTasks(s.ctx)
	if err != nil {
		logger.Error(s.ctx, "scheduler get pending task err:", zap.Error(err))
		return nil, err
	}

	return tasks, nil
}

func (s *Scheduler) push2Queue(tasks []*dao.SchedulerTask) error {
	if len(tasks) == 0 {
		return nil
	}

	taskIds := make([]uint64, 0, len(tasks)/2)
	pipe := client.RedisMaster.Pipeline()
	for _, task := range tasks {
		taskIds = append(taskIds, task.ID)
		taskJson, _ := json.Marshal(task)
		pipe.RPush(s.ctx, dao.TaskScheduleQueue, taskJson)
	}

	if _, err := pipe.Exec(s.ctx); err != nil {
		logger.Error(s.ctx, "scheduler push task err:", zap.Error(err))
		go s.fallbackTask(taskIds)
	}

	return nil
}

func (s *Scheduler) autoFallback() {
	td := &dao.SchedulerTask{}
	rows, err := td.AutoFallback(s.ctx, nil)
	if err != nil {
		logger.Error(s.ctx, "scheduler auto fallback task get err:", zap.Error(err))
		return
	}

	if rows > 0 {
		logger.Info(s.ctx, "scheduler auto fallback task", zap.Int64("num", rows))
	}
}

func (s *Scheduler) fallbackTask(taskIds []uint64) {
	if len(taskIds) == 0 {
		return
	}

	td := &dao.SchedulerTask{}
	err := client.MySQLMaster.Transaction(func(tx *gorm.DB) error {
		rows, err := td.Fallback(s.ctx, taskIds, tx)
		if err != nil {
			return err
		}

		if rows == 0 {
			return fmt.Errorf("no task fallbacked")
		}

		return nil
	})

	if err != nil {
		logger.Error(s.ctx, "fallback task get err:", zap.Error(err), zap.Any("ids", taskIds))
	}
}
