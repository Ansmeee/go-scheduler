package service

import (
	"context"
	"go-scheduler/internal/dao"
	"go-scheduler/internal/logger"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Worker struct {
	ID       string
	Status   WorkerStatus
	TaskChan chan *dao.SchedulerTask
	Load     int64
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-w.TaskChan:
			if !ok {
				return
			}

			w.execute(ctx, task)
		}
	}
}

func (w *Worker) execute(ctx context.Context, task *dao.SchedulerTask) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(ctx, "worker execute panic:", zap.Any("err", err))
		}
	}()

	atomic.AddInt64(&w.Load, 1)
	defer atomic.AddInt64(&w.Load, -1)

	if err := task.Running(ctx, nil); err != nil {
		logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
		return
	}

	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(task.Timeout))
	defer cancel()

	if err := task.Execute(taskCtx); err != nil {
		w.onExecuteFail(ctx, task)
		return
	}

	if err := task.Success(ctx, nil); err != nil {
		logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
		return
	}
}

func (w *Worker) onExecuteFail(ctx context.Context, task *dao.SchedulerTask) {
	retry := atomic.AddInt32(&task.RetryCount, 1)
	if retry > task.MaxRetryCount {
		if err := task.Dead(ctx, nil); err != nil {
			logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
		}
		return
	}

	if err := task.Failed(ctx, nil); err != nil {
		logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
		return
	}

	delay := time.Duration(retry) * time.Second

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			workerPool.AddTask(task)
		}
	}()
}
