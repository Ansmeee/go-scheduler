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
	id   string
	pool *WorkerPool
}

func (w *Worker) Run(ctx context.Context, taskChan chan uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		case taskId := <-taskChan:
			w.execute(ctx, taskId)
		}
	}
}

func (w *Worker) execute(ctx context.Context, taskId uint64) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(ctx, "worker execute task panic:", zap.Any("err", err))
		}
	}()

	task := &dao.SchedulerTask{ID: taskId}
	if err := task.GetCacheData(ctx); err != nil {
		logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", taskId))
		return
	}

	if err := task.Running(ctx, nil); err != nil {
		logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
		return
	}

	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(task.Timeout))
	defer cancel()

	if err := task.Execute(taskCtx); err != nil {
		w.onExecuteFail(ctx, task)
		w.pool.taskQueue.Nack(ctx, task.ID)
		return
	}

	if err := task.Success(ctx, nil); err != nil {
		logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
		w.pool.taskQueue.Nack(ctx, task.ID)
		return
	}

	w.pool.taskQueue.Ack(ctx, task.ID)
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
			if err := w.pool.taskQueue.Enqueue(ctx, task); err != nil {
				logger.Error(ctx, "worker execute task failed", zap.Error(err), zap.Uint64("task_id", task.ID))
			}
		}
	}()
}
