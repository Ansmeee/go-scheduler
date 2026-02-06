package service

import (
	"context"
	"fmt"
	"go-scheduler/internal/dao"
	"sync/atomic"
	"time"
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
			fmt.Println(err)
		}
	}()

	atomic.AddInt64(&w.Load, 1)
	defer atomic.AddInt64(&w.Load, -1)

	task.Running(ctx, nil)

	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(task.Timeout))
	defer cancel()

	if err := task.Execute(taskCtx); err != nil {
		w.onExecuteFail(ctx, task)
		return
	}

	task.Success(ctx, nil)
}

func (w *Worker) onExecuteFail(ctx context.Context, task *dao.SchedulerTask) {
	retry := atomic.AddInt32(&task.RetryCount, 1)
	if retry > task.MaxRetryCount {
		task.Dead(ctx, nil)
		return
	}

	task.Failed(ctx, nil)
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
