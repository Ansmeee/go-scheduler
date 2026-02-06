package service

import (
	"context"
	"sync/atomic"
	"time"
)

type TaskStatus int32

const (
	TaskStatusPending TaskStatus = iota + 1
	TaskStatusScheduling
	TaskStatusExecuting
	TaskStatusSuccess
	TaskStatusFailed
	TaskStatusDead
	TaskStatusCancelled
)

type Task struct {
	ID            int
	Name          string
	NextRetryTime time.Time
	RetryCount    int64
	MaxRetryCount int64
	Deleted       bool
	TaskStatus    TaskStatus
	Timeout       time.Duration
	Executor      func(ctx context.Context) error
}

func (task *Task) SetStatus(status TaskStatus) {
	atomic.StoreInt32((*int32)(&task.TaskStatus), int32(status))
}

func (task *Task) GetStatus() int32 {
	return atomic.LoadInt32((*int32)(&task.TaskStatus))
}
