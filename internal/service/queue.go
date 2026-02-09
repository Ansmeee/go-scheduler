package service

import (
	"context"
	"go-scheduler/internal/dao"
)

type TaskQueue interface {
	Enqueue(ctx context.Context, task *dao.SchedulerTask) error
	BatchEnqueue(ctx context.Context, taskIds []uint64) error
	BlockDequeue(ctx context.Context) (*dao.SchedulerTask, error)
	Ack(ctx context.Context, task uint64)
	Nack(ctx context.Context, task uint64)
}
