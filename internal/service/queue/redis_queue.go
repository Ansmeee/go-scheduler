package queue

import (
	"context"
	"encoding/json"
	"errors"
	"go-scheduler/internal/client"
	"go-scheduler/internal/dao"
	"go-scheduler/internal/logger"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const TaskScheduleQueue = "go:scheduler:task:schedule:queue"
const TaskProcessQueue = "go:scheduler:task:process:queue"

type RedisQueue struct{}

func (t *RedisQueue) Enqueue(ctx context.Context, task *dao.SchedulerTask) error {
	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return client.RedisMaster.LPush(ctx, TaskScheduleQueue, string(taskJson)).Err()
}

func (t *RedisQueue) BatchEnqueue(ctx context.Context, taskIds []uint64) error {
	pipe := client.RedisMaster.Pipeline()
	for _, id := range taskIds {
		pipe.LPush(ctx, TaskScheduleQueue, id)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error(ctx, "task batch enqueue error", zap.Error(err))
		return err
	}

	return nil
}

func (t *RedisQueue) BlockDequeue(ctx context.Context) (*dao.SchedulerTask, error) {
	res, err := client.RedisMaster.BRPopLPush(ctx, TaskScheduleQueue, TaskProcessQueue, time.Second*5).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}

		logger.Error(ctx, "task dequeue error", zap.Error(err))
		return nil, err
	}

	if len(res) != 2 {
		logger.Error(ctx, "task dequeue error", zap.Error(err))
		return nil, err
	}

	task := &dao.SchedulerTask{}
	if err = json.Unmarshal([]byte(res), task); err != nil {
		logger.Error(ctx, "task dequeue error", zap.Error(err))
		return nil, err
	}

	return task, nil
}

func (t *RedisQueue) Ack(ctx context.Context, taskId uint64) {
	if err := client.RedisMaster.LRem(ctx, TaskProcessQueue, 1, taskId).Err(); err != nil {
		logger.Error(ctx, "task ack error", zap.Error(err), zap.Uint64("task_id", taskId))
	}
}

func (t *RedisQueue) Nack(ctx context.Context, taskId uint64) {
	pipe := client.RedisMaster.Pipeline()
	pipe.LRem(ctx, TaskProcessQueue, 1, taskId)
	pipe.LPush(ctx, TaskScheduleQueue, taskId)

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error(ctx, "task nack error", zap.Error(err), zap.Uint64("task_id", taskId))
	}
}
