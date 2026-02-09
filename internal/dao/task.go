package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"go-scheduler/internal/client"
	"go-scheduler/internal/logger"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

const SchedulerTaskCacheTTL = time.Hour
const SchedulerTaskInfo = "go:scheduler:task:info"

const (
	SchedulerTaskStatusStopped = iota
	SchedulerTaskStatusPending
	SchedulerTaskStatusScheduling
	SchedulerTaskStatusRunning
	SchedulerTaskStatusSuccess
	SchedulerTaskStatusFailed
	SchedulerTaskStatusRetrying
	SchedulerTaskStatusDead
	SchedulerTaskStatusTimeout
	SchedulerTaskStatusCancelled
)

type SchedulerTask struct {
	ID            uint64 `gorm:"column:id;primary_key" json:"id"`
	Name          string `gorm:"column:name" json:"name"`
	Status        int8   `gorm:"column:status" json:"status"`
	Priority      int8   `gorm:"column:priority" json:"priority"`
	NextRuntime   int64  `gorm:"column:next_run_time" json:"next_runtime"`
	LastSchedTime int64  `gorm:"column:last_sched_time" json:"last_sched_time"`
	LastRunTime   int64  `gorm:"column:last_run_time" json:"last_run_time"`
	RetryCount    int32  `gorm:"column:retry_count" json:"retry_count"`
	MaxRetryCount int32  `gorm:"column:max_retry_count" json:"max_retry_count"`
	Timeout       int64  `gorm:"column:timeout" json:"timeout"`
	Payload       string `gorm:"column:payload" json:"payload"`
}

func (*SchedulerTask) TableName() string {
	return "scheduler_task"
}

type SchedulerTaskListParams struct {
	Wheres map[string]interface{}
	Order  string
	Page   int
	Limit  int
}

func (s *SchedulerTask) Execute(ctx context.Context) error {
	return nil
}

func (s *SchedulerTask) GetCacheData(ctx context.Context) error {
	if s.ID == 0 {
		return fmt.Errorf("scheduler task not exist")
	}

	taskData, err := client.RedisSlaver.Get(ctx, fmt.Sprintf("%s:%d", SchedulerTaskInfo, s.ID)).Result()
	if err == nil && taskData != "" {
		if err = json.Unmarshal([]byte(taskData), &s); err == nil {
			return nil
		}

		client.RedisSlaver.Del(ctx, fmt.Sprintf("%s:%d", SchedulerTaskInfo, s.ID))
	}

	if err = client.MySQLSlaver.Model(&SchedulerTask{}).Where("id = ?", s.ID).Find(s).Error; err != nil {
		return err
	}
	
	return nil
}

func (s *SchedulerTask) SetCacheData(ctx context.Context) error {
	taskJson, err := json.Marshal(s)
	if err != nil {
		return err
	}

	return client.RedisSlaver.Set(ctx, fmt.Sprintf("%s:%d", SchedulerTaskInfo, s.ID), string(taskJson), SchedulerTaskCacheTTL).Err()
}

func (s *SchedulerTask) BatchSetCacheData(ctx context.Context, tasks []*SchedulerTask) ([]uint64, error) {
	taskIds := make([]uint64, 0, len(tasks))
	pipe := client.RedisMaster.Pipeline()
	for _, task := range tasks {
		taskJson, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}

		taskIds = append(taskIds, task.ID)
		pipe.Set(ctx, fmt.Sprintf("%s:%d", SchedulerTaskInfo, task.ID), string(taskJson), SchedulerTaskCacheTTL)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error(ctx, "scheduler claim task error", zap.Error(err))
		return nil, err
	}

	return taskIds, nil
}

func (s *SchedulerTask) List(ctx context.Context, params SchedulerTaskListParams) ([]*SchedulerTask, error) {
	tasks := make([]*SchedulerTask, 0)

	query := client.MySQLSlaver.Model(&SchedulerTask{}).Debug()
	if params.Wheres != nil {
		for k, v := range params.Wheres {
			query = query.Where(k, v)
		}
	}

	if params.Order != "" {
		query = query.Order(params.Order)
	}

	limit := 100
	if params.Limit != 0 {
		limit = params.Limit
	}

	query = query.Limit(limit)
	if err := query.Find(&tasks).Error; err != nil {
		return nil, err
	}

	return tasks, nil
}

func (s *SchedulerTask) ClaimTasks(ctx context.Context) ([]*SchedulerTask, error) {
	tasks := make([]*SchedulerTask, 0)

	err := client.MySQLMaster.Transaction(func(tx *gorm.DB) error {
		selectQuery := "SELECT * FROM scheduler_task WHERE status = 1 AND next_run_time <= NOW() ORDER BY priority DESC LIMIT ? FOR UPDATE SKIP LOCKED"
		if err := tx.Raw(selectQuery, 50).Scan(&tasks).Error; err != nil {
			return err
		}

		if len(tasks) == 0 {
			return nil
		}

		taskIds := make([]uint64, 0, len(tasks))
		for _, task := range tasks {
			taskIds = append(taskIds, task.ID)
		}

		updateData := map[string]interface{}{
			"status":          SchedulerTaskStatusRunning,
			"last_sched_time": time.Now().Unix(),
		}
		return tx.Model(&SchedulerTask{}).Where("id in (?)", taskIds).Updates(updateData).Error
	})

	if err != nil {
		return tasks, err
	}

	return tasks, nil
}

func (s *SchedulerTask) Update(ctx context.Context, updateData map[string]interface{}, wheres map[string]interface{}, db *gorm.DB) (int64, error) {
	if len(updateData) == 0 || len(wheres) == 0 {
		return 0, nil
	}

	if db == nil {
		db = client.MySQLMaster
	}
	query := db.Model(&SchedulerTask{}).Debug()

	for k, v := range wheres {
		query = query.Where(k, v)
	}

	if err := query.Updates(updateData).Error; err != nil {
		return 0, err
	}

	return query.RowsAffected, nil

}

func (s *SchedulerTask) AutoFallback(ctx context.Context, db *gorm.DB) (int64, error) {
	if db == nil {
		db = client.MySQLMaster
	}

	query := db.Model(&SchedulerTask{}).Debug()
	if err := query.Where("status = ? and last_sched_time < NOW() - INTERVAL 10 SECOND ?", SchedulerTaskStatusScheduling).Error; err != nil {
		return 0, err
	}

	return query.RowsAffected, nil
}

func (s *SchedulerTask) Fallback(ctx context.Context, ids []uint64, db *gorm.DB) (int64, error) {
	wheres := map[string]interface{}{"id": ids}

	updateData := map[string]interface{}{
		"status": SchedulerTaskStatusPending,
	}
	return s.Update(ctx, updateData, wheres, db)
}

func (s *SchedulerTask) Scheduling(ctx context.Context, ids []uint64, db *gorm.DB) error {
	wheres := map[string]interface{}{
		"status": SchedulerTaskStatusPending,
		"id":     ids,
	}

	updateData := map[string]interface{}{
		"status": SchedulerTaskStatusScheduling,
	}

	rows, err := s.Update(ctx, updateData, wheres, db)
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no task updated")
	}

	return nil
}

func (s *SchedulerTask) Running(ctx context.Context, db *gorm.DB) error {
	wheres := map[string]interface{}{
		"status": SchedulerTaskStatusScheduling,
		"id":     s.ID,
	}

	updateData := map[string]interface{}{
		"status": SchedulerTaskStatusRunning,
	}

	rows, err := s.Update(ctx, updateData, wheres, db)
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no task updated")
	}

	return nil
}

func (s *SchedulerTask) Success(ctx context.Context, db *gorm.DB) error {
	wheres := map[string]interface{}{
		"status": SchedulerTaskStatusRunning,
		"id":     s.ID,
	}

	updateData := map[string]interface{}{
		"status": SchedulerTaskStatusSuccess,
	}

	rows, err := s.Update(ctx, updateData, wheres, db)
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no task updated")
	}

	return nil
}

func (s *SchedulerTask) Failed(ctx context.Context, db *gorm.DB) error {
	wheres := map[string]interface{}{
		"status": SchedulerTaskStatusRunning,
		"id":     s.ID,
	}

	updateData := map[string]interface{}{
		"status":      SchedulerTaskStatusFailed,
		"retry_count": "retry_count + 1",
	}

	rows, err := s.Update(ctx, updateData, wheres, db)
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no task updated")
	}

	return nil
}

func (s *SchedulerTask) Retrying(ctx context.Context, db *gorm.DB) error {
	wheres := map[string]interface{}{
		"status": SchedulerTaskStatusFailed,
		"id":     s.ID,
	}

	updateData := map[string]interface{}{
		"status": SchedulerTaskStatusRetrying,
	}
	rows, err := s.Update(ctx, updateData, wheres, db)
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no task updated")
	}

	return nil
}

func (s *SchedulerTask) Dead(ctx context.Context, db *gorm.DB) error {
	wheres := map[string]interface{}{
		"status": SchedulerTaskStatusFailed,
		"id":     s.ID,
	}

	updateData := map[string]interface{}{
		"status": SchedulerTaskStatusDead,
	}

	rows, err := s.Update(ctx, updateData, wheres, db)
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no task updated")
	}

	return nil
}
