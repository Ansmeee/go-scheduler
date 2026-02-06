package service

type WorkerStatus int

const (
	WorkerStatusActive WorkerStatus = iota + 1
	WorkerStatusDown
	WorkerStatusBusy
)
