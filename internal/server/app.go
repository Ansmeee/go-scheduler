package server

import (
	"context"
	"fmt"
	"go-scheduler/internal/client"
	"go-scheduler/internal/config"
	"go-scheduler/internal/logger"
	"go-scheduler/internal/service"
	"os"
	"os/signal"
	"syscall"
)

type App struct{}

func NewApp() *App {
	return &App{}
}

func (app *App) Start() error {
	if err := config.Load(); err != nil {
		fmt.Println("load config error:", err.Error())
		return err
	}

	client.Register()
	defer client.Close()

	if err := logger.Init(); err != nil {
		fmt.Println("init logger error:", err.Error())
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化 worker pool
	go service.NewWorkerPool(ctx).Start()
	fmt.Println("worker pool started")

	// 启动调度程序
	go service.NewScheduler(ctx).Start()
	fmt.Println("scheduler pool started")
	
	app.waitForSignal()
	return nil
}

func (app *App) LoadConfig() error {
	return nil
}

func (app *App) InitLogger() error {
	return nil
}

func (app *App) waitForSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sigChan
}
