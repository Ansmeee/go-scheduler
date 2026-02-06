package main

import (
	"fmt"
	"go-scheduler/internal/server"
	"os"
)

func main() {
	if err := server.NewApp().Start(); err != nil {
		fmt.Printf("app start error: %s", err.Error())
		os.Exit(1)
	}
}
