package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/winterman/badis/server"
	"github.com/winterman/badis/store"
)

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	dbPath := getEnvOrDefault("BADIS_DATA_DIR", "badis-data")
	port := getEnvOrDefault("BADIS_PORT", ":6379")
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize FSM: %v", err)
	}
	defer fsm.Close()

	srv := server.NewServer(port, fsm)

	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errChan <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	
	select {
	case <-quit:
		log.Println("Shutting down server...")
	case err := <-errChan:
		log.Printf("Server failed: %v", err)
	}

	srv.Stop()
}
