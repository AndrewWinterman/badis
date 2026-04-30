package main

import (
	"log"
	"os"
	"os/signal"
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

	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize FSM: %v", err)
	}

	srv := server.NewServer(port, fsm)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	srv.Stop()
}
