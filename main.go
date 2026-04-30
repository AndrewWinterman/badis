package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/winterman/badis/server"
	"github.com/winterman/badis/store"
)

func main() {
	dbPath := "badis-data"
	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize FSM: %v", err)
	}

	srv := server.NewServer(":6379", fsm)

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
