package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/winterman/badis/server"
)

func main() {
	srv := server.NewServer(":6379")
	
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
