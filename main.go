package main

import (
	"log"

	"github.com/winterman/badis/server"
)

func main() {
	srv := server.NewServer(":6379")
	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
