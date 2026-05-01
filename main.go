package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/winterman/badis/proxy"
	"github.com/winterman/badis/server"
	"github.com/winterman/badis/store"
)

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

type StartStopper interface {
	Start() error
	Stop()
}

func main() {
	port := getEnvOrDefault("BADIS_PORT", ":6379")
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	isProxyStr := getEnvOrDefault("BADIS_PROXY_MODE", "false")
	isProxy, _ := strconv.ParseBool(isProxyStr)

	var srv StartStopper

	if isProxy {
		shardsStr := getEnvOrDefault("BADIS_SHARDS", "")
		if shardsStr == "" {
			log.Fatal("BADIS_SHARDS must be set in proxy mode")
		}
		var shards []string
		for _, s := range strings.Split(shardsStr, ",") {
			if s != "" {
				shards = append(shards, s)
			}
		}
		if len(shards) == 0 {
			log.Fatal("BADIS_SHARDS must contain at least one valid shard")
		}
		router := proxy.NewRouter(shards)
		srv = proxy.NewServer(port, router)
	} else {
		dbPath := getEnvOrDefault("BADIS_DATA_DIR", "badis-data")
		fsm, err := store.NewFSM(dbPath)
		if err != nil {
			log.Fatalf("Failed to initialize FSM: %v", err)
		}
		defer fsm.Close()
		srv = server.NewServer(port, fsm)
	}

	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errChan <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		log.Printf("Server failed: %v", err)
	case <-quit:
		log.Println("Shutting down...")
	}

	srv.Stop()
}
