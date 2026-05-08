package main

import (
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/winterman/badis/cluster"
	"github.com/winterman/badis/config"
	"github.com/winterman/badis/router"
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

	nodeID := getEnvOrDefault("BADIS_NODE_ID", "local-node")

	gossipPort := getEnvOrDefault("BADIS_GOSSIP_PORT", "7946")
	joinAddrs := getEnvOrDefault("BADIS_JOIN", "")
	var joinList []string
	if joinAddrs != "" {
		joinList = strings.Split(joinAddrs, ",")
	}

	gossipNode, err := cluster.NewGossip("0.0.0.0:"+gossipPort, nodeID, joinList)
	if err != nil {
		slog.Error("failed to start gossip", "error", err)
		os.Exit(1)
	}
	defer gossipNode.Shutdown()

	slotMap := config.NewSlotMap()

	r := router.NewRouter(nil, slotMap, nodeID)

	dbPath := getEnvOrDefault("BADIS_DATA_DIR", "badis-data")
	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		slog.Error("Failed to initialize FSM", "error", err)
		os.Exit(1)
	}
	defer fsm.Close()
	srv := server.NewServer(port, fsm, r)

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
		slog.Error("Server failed", "error", err)
	case <-quit:
		slog.Info("Shutting down...")
	}

	srv.Stop()
}
