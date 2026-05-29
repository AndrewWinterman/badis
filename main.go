package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
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
	verbose := getEnvOrDefault("BADIS_VERBOSE", "") == "1"

	logOutput := io.Discard
	logLevel := slog.LevelError
	if verbose {
		logOutput = os.Stderr
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	port := getEnvOrDefault("BADIS_PORT", ":6379")
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	nodeID := getEnvOrDefault("BADIS_NODE_ID", "local-node")

	gossipPort := getEnvOrDefault("BADIS_GOSSIP_PORT", "7946")
	raftPort := getEnvOrDefault("BADIS_RAFT_PORT", "8300")
	shardID := getEnvOrDefault("BADIS_SHARD_ID", "shard-1")

	joinAddrs := getEnvOrDefault("BADIS_JOIN", "")
	var joinList []string
	if joinAddrs != "" {
		joinList = strings.Split(joinAddrs, ",")
	}

	gossipNode, err := cluster.NewGossipWithOptions(
		"0.0.0.0:"+gossipPort,
		nodeID,
		joinList,
		cluster.WithLogOutput(logOutput),
	)
	if err != nil {
		logger.Error("failed to start gossip", "error", err)
		os.Exit(1)
	}
	defer gossipNode.Shutdown()

	slotMap := config.NewSlotMap()

	r := router.NewRouter(nil, slotMap, nodeID)

	dbPath := getEnvOrDefault("BADIS_DATA_DIR", "badis-data")
	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		logger.Error("failed to initialize fsm", "error", err)
		os.Exit(1)
	}
	defer fsm.Close()
	srv := server.NewServerWithOptions(port, fsm, r, shardID, server.WithLogOutput(logOutput))

	rPort, err := strconv.Atoi(raftPort)
	if err != nil {
		logger.Error("invalid raft port", "error", err)
		os.Exit(1)
	}
	adminPort := ":" + strconv.Itoa(rPort+100)
	srv.StartAdmin(adminPort)

	err = srv.SetupRaft(nodeID, "127.0.0.1:"+raftPort)
	if err != nil {
		logger.Error("failed to setup raft", "error", err)
		os.Exit(1)
	}

	time.Sleep(2 * time.Second)

	if len(joinList) == 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress("127.0.0.1:" + raftPort),
				},
			},
		}
		srv.GetRaft().BootstrapCluster(configuration)
	} else {
		// join first node
		peer := joinList[0]
		host := strings.Split(peer, ":")[0]
		leaderAdminPort := getEnvOrDefault("BADIS_LEADER_ADMIN_PORT", "8400")
		joinURL := fmt.Sprintf("http://%s:%s/join?id=%s&addr=127.0.0.1:%s", host, leaderAdminPort, nodeID, raftPort)

		// Retry join a few times in case leader is still electing
		for i := 0; i < 10; i++ {
			resp, err := http.Get(joinURL)
			if err != nil {
				logger.Error("failed to join cluster", "error", err)
			} else if resp.StatusCode != http.StatusOK {
				logger.Error("failed to join cluster", "status", resp.Status)
				resp.Body.Close()
			} else {
				logger.Info("joined cluster via peer", "peer", peer)
				resp.Body.Close()
				break
			}
			time.Sleep(1 * time.Second)
		}
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
		logger.Error("server failed", "error", err)
	case <-quit:
		logger.Info("shutting down")
	}

	srv.Stop()
}
