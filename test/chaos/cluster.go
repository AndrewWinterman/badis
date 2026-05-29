package chaos

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

type Node struct {
	ID         string
	Port       int
	GossipPort int
	RaftPort   int
	Dir        string
	JoinAddr   string
	cmd        *exec.Cmd
}

func (n *Node) Start() error {
	if n.cmd == nil {
		n.cmd = exec.Command("go", "run", "../../main.go")
	}
	n.cmd.Env = append(os.Environ(),
		"BADIS_NODE_ID="+n.ID,
		"BADIS_PORT=:"+strconv.Itoa(n.Port),
		"BADIS_GOSSIP_PORT="+strconv.Itoa(n.GossipPort),
		"BADIS_RAFT_PORT="+strconv.Itoa(n.RaftPort),
		"BADIS_SHARD_ID=shard-1",
		"BADIS_DATA_DIR="+n.Dir,
		"BADIS_LEADER_ADMIN_PORT=31400",
	)
	if n.JoinAddr != "" {
		n.cmd.Env = append(n.cmd.Env, "BADIS_JOIN="+n.JoinAddr)
	}

	n.cmd.Stdout = os.Stdout
	n.cmd.Stderr = os.Stderr

	err := n.cmd.Start()
	if err == nil {
		fmt.Printf("Starting node %s on port %d with PID %d (cmd: %v)\n", n.ID, n.Port, n.cmd.Process.Pid, n.cmd.Args)
	}
	return err
}

func (n *Node) Stop() {
	if n.cmd != nil && n.cmd.Process != nil {
		n.cmd.Process.Kill()
		n.cmd.Wait()
	}
}

type Cluster struct {
	Nodes []*Node
}

func NewCluster(size int, baseDir string) *Cluster {
	c := &Cluster{}
	for i := 0; i < size; i++ {
		joinAddr := ""
		if i > 0 {
			joinAddr = "127.0.0.1:30946" // First node gossip port
		}
		c.Nodes = append(c.Nodes, &Node{
			ID:         fmt.Sprintf("node-%d", i),
			Port:       29379 + i,
			GossipPort: 30946 + i,
			RaftPort:   31300 + i,
			Dir:        filepath.Join(baseDir, fmt.Sprintf("data-%d", i)),
			JoinAddr:   joinAddr,
		})
	}
	return c
}

func (c *Cluster) Start() error {
	for _, n := range c.Nodes {
		if err := n.Start(); err != nil {
			return err
		}
		time.Sleep(1 * time.Second) // Give it a sec to bind
	}
	return nil
}

func (c *Cluster) Stop() {
	for _, n := range c.Nodes {
		n.Stop()
	}
}
