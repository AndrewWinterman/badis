package chaos

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/redis/go-redis/v9"
)

// Define a simple KV model for Porcupine (Linearizability checking)
type kvInput struct {
	op    uint8 // 0 for GET, 1 for SET
	value string
}

type kvOutput struct {
	value string
}

var kvModel = porcupine.Model{
	Init: func() interface{} {
		return ""
	},
	Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
		inp := input.(kvInput)
		out := output.(kvOutput)
		st := state.(string)

		if inp.op == 0 {
			// GET
			return out.value == st, state
		} else {
			// SET
			return true, inp.value
		}
	},
	Equal: func(state1, state2 interface{}) bool {
		return state1.(string) == state2.(string)
	},
}

func TestClusterLinearizability(t *testing.T) {
	if os.Getenv("RUN_CHAOS") == "" {
		t.Skip("Skipping chaos tests. Run with RUN_CHAOS=1")
	}

	tempDir, err := ioutil.TempDir("", "badis-chaos-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 1. Build the binary for the test
	t.Log("Building badis binary for chaos tests...")
	buildCmd := exec.Command("go", "build", "-o", filepath.Join(tempDir, "badis"), "../../main.go")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build badis: %v", err)
	}

	// 2. Start the cluster
	cluster := NewCluster(3, tempDir)
	for _, n := range cluster.Nodes {
		n.cmd = exec.Command(filepath.Join(tempDir, "badis"))
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
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	// Give cluster time to elect leader
	time.Sleep(5 * time.Second)

	var operations []porcupine.Operation
	var opsMu sync.Mutex
	ctx := context.Background()

	runWorkload := func(client *redis.Client, startIdx, endIdx int, wg *sync.WaitGroup) {
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(clientID int, c *redis.Client) {
				defer wg.Done()
				for j := startIdx; j < endIdx; j++ {
					start := time.Now()
					isSet := rand.Intn(2) == 0

					var inp kvInput
					var out kvOutput
					var err error

					if isSet {
						val := fmt.Sprintf("val-%d-%d", clientID, j)
						inp = kvInput{op: 1, value: val}
						err = c.Set(ctx, "chaos-key", val, 0).Err()
						out = kvOutput{}
					} else {
						inp = kvInput{op: 0}
						val, getErr := c.Get(ctx, "chaos-key").Result()
						if getErr == redis.Nil {
							val = ""
							getErr = nil
						}
						err = getErr
						out = kvOutput{value: val}
					}
					
					end := time.Now()

					// We only record successful operations for this simple test,
					// because failed operations might or might not have applied
					if err == nil {
						opsMu.Lock()
						operations = append(operations, porcupine.Operation{
							ClientId: clientID,
							Input:    inp,
							Call:     start.UnixNano(),
							Return:   end.UnixNano(),
							Output:   out,
						})
						opsMu.Unlock()
					}
					
					time.Sleep(10 * time.Millisecond)
				}
			}(i, client)
		}
	}

	// 3. Setup client to a node
	client1 := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", cluster.Nodes[0].Port),
	})
	defer client1.Close()

	var wg1 sync.WaitGroup
	// 4. Run concurrent workload
	runWorkload(client1, 0, 50, &wg1)

	// Wait a bit to get some operations recorded before the fault
	time.Sleep(500 * time.Millisecond)

	// 5. Inject a fault (kill the node we are talking to)
	t.Log("Injecting fault: Killing Node 0")
	cluster.Nodes[0].Stop()

	// Switch client to node 1
	client2 := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", cluster.Nodes[1].Port),
	})
	defer client2.Close()

	// Wait for failover
	time.Sleep(3 * time.Second)

	var wg2 sync.WaitGroup
	// Continue workload...
	runWorkload(client2, 50, 100, &wg2)

	wg1.Wait()
	wg2.Wait()
	
	// 6. Verify Linearizability
	opsMu.Lock()
	defer opsMu.Unlock()
	
	t.Logf("Checking linearizability for %d operations", len(operations))
	isLinearizable := porcupine.CheckOperations(kvModel, operations)
	if !isLinearizable {
		t.Fatal("History is not linearizable!")
	} else {
		t.Log("History is linearizable!")
	}
}
