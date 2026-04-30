package store

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type Command struct {
	Op   string
	Key  string
	Args [][]byte
}

type FSM struct {
	db *badger.DB
}

func NewFSM(path string) (*FSM, error) {
	opts := badger.DefaultOptions(path).WithLoggingLevel(badger.WARNING)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &FSM{db: db}, nil
}

func (f *FSM) Close() error { return f.db.Close() }

func (f *FSM) Get(key string) ([]byte, error) {
	var val []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	return f.db.Update(func(txn *badger.Txn) error {
		switch cmd.Op {
		case "SET":
			if len(cmd.Args) == 0 {
				return fmt.Errorf("SET missing args")
			}
			return txn.Set([]byte(cmd.Key), cmd.Args[0])
		case "DEL":
			return txn.Delete([]byte(cmd.Key))
		default:
			return fmt.Errorf("unknown op: %s", cmd.Op)
		}
	})
}

// Required Raft FSM methods
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{db: f.db}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	return f.db.Load(rc, 10)
}

type fsmSnapshot struct {
	db *badger.DB
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := s.db.Backup(sink, 0)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
