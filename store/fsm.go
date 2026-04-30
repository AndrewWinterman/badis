package store

import (
	"encoding/json"
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
			return txn.Set([]byte(cmd.Key), cmd.Args[0])
		case "DEL":
			return txn.Delete([]byte(cmd.Key))
		}
		return nil
	})
}

// Required Raft FSM methods
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &fsmSnapshot{}, nil }
func (f *FSM) Restore(io.ReadCloser) error { return nil }

type fsmSnapshot struct{}

func (s *fsmSnapshot) Persist(raft.SnapshotSink) error { return nil }
func (s *fsmSnapshot) Release()                        {}
