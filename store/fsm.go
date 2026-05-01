package store

import (
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

const prefixUser = byte('K')

func userKey(key string) []byte {
	k := make([]byte, len(key)+1)
	k[0] = prefixUser
	copy(k[1:], key)
	return k
}

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

func (f *FSM) GetString(key string) ([]byte, error) {
	var val []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
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
	dec := codec.NewDecoderBytes(log.Data, &codec.MsgpackHandle{})
	if err := dec.Decode(&cmd); err != nil {
		return err
	}

	return f.db.Update(func(txn *badger.Txn) error {
		switch cmd.Op {
		case "SET":
			if len(cmd.Args) == 0 {
				return fmt.Errorf("SET missing args")
			}
			return txn.Set(userKey(cmd.Key), cmd.Args[0])
		case "DEL":
			return txn.Delete(userKey(cmd.Key))
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
