package store

import (
	"bytes"
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

var (
	prefixLog    = []byte{'L'}
	prefixStable = []byte{'S'}
)

func logKey(index uint64) []byte {
	k := make([]byte, 9)
	k[0] = prefixLog[0]
	binary.BigEndian.PutUint64(k[1:], index)
	return k
}

func stableKey(key []byte) []byte {
	k := make([]byte, 1+len(key))
	k[0] = prefixStable[0]
	copy(k[1:], key)
	return k
}

// Encode/Decode raft.Log using msgpack
func encodeLog(log *raft.Log) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(log); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeLog(buf []byte, log *raft.Log) error {
	dec := codec.NewDecoderBytes(buf, &codec.MsgpackHandle{})
	return dec.Decode(log)
}

// FirstIndex returns the first index written. 0 for no entries.
func (f *FSM) FirstIndex() (uint64, error) {
	var idx uint64
	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(prefixLog)
		if it.ValidForPrefix(prefixLog) {
			k := it.Item().Key()
			idx = binary.BigEndian.Uint64(k[1:])
		}
		return nil
	})
	return idx, err
}

// LastIndex returns the last index written. 0 for no entries.
func (f *FSM) LastIndex() (uint64, error) {
	var idx uint64
	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to the end of the log prefix
		prefixEnd := append([]byte(nil), prefixLog...)
		prefixEnd[len(prefixEnd)-1]++
		it.Seek(prefixEnd)
		if it.ValidForPrefix(prefixLog) {
			k := it.Item().Key()
			idx = binary.BigEndian.Uint64(k[1:])
		}
		return nil
	})
	return idx, err
}

// GetLog gets a log entry at a given index.
func (f *FSM) GetLog(index uint64, log *raft.Log) error {
	return f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(index))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return raft.ErrLogNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return decodeLog(val, log)
		})
	})
}

// StoreLog stores a log entry.
func (f *FSM) StoreLog(log *raft.Log) error {
	return f.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (f *FSM) StoreLogs(logs []*raft.Log) error {
	type logData struct {
		key []byte
		val []byte
	}
	var data []logData
	for _, log := range logs {
		val, err := encodeLog(log)
		if err != nil {
			return err
		}
		data = append(data, logData{key: logKey(log.Index), val: val})
	}

	return f.db.Update(func(txn *badger.Txn) error {
		for _, d := range data {
			if err := txn.Set(d.key, d.val); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (f *FSM) DeleteRange(min, max uint64) error {
	return f.db.Update(func(txn *badger.Txn) error {
		for i := min; i <= max; i++ {
			if err := txn.Delete(logKey(i)); err != nil {
				if err != badger.ErrKeyNotFound {
					return err
				}
			}
		}
		return nil
	})
}

// Set is used to set a key/value set outside of the raft log
func (f *FSM) Set(key []byte, val []byte) error {
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(stableKey(key), val)
	})
}

// Get is used to retrieve a value from the k/v store by key
func (f *FSM) Get(key []byte) ([]byte, error) {
	var val []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(stableKey(key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil // Return empty value
	}
	return val, err
}

// SetUint64 is like Set, but handles uint64 values
func (f *FSM) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return f.Set(key, b)
}

// GetUint64 is like Get, but handles uint64 values
func (f *FSM) GetUint64(key []byte) (uint64, error) {
	val, err := f.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(val), nil
}
