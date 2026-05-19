package store

import (
	"fmt"
	"io"
	"strconv"
	"time"

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
	Op        string
	Key       string
	Args      [][]byte
	TTLMs     int64
	Condition string // "NX" or "XX"
	ReturnOld bool
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

func (f *FSM) GetStrings(keys []string) ([][]byte, error) {
	res := make([][]byte, len(keys))
	err := f.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			item, err := txn.Get(userKey(key))
			if err == badger.ErrKeyNotFound {
				res[i] = nil
				continue
			} else if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			res[i] = val
		}
		return nil
	})
	return res, err
}

func (f *FSM) Exists(keys []string) (int, error) {
	count := 0
	err := f.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			_, err := txn.Get(userKey(key))
			if err == badger.ErrKeyNotFound {
				continue
			} else if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

func (f *FSM) TTL(key string) (int64, error) {
	var ttl int64 = -2 // -2 means key does not exist
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		
		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1 means exists but no associated expire
		} else {
			ttl = int64(time.Until(time.Unix(int64(expiresAt), 0)).Seconds())
			if ttl < 0 {
				ttl = -2
			}
		}
		return nil
	})
	return ttl, err
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	dec := codec.NewDecoderBytes(log.Data, &codec.MsgpackHandle{})
	if err := dec.Decode(&cmd); err != nil {
		return err
	}

	var result interface{}
	err := f.db.Update(func(txn *badger.Txn) error {
		switch cmd.Op {
		case "SET":
			if len(cmd.Args) == 0 {
				return fmt.Errorf("SET missing args")
			}
			ukey := userKey(cmd.Key)
			var oldVal []byte
			exists := false

			item, err := txn.Get(ukey)
			if err == nil {
				exists = true
				if cmd.ReturnOld {
					oldVal, _ = item.ValueCopy(nil)
				}
			} else if err != badger.ErrKeyNotFound {
				return err
			}

			setKey := true
			if cmd.Condition == "NX" && exists {
				setKey = false
			}
			if cmd.Condition == "XX" && !exists {
				setKey = false
			}

			if setKey {
				entry := badger.NewEntry(ukey, cmd.Args[0])
				if cmd.TTLMs > 0 {
					entry = entry.WithTTL(time.Duration(cmd.TTLMs) * time.Millisecond)
				}
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
			}

			if cmd.ReturnOld {
				if exists {
					result = oldVal
				} else {
					result = nil
				}
				return nil
			}

			if !setKey {
				result = nil
				return nil
			}

			result = "OK"
			return nil
		case "MSET":
			if len(cmd.Args)%2 != 0 {
				return fmt.Errorf("MSET invalid args")
			}
			for i := 0; i < len(cmd.Args); i += 2 {
				k := userKey(string(cmd.Args[i]))
				if err := txn.Set(k, cmd.Args[i+1]); err != nil {
					return err
				}
			}
			result = "OK"
			return nil
		case "DEL":
			count := 0
			for _, k := range cmd.Args {
				ukey := userKey(string(k))
				_, err := txn.Get(ukey)
				if err == badger.ErrKeyNotFound {
					continue
				} else if err != nil {
					return err
				}
				if err := txn.Delete(ukey); err != nil {
					return err
				}
				count++
			}
			// For backward compatibility if args is empty, check key
			if len(cmd.Args) == 0 && cmd.Key != "" {
				ukey := userKey(cmd.Key)
				_, err := txn.Get(ukey)
				if err == nil {
					if err := txn.Delete(ukey); err == nil {
						count++
					}
				}
			}
			result = count
			return nil
		case "INCR", "DECR":
			ukey := userKey(cmd.Key)
			var val int64 = 0
			item, err := txn.Get(ukey)
			if err == nil {
				b, _ := item.ValueCopy(nil)
				if parsed, err := strconv.ParseInt(string(b), 10, 64); err == nil {
					val = parsed
				}
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			
			if cmd.Op == "INCR" {
				val++
			} else {
				val--
			}
			
			if err := txn.Set(ukey, []byte(strconv.FormatInt(val, 10))); err != nil {
				return err
			}
			result = val
			return nil
		case "EXPIRE":
			ukey := userKey(cmd.Key)
			item, err := txn.Get(ukey)
			if err == badger.ErrKeyNotFound {
				result = 0 // false
				return nil
			} else if err != nil {
				return err
			}
			
			val, _ := item.ValueCopy(nil)
			entry := badger.NewEntry(ukey, val).WithTTL(time.Duration(cmd.TTLMs) * time.Millisecond)
			if err := txn.SetEntry(entry); err != nil {
				return err
			}
			result = 1 // true
			return nil
		case "HSET":
			if len(cmd.Args)%2 != 0 {
				return fmt.Errorf("HSET invalid args")
			}
			ukey := userKey(cmd.Key)
			hash := make(map[string]string)
			item, err := txn.Get(ukey)
			if err == nil {
				val, _ := item.ValueCopy(nil)
				_ = decodeMsgpack(val, &hash)
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			
			count := 0
			for i := 0; i < len(cmd.Args); i += 2 {
				field := string(cmd.Args[i])
				if _, ok := hash[field]; !ok {
					count++
				}
				hash[field] = string(cmd.Args[i+1])
			}
			
			if err := txn.Set(ukey, encodeMsgpack(hash)); err != nil {
				return err
			}
			result = count
			return nil
		case "LPUSH", "RPUSH":
			ukey := userKey(cmd.Key)
			var list []string
			item, err := txn.Get(ukey)
			if err == nil {
				val, _ := item.ValueCopy(nil)
				_ = decodeMsgpack(val, &list)
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			
			for _, arg := range cmd.Args {
				if cmd.Op == "LPUSH" {
					list = append([]string{string(arg)}, list...)
				} else {
					list = append(list, string(arg))
				}
			}
			
			if err := txn.Set(ukey, encodeMsgpack(list)); err != nil {
				return err
			}
			result = len(list)
			return nil
		case "LPOP", "RPOP":
			ukey := userKey(cmd.Key)
			var list []string
			item, err := txn.Get(ukey)
			if err == nil {
				val, _ := item.ValueCopy(nil)
				_ = decodeMsgpack(val, &list)
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			
			if len(list) == 0 {
				result = nil
				return nil
			}
			
			var popped string
			if cmd.Op == "LPOP" {
				popped = list[0]
				list = list[1:]
			} else {
				popped = list[len(list)-1]
				list = list[:len(list)-1]
			}
			
			if err := txn.Set(ukey, encodeMsgpack(list)); err != nil {
				return err
			}
			result = []byte(popped)
			return nil
		case "SADD":
			ukey := userKey(cmd.Key)
			set := make(map[string]struct{})
			item, err := txn.Get(ukey)
			if err == nil {
				val, _ := item.ValueCopy(nil)
				var members []string
				_ = decodeMsgpack(val, &members)
				for _, m := range members {
					set[m] = struct{}{}
				}
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			
			count := 0
			for _, arg := range cmd.Args {
				sarg := string(arg)
				if _, ok := set[sarg]; !ok {
					set[sarg] = struct{}{}
					count++
				}
			}
			
			var members []string
			for k := range set {
				members = append(members, k)
			}
			if err := txn.Set(ukey, encodeMsgpack(members)); err != nil {
				return err
			}
			result = count
			return nil
		default:
			return fmt.Errorf("unknown op: %s", cmd.Op)
		}
	})

	if err != nil {
		return err
	}
	return result
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

func (f *FSM) HGet(key string, field string) ([]byte, error) {
	var val []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		hash := make(map[string]string)
		if err := decodeMsgpack(data, &hash); err != nil {
			return err
		}
		if v, ok := hash[field]; ok {
			val = []byte(v)
		}
		return nil
	})
	return val, err
}

func (f *FSM) HGetAll(key string) (map[string]string, error) {
	hash := make(map[string]string)
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return decodeMsgpack(data, &hash)
	})
	return hash, err
}

func (f *FSM) LRange(key string, start int, stop int) ([]string, error) {
	var list []string
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return decodeMsgpack(data, &list)
	})
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return []string{}, nil
	}
	if start < 0 {
		start = len(list) + start
	}
	if stop < 0 {
		stop = len(list) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(list) {
		stop = len(list) - 1
	}
	if start > stop || start >= len(list) {
		return []string{}, nil
	}
	return list[start : stop+1], nil
}

func (f *FSM) SMembers(key string) ([]string, error) {
	var members []string
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return decodeMsgpack(data, &members)
	})
	return members, err
}

func (f *FSM) SIsMember(key string, member string) (int, error) {
	var members []string
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return decodeMsgpack(data, &members)
	})
	if err != nil {
		return 0, err
	}
	for _, m := range members {
		if m == member {
			return 1, nil
		}
	}
	return 0, nil
}
