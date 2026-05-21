package store

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

const prefixUser = byte('K')
const prefixType = byte('T')

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

func userKey(key string) []byte {
	k := make([]byte, len(key)+1)
	k[0] = prefixUser
	copy(k[1:], key)
	return k
}

func typeKey(key string) []byte {
	k := make([]byte, len(key)+1)
	k[0] = prefixType
	copy(k[1:], key)
	return k
}

func setValue(txn *badger.Txn, key string, val []byte, typ string, ttlMs int64) error {
	entry := badger.NewEntry(userKey(key), val)
	typeEntry := badger.NewEntry(typeKey(key), []byte(typ))
	if ttlMs > 0 {
		ttl := time.Duration(ttlMs) * time.Millisecond
		entry = entry.WithTTL(ttl)
		typeEntry = typeEntry.WithTTL(ttl)
	}
	if err := txn.SetEntry(entry); err != nil {
		return err
	}
	return txn.SetEntry(typeEntry)
}

func remainingTTLMs(item *badger.Item) int64 {
	expiresAt := item.ExpiresAt()
	if expiresAt == 0 {
		return 0
	}
	ttl := time.Until(time.Unix(int64(expiresAt), 0)).Milliseconds()
	if ttl < 1 {
		return 1
	}
	return ttl
}

func getExistingType(txn *badger.Txn, key string) (string, error) {
	item, err := txn.Get(typeKey(key))
	if err == badger.ErrKeyNotFound {
		return "string", nil
	} else if err != nil {
		return "", err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

func ensureExistingType(txn *badger.Txn, key string, want string) error {
	typ, err := getExistingType(txn, key)
	if err != nil {
		return err
	}
	if typ != want {
		return ErrWrongType
	}
	return nil
}

func redisGlobMatch(pattern string, text string) (bool, error) {
	var match func(int, int) (bool, error)
	match = func(pi, ti int) (bool, error) {
		for pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				for pi < len(pattern) && pattern[pi] == '*' {
					pi++
				}
				if pi == len(pattern) {
					return true, nil
				}
				for i := ti; i <= len(text); i++ {
					ok, err := match(pi, i)
					if err != nil || ok {
						return ok, err
					}
				}
				return false, nil
			case '?':
				if ti >= len(text) {
					return false, nil
				}
				pi++
				ti++
			case '[':
				end := pi + 1
				for end < len(pattern) && pattern[end] != ']' {
					end++
				}
				if end == len(pattern) {
					return false, fmt.Errorf("bad pattern")
				}
				if ti >= len(text) {
					return false, nil
				}
				negated := pattern[pi+1] == '^'
				start := pi + 1
				if negated {
					start++
				}
				matched := false
				for i := start; i < end; i++ {
					if i+2 < end && pattern[i+1] == '-' {
						if text[ti] >= pattern[i] && text[ti] <= pattern[i+2] {
							matched = true
						}
						i += 2
						continue
					}
					if text[ti] == pattern[i] {
						matched = true
					}
				}
				if matched == negated {
					return false, nil
				}
				pi = end + 1
				ti++
			case '\\':
				pi++
				if pi == len(pattern) {
					return false, fmt.Errorf("bad pattern")
				}
				fallthrough
			default:
				if ti >= len(text) || pattern[pi] != text[ti] {
					return false, nil
				}
				pi++
				ti++
			}
		}
		return ti == len(text), nil
	}
	return match(0, 0)
}

func sliceList(list []string, start int, stop int) []string {
	if len(list) == 0 {
		return []string{}
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
		return []string{}
	}
	return append([]string{}, list[start:stop+1]...)
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
		if err := ensureExistingType(txn, key, "string"); err != nil {
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

func (f *FSM) PTTL(key string) (int64, error) {
	var ttl int64 = -2
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1
		} else {
			ttl = time.Until(time.Unix(int64(expiresAt), 0)).Milliseconds()
			if ttl < 0 {
				ttl = -2
			}
		}
		return nil
	})
	return ttl, err
}

func (f *FSM) Type(key string) (string, error) {
	typ := "none"
	err := f.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		item, err := txn.Get(typeKey(key))
		if err == badger.ErrKeyNotFound {
			typ = "string"
			return nil
		} else if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		typ = string(val)
		return nil
	})
	return typ, err
}

func (f *FSM) Keys(pattern string) ([]string, error) {
	var keys []string
	err := f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte{prefixUser}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key()[1:])
			matched, err := redisGlobMatch(pattern, key)
			if err != nil {
				return err
			}
			if matched {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, err
}

func (f *FSM) StrLen(key string) (int, error) {
	val, err := f.GetString(key)
	if err != nil || val == nil {
		return 0, err
	}
	return len(val), nil
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	dec := codec.NewDecoderBytes(log.Data, &codec.MsgpackHandle{})
	if err := dec.Decode(&cmd); err != nil {
		return err
	}

	var result interface{}
	err := f.db.Update(func(txn *badger.Txn) error {
		var inErr error
		result, inErr = f.applyCmdInTxn(txn, &cmd)
		return inErr
	})

	if err != nil {
		return err
	}
	return result
}

func (f *FSM) applyCmdInTxn(txn *badger.Txn, cmd *Command) (interface{}, error) {
	var result interface{}
	switch cmd.Op {
	case "EVAL", "EVALSHA", "SCRIPT_LOAD", "SCRIPT_EXISTS", "SCRIPT_FLUSH":
		return f.handleLuaCommand(txn, cmd)
	case "SET", "GETSET", "APPEND":
		if len(cmd.Args) == 0 {
			return nil, fmt.Errorf("%s missing args", cmd.Op)
		}
		ukey := userKey(cmd.Key)
		var oldVal []byte
		exists := false

		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			exists = true
			if err := ensureExistingType(txn, cmd.Key, "string"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			if cmd.ReturnOld || cmd.Op == "GETSET" || cmd.Op == "APPEND" {
				oldVal, _ = item.ValueCopy(nil)
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		}

		setKey := true
		if cmd.Condition == "NX" && exists {
			setKey = false
		}
		if cmd.Condition == "XX" && !exists {
			setKey = false
		}

		newVal := cmd.Args[0]
		if cmd.Op == "APPEND" {
			newVal = append(append([]byte{}, oldVal...), cmd.Args[0]...)
		}

		if setKey {
			ttlMs := cmd.TTLMs
			if ttlMs == 0 && (cmd.Op == "APPEND") {
				ttlMs = oldTTLMs
			}
			if err := setValue(txn, cmd.Key, newVal, "string", ttlMs); err != nil {
				return nil, err
			}
		}

		if cmd.Op == "APPEND" {
			result = len(newVal)
			return result, nil
		}

		if cmd.ReturnOld || cmd.Op == "GETSET" {
			if exists {
				result = oldVal
			} else {
				result = nil
			}
			return result, nil
		}

		if !setKey {
			result = nil
			return result, nil
		}

		result = "OK"
		return result, nil
	case "MSET":
		if len(cmd.Args)%2 != 0 {
			return nil, fmt.Errorf("MSET invalid args")
		}
		for i := 0; i < len(cmd.Args); i += 2 {
			if err := setValue(txn, string(cmd.Args[i]), cmd.Args[i+1], "string", 0); err != nil {
				return nil, err
			}
		}
		result = "OK"
		return result, nil
	case "DEL":
		count := 0
		for _, k := range cmd.Args {
			ukey := userKey(string(k))
			_, err := txn.Get(ukey)
			if err == badger.ErrKeyNotFound {
				continue
			} else if err != nil {
				return nil, err
			}
			if err := txn.Delete(ukey); err != nil {
				return nil, err
			}
			_ = txn.Delete(typeKey(string(k)))
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
		return result, nil
	case "FLUSHDB":
		for _, prefix := range [][]byte{{prefixUser}, {prefixType}} {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
					it.Close()
					return nil, err
				}
			}
			it.Close()
		}
		result = "OK"
		return result, nil
	case "INCR", "DECR", "INCRBY", "DECRBY":
		ukey := userKey(cmd.Key)
		var val int64 = 0
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "string"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			b, _ := item.ValueCopy(nil)
			if parsed, err := strconv.ParseInt(string(b), 10, 64); err == nil {
				val = parsed
			} else {
				return nil, fmt.Errorf("value is not an integer or out of range")
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		}

		delta := int64(1)
		if cmd.Op == "INCRBY" || cmd.Op == "DECRBY" {
			var err error
			delta, err = strconv.ParseInt(string(cmd.Args[0]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("value is not an integer or out of range")
			}
		}
		if cmd.Op == "INCR" || cmd.Op == "INCRBY" {
			val += delta
		} else {
			val -= delta
		}

		if err := setValue(txn, cmd.Key, []byte(strconv.FormatInt(val, 10)), "string", oldTTLMs); err != nil {
			return nil, err
		}
		result = val
		return result, nil
	case "EXPIRE":
		ukey := userKey(cmd.Key)
		item, err := txn.Get(ukey)
		if err == badger.ErrKeyNotFound {
			result = 0 // false
			return result, nil
		} else if err != nil {
			return nil, err
		}

		val, _ := item.ValueCopy(nil)
		entry := badger.NewEntry(ukey, val).WithTTL(time.Duration(cmd.TTLMs) * time.Millisecond)
		if err := txn.SetEntry(entry); err != nil {
			return nil, err
		}
		result = 1 // true
		return result, nil
	case "PERSIST":
		ukey := userKey(cmd.Key)
		item, err := txn.Get(ukey)
		if err == badger.ErrKeyNotFound {
			result = 0
			return result, nil
		} else if err != nil {
			return nil, err
		}
		if item.ExpiresAt() == 0 {
			result = 0
			return result, nil
		}
		val, _ := item.ValueCopy(nil)
		typ := "string"
		if typeItem, err := txn.Get(typeKey(cmd.Key)); err == nil {
			typeVal, _ := typeItem.ValueCopy(nil)
			typ = string(typeVal)
		}
		if err := setValue(txn, cmd.Key, val, typ, 0); err != nil {
			return nil, err
		}
		result = 1
		return result, nil
	case "HSET", "HSETNX":
		if len(cmd.Args)%2 != 0 {
			return nil, fmt.Errorf("%s invalid args", cmd.Op)
		}
		ukey := userKey(cmd.Key)
		hash := make(map[string]string)
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "hash"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			val, _ := item.ValueCopy(nil)
			if err := decodeMsgpack(val, &hash); err != nil {
				return nil, err
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		}

		count := 0
		for i := 0; i < len(cmd.Args); i += 2 {
			field := string(cmd.Args[i])
			_, ok := hash[field]
			if cmd.Op == "HSETNX" && ok {
				continue
			}
			if !ok {
				count++
			}
			hash[field] = string(cmd.Args[i+1])
		}

		if err := setValue(txn, cmd.Key, encodeMsgpack(hash), "hash", oldTTLMs); err != nil {
			return nil, err
		}
		result = count
		return result, nil
	case "HDEL":
		ukey := userKey(cmd.Key)
		hash := make(map[string]string)
		item, err := txn.Get(ukey)
		if err == badger.ErrKeyNotFound {
			result = 0
			return result, nil
		} else if err != nil {
			return nil, err
		}
		val, _ := item.ValueCopy(nil)
		if err := ensureExistingType(txn, cmd.Key, "hash"); err != nil {
			return nil, err
		}
		oldTTLMs := remainingTTLMs(item)
		if err := decodeMsgpack(val, &hash); err != nil {
			return nil, err
		}
		count := 0
		for _, arg := range cmd.Args {
			field := string(arg)
			if _, ok := hash[field]; ok {
				delete(hash, field)
				count++
			}
		}
		if len(hash) == 0 {
			_ = txn.Delete(ukey)
			_ = txn.Delete(typeKey(cmd.Key))
		} else if err := setValue(txn, cmd.Key, encodeMsgpack(hash), "hash", oldTTLMs); err != nil {
			return nil, err
		}
		result = count
		return result, nil
	case "LPUSH", "RPUSH", "LPUSHX", "RPUSHX":
		ukey := userKey(cmd.Key)
		var list []string
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "list"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			val, _ := item.ValueCopy(nil)
			if err := decodeMsgpack(val, &list); err != nil {
				return nil, err
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		} else if cmd.Op == "LPUSHX" || cmd.Op == "RPUSHX" {
			result = 0
			return result, nil
		}

		for _, arg := range cmd.Args {
			if cmd.Op == "LPUSH" || cmd.Op == "LPUSHX" {
				list = append([]string{string(arg)}, list...)
			} else {
				list = append(list, string(arg))
			}
		}

		if err := setValue(txn, cmd.Key, encodeMsgpack(list), "list", oldTTLMs); err != nil {
			return nil, err
		}
		result = len(list)
		return result, nil
	case "LPOP", "RPOP":
		ukey := userKey(cmd.Key)
		var list []string
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "list"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			val, _ := item.ValueCopy(nil)
			if err := decodeMsgpack(val, &list); err != nil {
				return nil, err
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		}

		if len(list) == 0 {
			result = nil
			return result, nil
		}

		count := 1
		if len(cmd.Args) > 0 {
			parsed, err := strconv.Atoi(string(cmd.Args[0]))
			if err != nil || parsed < 0 {
				return nil, fmt.Errorf("value is not an integer or out of range")
			}
			count = parsed
		}
		if count > len(list) {
			count = len(list)
		}
		popped := make([]string, count)
		if cmd.Op == "LPOP" {
			copy(popped, list[:count])
			list = list[count:]
		} else {
			for i := 0; i < count; i++ {
				popped[i] = list[len(list)-1-i]
			}
			list = list[:len(list)-count]
		}

		if len(list) == 0 {
			_ = txn.Delete(ukey)
			_ = txn.Delete(typeKey(cmd.Key))
		} else {
			if err := setValue(txn, cmd.Key, encodeMsgpack(list), "list", oldTTLMs); err != nil {
				return nil, err
			}
		}
		if len(cmd.Args) > 0 {
			result = popped
		} else {
			result = []byte(popped[0])
		}
		return result, nil
	case "LTRIM":
		ukey := userKey(cmd.Key)
		var list []string
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "list"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			val, _ := item.ValueCopy(nil)
			if err := decodeMsgpack(val, &list); err != nil {
				return nil, err
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		}
		start, _ := strconv.Atoi(string(cmd.Args[0]))
		stop, _ := strconv.Atoi(string(cmd.Args[1]))
		list = sliceList(list, start, stop)
		if len(list) == 0 {
			_ = txn.Delete(ukey)
			_ = txn.Delete(typeKey(cmd.Key))
		} else {
			if err := setValue(txn, cmd.Key, encodeMsgpack(list), "list", oldTTLMs); err != nil {
				return nil, err
			}
		}
		result = "OK"
		return result, nil
	case "LREM":
		ukey := userKey(cmd.Key)
		var list []string
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "list"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			val, _ := item.ValueCopy(nil)
			if err := decodeMsgpack(val, &list); err != nil {
				return nil, err
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
		}
		limit, _ := strconv.Atoi(string(cmd.Args[0]))
		needle := string(cmd.Args[1])
		count := 0
		kept := make([]string, 0, len(list))
		if limit >= 0 {
			for _, v := range list {
				if v == needle && (limit == 0 || count < limit) {
					count++
					continue
				}
				kept = append(kept, v)
			}
		} else {
			for i := len(list) - 1; i >= 0; i-- {
				v := list[i]
				if v == needle && count < -limit {
					count++
					continue
				}
				kept = append([]string{v}, kept...)
			}
		}
		if len(kept) == 0 {
			_ = txn.Delete(ukey)
			_ = txn.Delete(typeKey(cmd.Key))
		} else {
			if err := setValue(txn, cmd.Key, encodeMsgpack(kept), "list", oldTTLMs); err != nil {
				return nil, err
			}
		}
		result = count
		return result, nil
	case "SADD":
		ukey := userKey(cmd.Key)
		set := make(map[string]struct{})
		item, err := txn.Get(ukey)
		oldTTLMs := int64(0)
		if err == nil {
			if err := ensureExistingType(txn, cmd.Key, "set"); err != nil {
				return nil, err
			}
			oldTTLMs = remainingTTLMs(item)
			val, _ := item.ValueCopy(nil)
			var members []string
			if err := decodeMsgpack(val, &members); err != nil {
				return nil, err
			}
			for _, m := range members {
				set[m] = struct{}{}
			}
		} else if err != badger.ErrKeyNotFound {
			return nil, err
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
		if err := setValue(txn, cmd.Key, encodeMsgpack(members), "set", oldTTLMs); err != nil {
			return nil, err
		}
		result = count
		return result, nil
	case "SREM", "SPOP":
		ukey := userKey(cmd.Key)
		set := make(map[string]struct{})
		item, err := txn.Get(ukey)
		if err == badger.ErrKeyNotFound {
			if cmd.Op == "SPOP" && len(cmd.Args) > 0 {
				result = []string{}
			} else if cmd.Op == "SPOP" {
				result = nil
			} else {
				result = 0
			}
			return result, nil
		} else if err != nil {
			return nil, err
		}
		val, _ := item.ValueCopy(nil)
		if err := ensureExistingType(txn, cmd.Key, "set"); err != nil {
			return nil, err
		}
		oldTTLMs := remainingTTLMs(item)
		var members []string
		if err := decodeMsgpack(val, &members); err != nil {
			return nil, err
		}
		for _, m := range members {
			set[m] = struct{}{}
		}
		count := 0
		var popped []string
		if cmd.Op == "SREM" {
			for _, arg := range cmd.Args {
				m := string(arg)
				if _, ok := set[m]; ok {
					delete(set, m)
					count++
				}
			}
		} else {
			count = 1
			if len(cmd.Args) > 0 {
				count, _ = strconv.Atoi(string(cmd.Args[0]))
			}
			for m := range set {
				if len(popped) >= count {
					break
				}
				popped = append(popped, m)
				delete(set, m)
			}
		}
		members = members[:0]
		for m := range set {
			members = append(members, m)
		}
		if len(members) == 0 {
			_ = txn.Delete(ukey)
			_ = txn.Delete(typeKey(cmd.Key))
		} else if err := setValue(txn, cmd.Key, encodeMsgpack(members), "set", oldTTLMs); err != nil {
			return nil, err
		}
		if cmd.Op == "SPOP" {
			if len(cmd.Args) > 0 {
				result = popped
			} else if len(popped) == 0 {
				result = nil
			} else {
				result = []byte(popped[0])
			}
		} else {
			result = count
		}
		return result, nil
		default:
			return nil, fmt.Errorf("unknown op: %s", cmd.Op)
		}

	return result, nil
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
		if err := ensureExistingType(txn, key, "hash"); err != nil {
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
		if err := ensureExistingType(txn, key, "hash"); err != nil {
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

func (f *FSM) HLen(key string) (int, error) {
	hash, err := f.HGetAll(key)
	if err != nil {
		return 0, err
	}
	return len(hash), nil
}

func (f *FSM) HExists(key string, field string) (int, error) {
	hash, err := f.HGetAll(key)
	if err != nil {
		return 0, err
	}
	if _, ok := hash[field]; ok {
		return 1, nil
	}
	return 0, nil
}

func (f *FSM) HMGet(key string, fields []string) ([][]byte, error) {
	hash, err := f.HGetAll(key)
	if err != nil {
		return nil, err
	}
	vals := make([][]byte, len(fields))
	for i, field := range fields {
		if val, ok := hash[field]; ok {
			vals[i] = []byte(val)
		}
	}
	return vals, nil
}

func (f *FSM) HKeys(key string) ([]string, error) {
	hash, err := f.HGetAll(key)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(hash))
	for k := range hash {
		keys = append(keys, k)
	}
	return keys, nil
}

func (f *FSM) HVals(key string) ([]string, error) {
	hash, err := f.HGetAll(key)
	if err != nil {
		return nil, err
	}
	vals := make([]string, 0, len(hash))
	for _, v := range hash {
		vals = append(vals, v)
	}
	return vals, nil
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
		if err := ensureExistingType(txn, key, "list"); err != nil {
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
	return sliceList(list, start, stop), nil
}

func (f *FSM) LLen(key string) (int, error) {
	list, err := f.LRange(key, 0, -1)
	if err != nil {
		return 0, err
	}
	return len(list), nil
}

func (f *FSM) LIndex(key string, index int) ([]byte, error) {
	list, err := f.LRange(key, 0, -1)
	if err != nil {
		return nil, err
	}
	if index < 0 {
		index = len(list) + index
	}
	if index < 0 || index >= len(list) {
		return nil, nil
	}
	return []byte(list[index]), nil
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
		if err := ensureExistingType(txn, key, "set"); err != nil {
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

func (f *FSM) SCard(key string) (int, error) {
	members, err := f.SMembers(key)
	if err != nil {
		return 0, err
	}
	return len(members), nil
}

func (f *FSM) SRandMember(key string, count *int) ([]string, error) {
	members, err := f.SMembers(key)
	if err != nil {
		return nil, err
	}
	if count != nil && *count < 0 {
		if len(members) == 0 {
			return []string{}, nil
		}
		res := make([]string, -*count)
		for i := range res {
			res[i] = members[i%len(members)]
		}
		return res, nil
	}
	if count == nil || *count >= len(members) {
		return members, nil
	}
	return members[:*count], nil
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
		if err := ensureExistingType(txn, key, "set"); err != nil {
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
