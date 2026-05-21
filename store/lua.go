package store

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
	lua "github.com/yuin/gopher-lua"
)

const scriptPrefix = "SCRIPT:"

func getScriptKey(sha string) []byte {
	return []byte(scriptPrefix + sha)
}

func (f *FSM) handleLuaCommand(txn *badger.Txn, cmd *Command) (interface{}, error) {
	switch cmd.Op {
	case "SCRIPT_LOAD":
		if len(cmd.Args) != 1 {
			return nil, fmt.Errorf("ERR wrong number of arguments for SCRIPT LOAD")
		}
		script := cmd.Args[0]
		h := sha1.New()
		h.Write(script)
		sha := hex.EncodeToString(h.Sum(nil))
		
		err := txn.Set(getScriptKey(sha), script)
		if err != nil {
			return nil, err
		}
		return sha, nil

	case "SCRIPT_EXISTS":
		res := make([]int64, len(cmd.Args))
		for i, shaBytes := range cmd.Args {
			_, err := txn.Get(getScriptKey(string(shaBytes)))
			if err == badger.ErrKeyNotFound {
				res[i] = 0
			} else if err != nil {
				return nil, err
			} else {
				res[i] = 1
			}
		}
		return res, nil

	case "SCRIPT_FLUSH":
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(scriptPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if err := txn.Delete(item.Key()); err != nil {
				return nil, err
			}
		}
		return "OK", nil

	case "EVALSHA":
		if len(cmd.Args) < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for EVALSHA")
		}
		sha := string(cmd.Args[0])
		item, err := txn.Get(getScriptKey(sha))
		if err == badger.ErrKeyNotFound {
			return nil, fmt.Errorf("NOSCRIPT No matching script. Please use EVAL.")
		} else if err != nil {
			return nil, err
		}
		scriptBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		return f.runLua(txn, string(scriptBytes), cmd.Args[1:])

	case "EVAL":
		if len(cmd.Args) < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for EVAL")
		}
		script := string(cmd.Args[0])
		return f.runLua(txn, script, cmd.Args[1:])
	}
	return nil, fmt.Errorf("unknown lua command")
}

func (f *FSM) runLua(txn *badger.Txn, script string, evalArgs [][]byte) (interface{}, error) {
	numKeysStr := string(evalArgs[0])
	numKeys, err := strconv.Atoi(numKeysStr)
	if err != nil || numKeys < 0 {
		return nil, fmt.Errorf("ERR value is not an integer or out of range")
	}

	if len(evalArgs)-1 < numKeys {
		return nil, fmt.Errorf("ERR Number of keys can't be greater than number of args")
	}

	keys := evalArgs[1 : 1+numKeys]
	argv := evalArgs[1+numKeys:]

	L := lua.NewState()
	defer L.Close()

	// Set KEYS table
	keysTable := L.NewTable()
	for i, k := range keys {
		L.SetTable(keysTable, lua.LNumber(i+1), lua.LString(k))
	}
	L.SetGlobal("KEYS", keysTable)

	// Set ARGV table
	argvTable := L.NewTable()
	for i, a := range argv {
		L.SetTable(argvTable, lua.LNumber(i+1), lua.LString(a))
	}
	L.SetGlobal("ARGV", argvTable)

	// Inject redis.call and redis.pcall
	redisTable := L.NewTable()
	L.SetField(redisTable, "call", L.NewFunction(func(L *lua.LState) int {
		res, err := f.luaRedisCall(txn, L)
		if err != nil {
			L.RaiseError(err.Error())
			return 0
		}
		L.Push(res)
		return 1
	}))
	L.SetField(redisTable, "pcall", L.NewFunction(func(L *lua.LState) int {
		res, err := f.luaRedisCall(txn, L)
		if err != nil {
			errTable := L.NewTable()
			L.SetField(errTable, "err", lua.LString(err.Error()))
			L.Push(errTable)
			return 1
		}
		L.Push(res)
		return 1
	}))
	L.SetGlobal("redis", redisTable)

	if err := L.DoString(script); err != nil {
		return nil, fmt.Errorf("ERR Error running script: %s", err.Error())
	}

	ret := L.Get(-1)
	return luaToGoValue(ret), nil
}

func (f *FSM) luaRedisCall(txn *badger.Txn, L *lua.LState) (lua.LValue, error) {
	top := L.GetTop()
	if top == 0 {
		return nil, fmt.Errorf("Please specify at least one argument for redis.call()")
	}

	op := strings.ToUpper(L.ToString(1))
	
	// Handle Read-Only commands directly using the current transaction
	switch op {
	case "GET":
		if top != 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for 'get' command")
		}
		key := L.ToString(2)
		item, err := txn.Get(userKey(key))
		if err == badger.ErrKeyNotFound {
			return lua.LNil, nil
		} else if err != nil {
			return nil, err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		return lua.LString(string(val)), nil
	case "EXISTS":
		if top < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for 'exists' command")
		}
		count := 0
		for i := 2; i <= top; i++ {
			key := L.ToString(i)
			_, err := txn.Get(userKey(key))
			if err == nil {
				count++
			} else if err != badger.ErrKeyNotFound {
				return nil, err
			}
		}
		return lua.LNumber(count), nil
	case "TYPE":
		if top != 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for 'type' command")
		}
		key := L.ToString(2)
		item, err := txn.Get(typeKey(key))
		if err == badger.ErrKeyNotFound {
			return lua.LString("none"), nil
		} else if err != nil {
			return nil, err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		return lua.LString(string(val)), nil
	}

	cmd := &Command{Op: op}

	if top > 1 {
		cmd.Key = L.ToString(2)
	}
	
	if top > 2 {
		for i := 3; i <= top; i++ {
			cmd.Args = append(cmd.Args, []byte(L.ToString(i)))
		}
	}

	// We don't support passing ReturnOld/TTL/Condition directly from simple redis.call
	// except if it's encoded in the standard args (like SET key val EX 10).
	// But our FSM applyCmdInTxn currently expects them in `cmd` struct fields.
	// Oh, wait! applyCmdInTxn uses `cmd.Args` for everything, or does it?
	// In fsm.go: SET uses cmd.TTLMs, cmd.Condition, etc! This is because the server
	// parsed it. So we need to parse standard redis args into the Command struct here!
	
	// Let's implement a quick parser for standard commands so Lua can do redis.call("SET", "k", "v", "EX", "10")
	err := parseCommandForLua(op, L, cmd)
	if err != nil {
		return nil, err
	}

	res, err := f.applyCmdInTxn(txn, cmd)
	if err != nil {
		return nil, err
	}

	return goToLuaValue(L, res), nil
}

func parseCommandForLua(op string, L *lua.LState, cmd *Command) error {
	top := L.GetTop()
	switch op {
	case "SET":
		if top < 3 {
			return fmt.Errorf("ERR wrong number of arguments for 'set' command")
		}
		cmd.Key = L.ToString(2)
		cmd.Args = [][]byte{[]byte(L.ToString(3))}
		for i := 4; i <= top; i++ {
			arg := strings.ToUpper(L.ToString(i))
			switch arg {
			case "NX", "XX":
				cmd.Condition = arg
			case "GET":
				cmd.ReturnOld = true
			case "EX":
				if i+1 > top {
					return fmt.Errorf("ERR syntax error")
				}
				i++
				ttl, err := strconv.ParseInt(L.ToString(i), 10, 64)
				if err != nil {
					return fmt.Errorf("ERR value is not an integer or out of range")
				}
				cmd.TTLMs = ttl * 1000
			case "PX":
				if i+1 > top {
					return fmt.Errorf("ERR syntax error")
				}
				i++
				ttl, err := strconv.ParseInt(L.ToString(i), 10, 64)
				if err != nil {
					return fmt.Errorf("ERR value is not an integer or out of range")
				}
				cmd.TTLMs = ttl
			}
		}
	// For other commands that might have special struct fields, we add them here.
	// For most commands, op, key, args is enough.
	default:
		cmd.Key = ""
		if top > 1 {
			cmd.Key = L.ToString(2)
		}
		cmd.Args = nil
		if top > 2 {
			for i := 3; i <= top; i++ {
				cmd.Args = append(cmd.Args, []byte(L.ToString(i)))
			}
		}
	}
	return nil
}

func goToLuaValue(L *lua.LState, val interface{}) lua.LValue {
	if val == nil {
		return lua.LFalse // standard Redis Lua returns false for nil
	}
	switch v := val.(type) {
	case string:
		return lua.LString(v)
	case []byte:
		return lua.LString(string(v))
	case int:
		return lua.LNumber(v)
	case int64:
		return lua.LNumber(v)
	case float64:
		return lua.LNumber(v)
	case bool:
		return lua.LBool(v)
	case []string:
		t := L.NewTable()
		for i, s := range v {
			L.SetTable(t, lua.LNumber(i+1), lua.LString(s))
		}
		return t
	case [][]byte:
		t := L.NewTable()
		for i, b := range v {
			if b == nil {
				L.SetTable(t, lua.LNumber(i+1), lua.LFalse)
			} else {
				L.SetTable(t, lua.LNumber(i+1), lua.LString(string(b)))
			}
		}
		return t
	case map[string]string:
		t := L.NewTable()
		i := 1
		for k, val := range v {
			L.SetTable(t, lua.LNumber(i), lua.LString(k))
			L.SetTable(t, lua.LNumber(i+1), lua.LString(val))
			i += 2
		}
		return t
	default:
		return lua.LString(fmt.Sprintf("%v", v))
	}
}

func luaToGoValue(val lua.LValue) interface{} {
	switch val.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		if val == lua.LTrue {
			return int64(1)
		}
		return nil
	case lua.LTNumber:
		return int64(lua.LVAsNumber(val))
	case lua.LTString:
		return string(val.(lua.LString))
	case lua.LTTable:
		t := val.(*lua.LTable)
		var arr []interface{}
		t.ForEach(func(k, v lua.LValue) {
			arr = append(arr, luaToGoValue(v))
		})
		return arr
	default:
		return val.String()
	}
}
