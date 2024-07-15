package handler

import (
	"myRedis/resp"
	"sync"
)

var Handlers = map[string]func([]resp.Value) resp.Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetAll,
}

type data struct {
	set     map[string]string
	hashMap map[string]map[string]string
}

var d = data{
	set:     make(map[string]string),
	hashMap: make(map[string]map[string]string),
}

func ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}

	return resp.Value{Typ: "string", Str: args[0].Bulk}
}

func set(args []resp.Value) resp.Value {
	mutexLock := &sync.RWMutex{}

	key := args[0].Bulk
	value := args[1].Bulk

	mutexLock.Lock()
	d.set[key] = value
	mutexLock.Unlock()

	return resp.Value{Typ: "string", Str: "OK"}
}

func get(args []resp.Value) resp.Value {
	mutexLock := &sync.RWMutex{}

	key := args[0].Bulk

	mutexLock.Lock()
	result, ok := d.set[key]
	if !ok {
		return resp.Value{Typ: "null"}
	}
	mutexLock.Unlock()

	return resp.Value{Typ: "string", Str: result}
}

func hset(args []resp.Value) resp.Value {
	mutexLock := &sync.RWMutex{}

	argsLength := len(args)
	if argsLength%2 == 0 {
		return resp.Value{Typ: "string", Str: "Error: Number of arguments is invalid"}
	}

	key := args[0].Bulk

	mutexLock.Lock()
	for i := 1; i < argsLength; i += 2 {
		mapKey := args[i].Bulk
		mapValue := args[i+1].Bulk

		if _, ok := d.hashMap[key]; !ok {
			d.hashMap[key] = make(map[string]string)
		}

		d.hashMap[key][mapKey] = mapValue
	}
	mutexLock.Unlock()

	return resp.Value{Typ: "string", Str: "OK"}
}

func hget(args []resp.Value) resp.Value {
	mutexLock := &sync.RWMutex{}

	argsLength := len(args)
	if argsLength != 2 {
		return resp.Value{Typ: "string", Str: "Error: Number of arguments is invalid"}
	}

	key := args[0].Bulk
	field := args[1].Bulk

	if _, ok := d.hashMap[key]; !ok {
		return resp.Value{Typ: "null"}
	}

	mutexLock.Lock()
	result := d.hashMap[key][field]
	mutexLock.Unlock()

	return resp.Value{Typ: "string", Str: result}
}

func hgetAll(args []resp.Value) resp.Value {
	mutexLock := &sync.RWMutex{}

	argsLength := len(args)
	if argsLength > 1 {
		return resp.Value{Typ: "string", Str: "Error: Number of arguments is invalid"}
	}

	key := args[0].Bulk

	if _, ok := d.hashMap[key]; !ok {
		return resp.Value{Typ: "null"}
	}

	mutexLock.Lock()
	var result []resp.Value
	for key, value := range d.hashMap[key] {
		result = append(result, resp.Value{Typ: "string", Str: key}, resp.Value{Typ: "string", Str: value})
	}
	mutexLock.Unlock()

	return resp.Value{Typ: "array", Array: result}
}
