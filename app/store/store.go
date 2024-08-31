package store

import (
	"sync"
)

var (
	data = make(map[string]string)
	mu   sync.RWMutex
)

func Set(key, value string) {
	mu.Lock()
	defer mu.Unlock()
	data[key] = value
}

func Get(key string) (string, bool) {
	mu.RLock()
	defer mu.RUnlock()
	value, ok := data[key]
	return value, ok
}
