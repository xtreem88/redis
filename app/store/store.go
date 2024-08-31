package store

import (
	"sync"
	"time"
)

type item struct {
	value      string
	expiration time.Time
}

var (
	data = make(map[string]item)
	mu   sync.RWMutex
)

func SetWithExpiry(key, value string, expiry time.Duration) {
	mu.Lock()
	defer mu.Unlock()

	var expiration time.Time
	if expiry > 0 {
		expiration = time.Now().Add(expiry)
	}

	data[key] = item{
		value:      value,
		expiration: expiration,
	}
}

func Get(key string) (string, bool) {
	item, ok := data[key]
	if !ok {
		return "", false
	}

	if IsExpired(key) {
		go Delete(key)
		return "", false
	}

	return item.value, true
}

func Delete(key string) {
	mu.Lock()
	defer mu.Unlock()

	delete(data, key)
}

func IsExpired(key string) bool {
	item, ok := data[key]
	if !ok {
		return false
	}

	return !item.expiration.IsZero() && time.Now().After(item.expiration)
}
