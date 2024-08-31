package persistence

import (
	"fmt"
	"time"
)

type RedisDB struct {
	data map[string]DataObject
}

type DataObject struct {
	value     string
	createdAt int64
	expiry    int64
}

func (rdb *RedisDB) SetValue(key string, value string, expiry int64) {
	rdb.data[key] = DataObject{
		value:     value,
		createdAt: time.Now().UnixMilli(),
		expiry:    expiry,
	}
}

func (rdb *RedisDB) GetValue(key string) (string, bool) {
	val, ok := rdb.data[key]
	if !ok {
		fmt.Println("Key not found: ", key)
		return "$-1\r\n", false
	}
	timeElapsed := time.Now().UnixMilli() - val.createdAt
	if val.expiry > 0 && timeElapsed > val.expiry {
		fmt.Println("Key expired: ", key)
		delete(rdb.data, key)
		return "$-1\r\n", false
	}
	return val.value, true
}
