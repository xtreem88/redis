package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type RedisDB struct {
	version string
	index   byte
	data    map[string]string
	expires map[string]time.Time
}

func LoadRDB(dir, filename string) (*RedisDB, error) {
	path := filepath.Join(dir, filename)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &RedisDB{data: make(map[string]string), expires: make(map[string]time.Time)}, nil
		}
		return nil, err
	}
	defer file.Close()

	rdb := &RedisDB{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
	}
	if err := rdb.parse(file); err != nil {
		return nil, err
	}
	return rdb, nil
}

func (rdb *RedisDB) parse(f *os.File) error {
	reader := bufio.NewReader(f)

	// Read and validate header
	header := make([]byte, 9)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	if string(header[:5]) != "REDIS" {
		return fmt.Errorf("invalid RedisDB file format")
	}
	rdb.version = string(header[5:])

	// Skip to database selector
	if _, err := reader.ReadBytes(0xFE); err != nil {
		return err
	}

	// Read database index
	if b, err := reader.ReadByte(); err != nil {
		return err
	} else {
		rdb.index = b
	}

	// Skip hash table size information
	if _, err := reader.ReadBytes(0xFB); err != nil {
		return err
	}
	if _, err := parseSize(reader); err != nil {
		return err
	}
	if _, err := parseSize(reader); err != nil {
		return err
	}

	// Read key-value pairs
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return err
		}
		if b == 0xFF {
			break // End of RedisDB file
		}

		var key, value string
		var expiry time.Time

		if b == 0xFC {
			// Read expiry in milliseconds
			milli, err := readUint64(reader)
			if err != nil {
				return err
			}
			expiry = time.UnixMilli(int64(milli))
			b, err = reader.ReadByte()
			if err != nil {
				return err
			}
		}

		if b != 0 {
			return fmt.Errorf("unsupported value type: %d", b)
		}

		key, err = readString(reader)
		if err != nil {
			return err
		}
		value, err = readString(reader)
		if err != nil {
			return err
		}

		rdb.data[key] = value
		if !expiry.IsZero() {
			rdb.expires[key] = expiry
		}
	}

	return nil
}

func parseSize(r *bufio.Reader) (int, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch b >> 6 {
	case 0:
		return int(b & 0x3F), nil
	case 1:
		next, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return (int(b&0x3F) << 8) | int(next), nil
	case 2:
		next := make([]byte, 4)
		if _, err := io.ReadFull(r, next); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(next)), nil
	default:
		return 0, fmt.Errorf("invalid length encoding")
	}
}

func readString(r *bufio.Reader) (string, error) {
	length, err := parseSize(r)
	if err != nil {
		return "", err
	}

	str := make([]byte, length)
	if _, err := io.ReadFull(r, str); err != nil {
		return "", err
	}

	return string(str), nil
}

func readUint64(r *bufio.Reader) (uint64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func (rdb *RedisDB) Get(key string) (string, bool) {
	value, ok := rdb.data[key]
	if !ok {
		return "", false
	}

	if expiry, ok := rdb.expires[key]; ok && time.Now().After(expiry) {
		delete(rdb.data, key)
		delete(rdb.expires, key)
		return "", false
	}

	return value, true
}

func (rdb *RedisDB) GetKeys() []string {
	keys := make([]string, 0, len(rdb.data))
	for k := range rdb.data {
		if expiry, ok := rdb.expires[k]; !ok || time.Now().Before(expiry) {
			keys = append(keys, k)
		}
	}
	return keys
}
