package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RDB struct {
	version string
	index   byte
	data    map[string]interface{}
	expires map[string]time.Time
	mu      sync.RWMutex
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type Stream struct {
	Entries []StreamEntry
}

func LoadRDB(dir, filename string) (*RDB, error) {
	path := filepath.Join(dir, filename)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &RDB{data: make(map[string]interface{}), expires: make(map[string]time.Time)}, nil
		}
		return nil, err
	}
	defer file.Close()

	rdb := &RDB{
		data:    make(map[string]interface{}),
		expires: make(map[string]time.Time),
	}
	if err := rdb.parse(file); err != nil {
		return nil, err
	}
	return rdb, nil
}

func (rdb *RDB) XAdd(key string, id string, fields map[string]string) string {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	stream, ok := rdb.data[key].(*Stream)
	if !ok {
		stream = &Stream{}
		rdb.data[key] = stream
	}

	entry := StreamEntry{
		ID:     id,
		Fields: fields,
	}

	stream.Entries = append(stream.Entries, entry)
	return id
}

func (rdb *RDB) GetType(key string) string {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	value, exists := rdb.data[key]
	if !exists {
		return "none"
	}

	switch value.(type) {
	case string:
		return "string"
	case *Stream:
		return "stream"
	default:
		return "none"
	}
}

func (rdb *RDB) parse(f *os.File) error {
	reader := bufio.NewReader(f)

	// Read and validate header
	header := make([]byte, 9)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	if string(header[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB file format")
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

	// Read key-value pair
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if b == 0xFF {
			break // End of RDB file
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
		value, err = readLengthPrefixedString(reader)
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

func readLengthPrefixedString(r *bufio.Reader) (string, error) {
	length, err := readLength(r)
	if err != nil {
		return "", err
	}

	str := make([]byte, length)
	if _, err := io.ReadFull(r, str); err != nil {
		return "", err
	}

	return string(str), nil
}

func readLength(r *bufio.Reader) (int, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	// Handle length-prefixed strings only
	if b>>6 == 0 {
		return int(b & 0x3F), nil
	} else if b>>6 == 1 {
		next, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return (int(b&0x3F) << 8) | int(next), nil
	}

	return 0, fmt.Errorf("unsupported length encoding")
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

func (rdb *RDB) Get(key string) (interface{}, bool) {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()
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

func (rdb *RDB) Set(key, value string, expiry *time.Time) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	rdb.data[key] = value
	if expiry != nil {
		rdb.expires[key] = *expiry
	} else {
		delete(rdb.expires, key)
	}
}

func (rdb *RDB) GetKeys() []string {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()
	keys := make([]string, 0, len(rdb.data))
	for k := range rdb.data {
		if expiry, ok := rdb.expires[k]; !ok || time.Now().Before(expiry) {
			keys = append(keys, k)
		}
	}
	return keys
}
