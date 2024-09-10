package persistence

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StreamEntry struct {
	Milliseconds int64
	Sequence     int64
	Fields       map[string]string
}

type Stream struct {
	Entries []StreamEntry
	Cond    *sync.Cond
}

type StreamResult struct {
	Key     string
	Entries []StreamEntry
}

const InvalidStreamError = "The ID specified in XADD is equal or smaller than the target stream top item"

func (rdb *RDB) XAdd(key string, milliseconds, sequence int64, fields map[string]string) (string, error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	stream, ok := rdb.data[key].(*Stream)
	if !ok {
		stream = &Stream{Cond: sync.NewCond(&sync.Mutex{})}
		rdb.data[key] = stream
	}

	if milliseconds == -1 {
		milliseconds = time.Now().UnixMilli()
	}

	if sequence == -1 {
		// Auto-generate sequence number
		if len(stream.Entries) > 0 {
			lastEntry := stream.Entries[len(stream.Entries)-1]
			if milliseconds == lastEntry.Milliseconds {
				sequence = lastEntry.Sequence + 1
			} else if milliseconds > lastEntry.Milliseconds {
				sequence = 0
			} else {
				return "", errors.New(InvalidStreamError)
			}
		} else {
			// If the stream is empty
			if milliseconds == 0 {
				sequence = 1
			} else {
				sequence = 0
			}
		}
	} else {
		// Explicit sequence number
		if len(stream.Entries) > 0 {
			lastEntry := stream.Entries[len(stream.Entries)-1]
			if milliseconds < lastEntry.Milliseconds ||
				(milliseconds == lastEntry.Milliseconds && sequence <= lastEntry.Sequence) {
				return "", errors.New(InvalidStreamError)
			}
		}
	}

	entry := StreamEntry{
		Milliseconds: milliseconds,
		Sequence:     sequence,
		Fields:       fields,
	}

	stream.Entries = append(stream.Entries, entry)
	stream.Cond.Broadcast()
	return fmt.Sprintf("%d-%d", milliseconds, sequence), nil
}

func (rdb *RDB) XRange(key, start, end string) ([]StreamEntry, error) {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	stream, ok := rdb.data[key].(*Stream)
	if !ok {
		return nil, fmt.Errorf("key does not exist")
	}

	startTime, startSeq := parseID(start)
	var endTime, endSeq int64
	if end == "+" {
		endTime, endSeq = math.MaxInt64, math.MaxInt64
	} else {
		endTime, endSeq = parseID(end)
	}

	var result []StreamEntry
	for _, entry := range stream.Entries {

		if (entry.Milliseconds >= startTime && entry.Sequence >= startSeq) &&
			(entry.Milliseconds <= endTime && entry.Sequence <= endSeq) {
			result = append(result, entry)
		}
	}

	return result, nil
}

func (rdb *RDB) XRead(keys []string, ids []string, block *time.Duration) ([]StreamResult, error) {
	var endTime time.Time
	indefiniteBlock := block != nil && *block == 0

	if block != nil && !indefiniteBlock {
		endTime = time.Now().Add(*block)
	}

	for {
		rdb.mu.RLock()
		results := make([]StreamResult, 0, len(keys))

		for i, key := range keys {
			stream, ok := rdb.data[key].(*Stream)
			if !ok {
				stream = &Stream{Cond: sync.NewCond(&sync.Mutex{})}
				rdb.data[key] = stream
			}

			startTime, startSeq := parseID(ids[i])
			entries := getEntriesAfterID(stream.Entries, startTime, startSeq)

			if len(entries) > 0 {
				results = append(results, StreamResult{
					Key:     key,
					Entries: entries,
				})
			}
		}

		if len(results) > 0 {
			rdb.mu.RUnlock()
			return results, nil
		}

		if block == nil {
			rdb.mu.RUnlock()
			return nil, nil
		}

		if !indefiniteBlock && time.Now().After(endTime) {
			rdb.mu.RUnlock()
			return nil, nil
		}

		rdb.mu.RUnlock()

		// Wait for a short duration before checking again
		if indefiniteBlock {
			time.Sleep(10 * time.Millisecond)
		} else {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-time.After(time.Until(endTime)):
				return nil, nil
			}
		}
	}
}

func getEntriesAfterID(entries []StreamEntry, startTime, startSeq int64) []StreamEntry {
	var result []StreamEntry
	for _, entry := range entries {
		if entry.Milliseconds > startTime || (entry.Milliseconds == startTime && entry.Sequence > startSeq) {
			result = append(result, entry)
		}
	}
	return result
}

func parseID(id string) (int64, int64) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0
	}
	time, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	return time, seq
}
