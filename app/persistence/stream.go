package persistence

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type StreamEntry struct {
	Milliseconds int64
	Sequence     int64
	Fields       map[string]string
}

type Stream struct {
	Entries []StreamEntry
}

const InvalidStreamError = "The ID specified in XADD is equal or smaller than the target stream top item"

func (rdb *RDB) XAdd(key string, milliseconds, sequence int64, fields map[string]string) (string, error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	stream, ok := rdb.data[key].(*Stream)
	if !ok {
		stream = &Stream{}
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

func parseID(id string) (int64, int64) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0
	}
	time, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	return time, seq
}
