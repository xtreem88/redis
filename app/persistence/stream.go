package persistence

import (
	"errors"
	"fmt"
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

	if len(stream.Entries) > 0 {
		lastEntry := stream.Entries[len(stream.Entries)-1]
		if milliseconds < lastEntry.Milliseconds ||
			(milliseconds == lastEntry.Milliseconds && sequence <= lastEntry.Sequence) {
			return "", errors.New(InvalidStreamError)
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
