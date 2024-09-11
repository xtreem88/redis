package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/communicate"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

var numAcksSinceLasSet = 0
var ackLock = sync.Mutex{}

var replicasLock = sync.Mutex{}

var setHasOccurred = false

type PingCommand struct{}

func (c *PingCommand) Execute(conn net.Conn, args []string) error {
	response := "+PONG\r\n"
	if conn != nil {
		return communicate.SendResponse(conn, response)
	}
	return nil
}

type EchoCommand struct{}

func (c *EchoCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'echo' command")
	}
	if conn != nil {
		return communicate.SendResponse(conn, communicate.EncodeBulkString(args[0]))
	}
	return nil
}

type SetCommand struct {
	rdb *persistence.RDB
}

func (c *SetCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 2 || len(args) > 4 {
		return fmt.Errorf("ERR wrong number of arguments for 'set' command")
	}

	key := args[0]
	value := args[1]
	var expiry *time.Time

	setHasOccurred = true

	ackLock.Lock()
	numAcksSinceLasSet = 0
	ackLock.Unlock()

	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		ms, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return fmt.Errorf("ERR invalid expire time in 'set' command")
		}
		t := time.Now().Add(time.Duration(ms) * time.Millisecond)
		expiry = &t
	}

	c.rdb.Set(key, value, expiry)

	if conn != nil {
		return communicate.SendResponse(conn, "+OK\r\n")
	}
	return nil
}

type GetCommand struct {
	rdb *persistence.RDB
}

func (c *GetCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'get' command")
	}

	key := args[0]
	value, ok := c.rdb.Get(key)
	if !ok {
		if conn != nil {
			return communicate.SendResponse(conn, "$-1\r\n")
		}
		return nil
	}

	if conn != nil {
		switch v := value.(type) {
		case string:
			return communicate.SendResponse(conn, communicate.EncodeBulkString(v))
		case *persistence.Stream:
			return fmt.Errorf("ERR GET command cannot be used with stream values")
		default:
			return communicate.SendResponse(conn, communicate.EncodeBulkString(fmt.Sprintf("%v", v)))
		}
	}
	return nil
}

type ConfigCommand struct {
	cfg *config.Config
}

func (c *ConfigCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'config' command")
	}

	subcommand := strings.ToUpper(args[0])
	if subcommand != "GET" {
		return fmt.Errorf("ERR unsupported CONFIG subcommand: %s", subcommand)
	}

	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'config get' command")
	}

	param := args[1]
	value := c.cfg.Get(param)

	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(param), param, len(value), value)
	return communicate.SendResponse(conn, response)
}

type KeysCommand struct {
	rdb *persistence.RDB
}

func (c *KeysCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'keys' command")
	}

	pattern := args[0]
	if pattern != "*" {
		return fmt.Errorf("ERR only '*' pattern is supported")
	}

	keys := c.rdb.GetKeys()
	response := fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}

	return communicate.SendResponse(conn, response)
}

type InfoCommand struct {
	server ServerInfo
}

func (c *InfoCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 1 || strings.ToLower(args[0]) != "replication" {
		return fmt.Errorf("ERR wrong number of arguments for 'info' command")
	}

	response := fmt.Sprintf("role:%s\r\n", c.server.GetRole())
	response += fmt.Sprintf("master_replid:%s\r\n", c.server.GetMasterReplID())
	response += fmt.Sprintf("master_repl_offset:%d\r\n", c.server.GetMasterReplOffset())

	if c.server.GetRole() == "slave" {
		response += fmt.Sprintf("master_host:%s\r\n", c.server.GetMasterHost())
		response += fmt.Sprintf("master_port:%d\r\n", c.server.GetMasterPort())
	}

	encodedResponse := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)

	return communicate.SendResponse(conn, encodedResponse)
}

type PsyncCommand struct {
	server ServerInfo
}

func (c *PsyncCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'psync' command")
	}

	replID := c.server.GetMasterReplID()
	offset := c.server.GetMasterReplOffset()
	response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", replID, offset)
	if err := communicate.SendResponse(conn, response); err != nil {
		return fmt.Errorf("failed to send FULLRESYNC response: %w", err)
	}

	// Trigger sending of RDB file
	if err := c.server.SendEmptyRDBFile(conn); err != nil {
		return fmt.Errorf("failed to send empty RDB file: %w", err)
	}

	c.server.AddReplica(conn)

	return nil
}

type ReplconfCommand struct {
	server ServerInfo
}

func (c *ReplconfCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'replconf' command")
	}

	subcommand := strings.ToLower(args[0])
	switch subcommand {
	case "getack":
		offset := c.server.GetMasterReplOffset()
		response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.FormatInt(offset, 10)), offset)
		fmt.Printf("Sending ACK response to replica %v with offset %d\n", conn.RemoteAddr(), offset)
		_, err := conn.Write([]byte(response))
		return err
	case "ack":
		if len(args) != 2 {
			return fmt.Errorf("ERR wrong number of arguments for 'replconf ack' command")
		}
		offset, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("ERR invalid offset for 'replconf ack' command")
		}
		fmt.Printf("Received ACK from replica %v with offset %d\n", conn.RemoteAddr(), offset)
		ackLock.Lock()
		numAcksSinceLasSet++
		ackLock.Unlock()

		return nil
	default:
		fmt.Printf("Unknown REPLCONF subcommand: %s\n", subcommand)
		return communicate.SendResponse(conn, "+OK\r\n")
	}
}

type WaitCommand struct {
	server ServerInfo
}

func (c *WaitCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("error performing wait: not enough args")
	}
	replicas := c.server.GetReplicas()

	if !setHasOccurred {
		fmt.Println("Set has not occurred, sending 0")
		replicasLock.Lock()
		defer replicasLock.Unlock()

		numAcks := len(replicas)

		response := fmt.Sprintf(":%d\r\n", numAcks)
		if _, err := conn.Write([]byte(response)); err != nil {
			return fmt.Errorf("error performing wait: %w", err)
		}

		return nil
	}

	replicasLock.Lock()
	getAckCmd := []byte(communicate.EncodeStringArray([]string{"REPLCONF", "GETACK", "*"}))
	for cn := range replicas {
		if _, err := cn.Write(getAckCmd); err != nil {
			fmt.Println("Failed to getack after relaying command to replica", err.Error())
		}
	}
	replicasLock.Unlock()

	requiredAcks, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("error performing wait: %w", err)
	}
	timeoutMS, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("error performing wait: %w", err)
	}

	timeout := time.Duration(timeoutMS) * time.Millisecond

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeoutChannel := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			if numAcksSinceLasSet >= requiredAcks {
				response := fmt.Sprintf(":%d\r\n", numAcksSinceLasSet)
				if _, err := conn.Write([]byte(response)); err != nil {
					return fmt.Errorf("error performing wait: %w", err)
				}
				return nil
			}
		case <-timeoutChannel:
			response := fmt.Sprintf(":%d\r\n", numAcksSinceLasSet)
			if _, err := conn.Write([]byte(response)); err != nil {
				return fmt.Errorf("error performing wait: %w", err)
			}
			return nil
		}
	}
}

type TypeCommand struct {
	rdb *persistence.RDB
}

func (c *TypeCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'type' command")
	}

	key := args[0]
	valueType := c.rdb.GetType(key)

	response := fmt.Sprintf("+%s\r\n", valueType)
	return communicate.SendResponse(conn, response)
}

type XaddCommand struct {
	rdb *persistence.RDB
}

func (c *XaddCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 3 || len(args)%2 != 0 {
		return communicate.SendResponse(conn, "-ERR wrong number of arguments for 'xadd' command\r\n")
	}

	key := args[0]
	id := args[1]

	var milliseconds, sequence int64
	var err error

	if id == "*" {
		milliseconds = -1
		sequence = -1
	} else {
		// Validate ID format
		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return communicate.SendResponse(conn, "-ERR Invalid stream ID format\r\n")
		}

		milliseconds, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return communicate.SendResponse(conn, "-ERR Invalid stream ID format\r\n")
		}

		if parts[1] == "*" {
			sequence = -1
		} else {
			sequence, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return communicate.SendResponse(conn, "-ERR Invalid stream ID format\r\n")
			}
		}
	}

	// Check if ID is greater than 0-0
	if milliseconds == 0 && sequence == 0 {
		return communicate.SendResponse(conn, "-ERR The ID specified in XADD must be greater than 0-0\r\n")
	}

	fields := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		fields[args[i]] = args[i+1]
	}

	resultID, err := c.rdb.XAdd(key, milliseconds, sequence, fields)
	if err != nil {
		if err.Error() == persistence.InvalidStreamError {
			return communicate.SendResponse(conn, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
		}
		return communicate.SendResponse(conn, fmt.Sprintf("-ERR %s\r\n", err.Error()))
	}

	response := fmt.Sprintf("$%d\r\n%s\r\n", len(resultID), resultID)
	return communicate.SendResponse(conn, response)
}

type XRangeCommand struct {
	rdb *persistence.RDB
}

func (c *XRangeCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 3 {
		return communicate.SendResponse(conn, "-ERR wrong number of arguments for 'xrange' command\r\n")
	}

	key := args[0]
	start := args[1]
	end := args[2]

	entries, err := c.rdb.XRange(key, start, end)
	if err != nil {
		return communicate.SendResponse(conn, fmt.Sprintf("-ERR %s\r\n", err.Error()))
	}

	response := encodeXRangeResponse(entries)
	return communicate.SendResponse(conn, response)
}

func encodeXRangeResponse(entries []persistence.StreamEntry) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))

	for _, entry := range entries {
		builder.WriteString("*2\r\n")

		// Encode ID
		id := fmt.Sprintf("%d-%d", entry.Milliseconds, entry.Sequence)
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id))

		// Encode fields
		builder.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
		for k, v := range entry.Fields {
			builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
			builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
		}
	}

	return builder.String()
}

type XReadCommand struct {
	rdb *persistence.RDB
}

func (c *XReadCommand) Execute(conn net.Conn, args []string) error {
	var blockDuration *time.Duration
	streamsIndex := -1

	for i, arg := range args {
		if strings.ToUpper(arg) == "BLOCK" {
			if i+1 >= len(args) {
				return communicate.SendResponse(conn, "-ERR BLOCK option requires a timeout\r\n")
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return communicate.SendResponse(conn, "-ERR BLOCK timeout is not an integer\r\n")
			}
			duration := time.Duration(ms) * time.Millisecond
			blockDuration = &duration
		} else if strings.ToUpper(arg) == "STREAMS" {
			streamsIndex = i
			break
		}
	}

	if streamsIndex == -1 || streamsIndex+1 >= len(args) {
		return communicate.SendResponse(conn, "-ERR wrong number of arguments for 'xread' command\r\n")
	}

	streamCount := (len(args) - streamsIndex - 1) / 2
	streams := args[streamsIndex+1 : streamsIndex+1+streamCount]
	ids := args[streamsIndex+1+streamCount:]

	if len(streams) != len(ids) {
		return communicate.SendResponse(conn, "-ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified.\r\n")
	}

	results, err := c.rdb.XRead(streams, ids, blockDuration)
	if err != nil {
		return communicate.SendResponse(conn, fmt.Sprintf("-ERR %s\r\n", err.Error()))
	}

	if results == nil {
		return communicate.SendResponse(conn, "$-1\r\n")
	}

	response := encodeXReadResponse(results)
	return communicate.SendResponse(conn, response)
}

func encodeXReadResponse(results []persistence.StreamResult) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(results)))

	for _, result := range results {
		builder.WriteString("*2\r\n")
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(result.Key), result.Key))
		builder.WriteString(fmt.Sprintf("*%d\r\n", len(result.Entries)))

		for _, entry := range result.Entries {
			builder.WriteString("*2\r\n")
			id := fmt.Sprintf("%d-%d", entry.Milliseconds, entry.Sequence)
			builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id))

			builder.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
			for k, v := range entry.Fields {
				builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
				builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
			}
		}
	}

	return builder.String()
}

type IncrCommand struct {
	rdb *persistence.RDB
}

func (c *IncrCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return communicate.SendResponse(conn, "-ERR wrong number of arguments for 'incr' command\r\n")
	}

	key := args[0]
	value, err := c.rdb.Incr(key)
	if err != nil {
		if err == persistence.ErrNotInteger {
			return communicate.SendResponse(conn, "-ERR value is not an integer or out of range\r\n")
		}
		return communicate.SendResponse(conn, fmt.Sprintf("-ERR %s\r\n", err.Error()))
	}

	return communicate.SendResponse(conn, fmt.Sprintf(":%d\r\n", value))
}

type MultiCommand struct {
	handler *Handler
}

func (c *MultiCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 0 {
		return communicate.SendResponse(conn, "-ERR wrong number of arguments for 'multi' command\r\n")
	}

	c.handler.inTransaction = true
	return communicate.SendResponse(conn, "+OK\r\n")
}

type ExecCommand struct {
	handler *Handler
}

func (c *ExecCommand) Execute(conn net.Conn, args []string) error {
	if len(args) != 0 {
		return communicate.SendResponse(conn, "-ERR wrong number of arguments for 'exec' command\r\n")
	}

	if !c.handler.inTransaction {
		return communicate.SendResponse(conn, "-ERR EXEC without MULTI\r\n")
	}

	c.handler.inTransaction = false

	return communicate.SendResponse(conn, "*0\r\n")
}
