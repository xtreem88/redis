package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/communicate"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type PingCommand struct{}

func (c *PingCommand) Execute(conn net.Conn, args []string) error {
	return communicate.SendResponse(conn, communicate.EncodeSimpleString("PONG"))
}

type EchoCommand struct{}

func (c *EchoCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'echo' command")
	}
	return communicate.SendResponse(conn, communicate.EncodeSimpleString(args[0]))
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

	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		ms, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return fmt.Errorf("ERR invalid expire time in 'set' command")
		}
		t := time.Now().Add(time.Duration(ms) * time.Millisecond)
		expiry = &t
	}

	c.rdb.Set(key, value, expiry)

	return communicate.SendResponse(conn, communicate.EncodeSimpleString("OK"))
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
		return communicate.SendResponse(conn, "$-1\r\n")
	}

	return communicate.SendResponse(conn, communicate.EncodeSimpleString(value))
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
	fmt.Println("====Received REPLCONF command", subcommand)
	switch subcommand {
	case "getack":
		// This is a command from the master, we need to respond
		offset := 0 // Hardcoded to 0 for now
		response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n%d\r\n", offset)
		_, err := conn.Write([]byte(response))
		return err

	default:
		fmt.Printf("unknown REPLCONF subcommand '%s'", subcommand)
		return communicate.SendResponse(conn, communicate.EncodeSimpleString("OK"))
	}
}
