package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type ServerInfo interface {
	GetRole() string
	GetMasterHost() string
	GetMasterPort() int
	GetMasterReplID() string
	GetMasterReplOffset() int64
}

type Command interface {
	Execute(conn net.Conn, args []string) error
}

type Handler struct {
	cfg    *config.Config
	rdb    *persistence.RDB
	server ServerInfo
}

func NewHandler(cfg *config.Config, rdb *persistence.RDB, server ServerInfo) *Handler {
	return &Handler{
		cfg:    cfg,
		rdb:    rdb,
		server: server,
	}
}

func (h *Handler) Handle(conn net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ERR no command provided")
	}

	cmdName := strings.ToUpper(args[0])
	cmd := h.getCommand(cmdName)
	if cmd == nil {
		return fmt.Errorf("ERR unknown command '%s'", cmdName)
	}

	return cmd.Execute(conn, args[1:])
}

func (h *Handler) getCommand(name string) Command {
	switch name {
	case "PING":
		return &PingCommand{}
	case "ECHO":
		return &EchoCommand{}
	case "SET":
		return &SetCommand{rdb: h.rdb}
	case "GET":
		return &GetCommand{rdb: h.rdb}
	case "CONFIG":
		return &ConfigCommand{cfg: h.cfg}
	case "KEYS":
		return &KeysCommand{rdb: h.rdb}
	case "INFO":
		return &InfoCommand{server: h.server}
	default:
		return nil
	}
}

type PingCommand struct{}

func (c *PingCommand) Execute(conn net.Conn, args []string) error {
	_, err := fmt.Fprint(conn, encodeSimpleString("PONG"))
	return err
}

type EchoCommand struct{}

func (c *EchoCommand) Execute(conn net.Conn, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'echo' command")
	}
	_, err := fmt.Fprint(conn, encodeSimpleString(args[0]))
	return err
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

	_, err := fmt.Fprint(conn, "+OK\r\n")
	return err
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
		_, err := fmt.Fprint(conn, "$-1\r\n")
		return err
	}

	response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	_, err := fmt.Fprint(conn, response)
	return err
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
	_, err := fmt.Fprint(conn, response)
	return err
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

	_, err := fmt.Fprint(conn, response)
	return err
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

	_, err := fmt.Fprint(conn, encodedResponse)
	return err
}

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
