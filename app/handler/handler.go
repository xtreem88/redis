package handler

import (
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type ServerInfo interface {
	GetRole() string
	GetMasterHost() string
	GetMasterPort() int
	GetMasterReplID() string
	GetMasterReplOffset() int64
	SendEmptyRDBFile(conn net.Conn) error
	PropagateCommand(args []string)
	AddReplica(conn net.Conn)
	RemoveReplica(conn net.Conn)
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

	err := cmd.Execute(conn, args[1:])
	if err == nil && h.server.GetRole() == "master" && h.IsWriteCommand(cmdName) {
		h.server.PropagateCommand(args)
	}

	return err
}

func (h *Handler) HandleReplicaCommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ERR no command provided")
	}

	cmdName := strings.ToUpper(args[0])
	cmd := h.getCommand(cmdName)
	if cmd == nil {
		return fmt.Errorf("ERR unknown command '%s'", cmdName)
	}

	if h.IsWriteCommand(cmdName) {
		fmt.Println("=============", args)
		return cmd.Execute(nil, args[1:])
	}

	return nil
}

func (h *Handler) IsWriteCommand(command string) bool {
	writeCommands := map[string]bool{
		"SET": true,
		"DEL": true,
	}
	return writeCommands[strings.ToUpper(command)]
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
	case "REPLCONF":
		return &ReplconfCommand{}
	case "PSYNC":
		return &PsyncCommand{server: h.server}
	default:
		return nil
	}
}

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
