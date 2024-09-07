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
	UpdateMasterReplOffset() error
	SendEmptyRDBFile(conn net.Conn) error
	PropagateCommand(args []string)
	AddReplica(conn net.Conn)
	RemoveReplica(conn net.Conn)
	GetMasterConn() net.Conn
	SendCommand(conn net.Conn, args ...string) error
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

	fmt.Printf("Replica executing: %s %v\n", cmdName, args[1:])

	err := cmd.Execute(conn, args[1:])
	if err == nil && h.server.GetRole() == "master" && h.IsWriteCommand(cmdName) {
		h.server.PropagateCommand(args)
	}

	return err
}

func (h *Handler) HandleReplicaCommand(conn net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ERR no command provided")
	}

	cmdName := strings.ToUpper(args[0])
	cmd := h.getCommand(cmdName)
	if cmd == nil {
		return fmt.Errorf("ERR unknown command '%s'", cmdName)
	}

	var c net.Conn
	if h.CanRespondToCommand(cmdName) {
		c = conn
	}
	// Execute all commands without sending a response
	return cmd.Execute(c, args[1:])
}

func (h *Handler) IsWriteCommand(command string) bool {
	writeCommands := map[string]bool{
		"SET": true,
		"DEL": true,
	}
	return writeCommands[strings.ToUpper(command)]
}

func (h *Handler) CanRespondToCommand(command string) bool {
	cmds := map[string]bool{
		"GET":      true,
		"REPLCONF": true,
		"PSYNC":    true,
	}
	return cmds[strings.ToUpper(command)]
}

func (h *Handler) getCommand(name string) Command {
	switch name {
	case "REPLCONF":
		return &ReplconfCommand{server: h.server}
	case "PSYNC":
		return &PsyncCommand{server: h.server}
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
	case "WAIT":
		return &WaitCommand{}
	default:
		return nil
	}
}
