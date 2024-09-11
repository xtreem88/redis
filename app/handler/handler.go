package handler

import (
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/communicate"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type Replica struct {
	Offset int64
	AckCh  chan struct{}
}
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
	GetReplicas() map[net.Conn]*Replica
}

type Command interface {
	Execute(conn net.Conn, args []string) error
}

type Handler struct {
	cfg            *config.Config
	rdb            *persistence.RDB
	server         ServerInfo
	inTransaction  bool
	queuedCommands []QueuedCommand
}

type QueuedCommand struct {
	Name string
	Args []string
}

func NewHandler(cfg *config.Config, rdb *persistence.RDB, server ServerInfo) *Handler {
	return &Handler{
		cfg:    cfg,
		rdb:    rdb,
		server: server,
	}
}

func (h *Handler) resetTransaction() {
	h.inTransaction = false
	h.queuedCommands = nil
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

	fmt.Printf("Master executing: %s %v\n", cmdName, args[1:])

	// Handle transaction commands
	switch cmdName {
	case "MULTI":
		h.inTransaction = true
		h.queuedCommands = []QueuedCommand{}
		return communicate.SendResponse(conn, "+OK\r\n")
	case "EXEC":
		if !h.inTransaction {
			return communicate.SendResponse(conn, "-ERR EXEC without MULTI\r\n")
		}
		defer h.resetTransaction()
		return h.executeTransaction(conn)
	case "DISCARD":
		if !h.inTransaction {
			return communicate.SendResponse(conn, "-ERR DISCARD without MULTI\r\n")
		}
		h.resetTransaction()
		return communicate.SendResponse(conn, "+OK\r\n")
	}

	// Execute read commands immediately, even in a transaction
	if h.isReadCommand(cmdName) {
		return cmd.Execute(conn, args[1:])
	}

	// Queue write commands if in a transaction
	if h.inTransaction {
		h.queuedCommands = append(h.queuedCommands, QueuedCommand{Name: cmdName, Args: args[1:]})
		return communicate.SendResponse(conn, "+QUEUED\r\n")
	}

	// Execute the command if not in a transaction
	err := cmd.Execute(conn, args[1:])
	if err == nil && h.server.GetRole() == "master" && h.IsWriteCommand(cmdName) {
		h.server.PropagateCommand(args)
	}

	return err
}

func (h *Handler) isReadCommand(command string) bool {
	readCommands := map[string]bool{
		"GET":   true,
		"KEYS":  true,
		"TYPE":  true,
		"INFO":  true,
		"PING":  true,
		"ECHO":  true,
		"XREAD": true,
	}
	return readCommands[strings.ToUpper(command)]
}

func (h *Handler) executeTransaction(conn net.Conn) error {
	responses := make([]string, len(h.queuedCommands))
	for i, queuedCommand := range h.queuedCommands {
		cmd := h.getCommand(queuedCommand.Name)
		if cmd == nil {
			responses[i] = fmt.Sprintf("-ERR unknown command '%s'\r\n", queuedCommand.Name)
		} else {
			responseWriter := &ResponseWriter{}
			err := cmd.Execute(responseWriter, queuedCommand.Args)
			if err != nil {
				responses[i] = fmt.Sprintf("-ERR %s\r\n", err.Error())
			} else {
				responses[i] = responseWriter.String()
			}
		}
	}

	// Send the array of responses
	response := fmt.Sprintf("*%d\r\n", len(responses))
	for _, resp := range responses {
		response += resp
	}
	return communicate.SendResponse(conn, response)
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
		return &WaitCommand{server: h.server}
	case "TYPE":
		return &TypeCommand{rdb: h.rdb}
	case "XADD":
		return &XaddCommand{rdb: h.rdb}
	case "XRANGE":
		return &XRangeCommand{rdb: h.rdb}
	case "XREAD":
		return &XReadCommand{rdb: h.rdb}
	case "INCR":
		return &IncrCommand{rdb: h.rdb}
	case "MULTI":
		return &MultiCommand{handler: h}
	case "EXEC":
		return &ExecCommand{handler: h}
	default:
		return nil
	}
}
