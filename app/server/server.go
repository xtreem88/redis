package server

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type Server struct {
	listener         net.Listener
	Port             int
	Addr             string
	RDB              *persistence.RDB
	Config           *config.Config
	role             string
	masterHost       string
	masterPort       int
	masterReplID     string
	masterReplOffset int64
	masterConn       net.Conn
	handler          *handler.Handler
	replicas         []net.Conn
	replicasMu       sync.RWMutex
	isReplica        bool
}

func New(addr string, port int, dir, dbfilename string, replicaof string) (*Server, error) {
	cfg := config.New(dir, dbfilename)
	rdb, err := persistence.LoadRDB(dir, dbfilename)
	if err != nil {
		return nil, fmt.Errorf("failed to load RDB: %w", err)
	}

	s := &Server{
		Port:             port,
		Addr:             addr,
		Config:           cfg,
		RDB:              rdb,
		role:             "master",
		masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		masterReplOffset: 0,
		isReplica:        replicaof != "",
	}

	if s.isReplica {
		parts := strings.Split(replicaof, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid replicaof format")
		}
		s.role = "slave"
		s.masterHost = parts[0]
		fmt.Sscan(parts[1], &s.masterPort)
	}

	s.handler = handler.NewHandler(cfg, rdb, s)

	return s, nil
}

func (s *Server) GetRole() string {
	return s.role
}

func (s *Server) GetMasterHost() string {
	return s.masterHost
}

func (s *Server) GetMasterPort() int {
	return s.masterPort
}

func (s *Server) GetMasterReplID() string {
	return s.masterReplID
}

func (s *Server) GetMasterReplOffset() int64 {
	return s.masterReplOffset
}

func (s *Server) IsReplica() bool {
	return s.role == "slave"
}

func (s *Server) GetMasterConn() net.Conn {
	return s.masterConn
}

func (s *Server) SendEmptyRDBFile(conn net.Conn) error {
	// Empty RDB file in base64
	emptyRDBBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	// Decode base64 to binary
	emptyRDB, err := base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		return fmt.Errorf("failed to decode empty RDB file: %w", err)
	}

	// Send the length of the RDB file
	length := len(emptyRDB)
	if _, err := fmt.Fprintf(conn, "$%d\r\n", length); err != nil {
		return fmt.Errorf("failed to send RDB file length: %w", err)
	}

	// Send the RDB file contents
	if _, err := conn.Write(emptyRDB); err != nil {
		return fmt.Errorf("failed to send RDB file contents: %w", err)
	}

	return nil
}

func (s *Server) AddReplica(conn net.Conn) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	s.replicas = append(s.replicas, conn)
	fmt.Printf("Added new replica: %s\n", conn.RemoteAddr())
}

func (s *Server) RemoveReplica(conn net.Conn) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	for i, replica := range s.replicas {
		if replica == conn {
			s.replicas = append(s.replicas[:i], s.replicas[i+1:]...)
			fmt.Printf("Removed replica: %s\n", conn.RemoteAddr())
			break
		}
	}
}

func (s *Server) PropagateCommand(args []string) {
	command := encodeRESPArray(args)
	s.replicasMu.RLock()
	defer s.replicasMu.RUnlock()
	for _, replica := range s.replicas {
		_, err := replica.Write([]byte(command))
		if err != nil {
			fmt.Printf("Error propagating command to replica %s: %v\n", replica.RemoteAddr(), err)
			s.RemoveReplica(replica)
		} else {
			fmt.Printf("Propagated command to replica %s: %s", replica.RemoteAddr(), command)
		}
	}
}

func encodeRESPArray(args []string) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return builder.String()
}

func (s *Server) Listen() error {
	addr := fmt.Sprintf("%s:%d", s.Addr, s.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %d: %w", s.Port, err)
	}
	defer l.Close()
	s.listener = l

	fmt.Printf("Server listening on %s\n", addr)

	if s.role == "slave" {
		go s.connectToMaster()
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		s.masterConn = conn
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("New connection from %s\n", conn.RemoteAddr())

	if s.role == "master" {
		s.handleMasterConnection(conn)
	} else {
		s.handleSlaveConnection(conn)
	}
}

func (s *Server) handleMasterConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		commandType, err := reader.ReadByte()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("error reading command type:", err)
			return
		}

		if commandType != '*' {
			reader.UnreadByte()
		}

		commandArgs, err := parser.ParseArray(reader)
		if err != nil {
			fmt.Println("error parsing array:", err)
			return
		}

		// Handle regular client command
		if err := s.handler.Handle(conn, commandArgs); err != nil {
			fmt.Printf("Error handling command: %v\n", err)
			return
		}
	}
}

func (s *Server) handleSlaveConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		commandType, err := reader.ReadByte()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("error reading command type:", err)
			return
		}

		if commandType != '*' {
			reader.UnreadByte()
		}

		commandArgs, err := parser.ParseArray(reader)
		if err != nil {
			fmt.Println("error parsing array:", err)
			return
		}

		// Process command from master without sending a response
		if err := s.handler.HandleReplicaCommand(conn, commandArgs); err != nil {
			fmt.Printf("Error handling replica command: %v\n", err)
		}
	}
}
