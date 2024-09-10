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
	replicas         map[net.Conn]*handler.Replica
	replicasMu       sync.RWMutex
	isReplica        bool
	offset           int64
	lastBytesLen     int
	offsetMu         sync.RWMutex
	ack              int
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
		offset:           0,
		isReplica:        replicaof != "",
		replicas:         make(map[net.Conn]*handler.Replica),
		ack:              0,
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
	return s.offset
}

func (s *Server) UpdateMasterReplOffset() error {
	s.offset += int64(s.lastBytesLen)

	return nil
}

func (s *Server) GetReplicas() map[net.Conn]*handler.Replica {
	return s.replicas
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
	s.replicas[conn] = &handler.Replica{Offset: 0, AckCh: make(chan struct{}, 1)}
}

func (s *Server) RemoveReplica(conn net.Conn) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	delete(s.replicas, conn)
}

func (s *Server) IncrementOffset(n int64) {
	s.offsetMu.Lock()
	defer s.offsetMu.Unlock()
	s.offset += n
}

func (s *Server) GetOffset() int64 {
	s.offsetMu.RLock()
	defer s.offsetMu.RUnlock()
	return s.offset
}

func (s *Server) PropagateCommand(args []string) {
	command := encodeRESPArray(args)
	s.replicasMu.RLock()
	defer s.replicasMu.RUnlock()
	for replicaConn := range s.replicas {
		_, err := replicaConn.Write([]byte(command))
		if err != nil {
			fmt.Printf("Error propagating command to replica: %v\n", err)
			s.RemoveReplica(replicaConn)
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
	if s.GetRole() == "slave" {
		go s.ConnectToMaster()
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	isReplica := false
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

		commandArgs, _, err := parser.ParseArray(reader)
		if err != nil {
			if err == io.EOF {
				if isReplica {
					s.RemoveReplica(conn)
				}
				return
			}
			fmt.Printf("Error parsing command: %v\n", err)
			continue
		}

		if len(commandArgs) > 0 {
			switch commandArgs[0] {
			case "PING":
				if !isReplica {
					isReplica = true
					s.AddReplica(conn)
				}
			}
		}

		if err := s.handler.Handle(conn, commandArgs); err != nil {
			fmt.Printf("Error handling command: %v\n", err)
		}

		if !isReplica {
			s.IncrementOffset(1)
		}
	}
}

func (s *Server) handleReplicaConnection(reader *bufio.Reader, conn net.Conn) {

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

	commandArgs, cmdSize, err := parser.ParseArray(reader)
	if err != nil {
		fmt.Println("error parsing array:", err)
		return
	}

	fmt.Printf("Received command: %v - %v\n", commandArgs, s.role)

	s.lastBytesLen = cmdSize

	// Process command from master without sending a response
	if err := s.handler.HandleReplicaCommand(conn, commandArgs); err != nil {
		fmt.Printf("Error handling replica command: %v\n", err)
	}

	s.UpdateMasterReplOffset()
}
