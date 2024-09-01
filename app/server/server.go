package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"

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
	}

	if replicaof != "" {
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
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("New connection from %s\n", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	for {
		_, err := reader.ReadByte()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("error parsing the data type:", err)
			return
		}

		commandArgs, err := parser.ParseArray(reader)
		if err != nil {
			fmt.Println("error parsing array:", err)
			return
		}
		if err := s.handler.Handle(conn, commandArgs); err != nil {
			fmt.Printf("Error handling command: %v\n", err)
			return
		}
	}
}
