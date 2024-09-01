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
	listener   net.Listener
	Port       int
	Addr       string
	RDB        *persistence.RDB
	Config     *config.Config
	role       string
	masterHost string
	masterPort int

	quitch  chan struct{}
	handler *handler.Handler
}

func New(addr string, port int, dir, dbfilename string, replicaof string) (*Server, error) {
	cfg := config.New(dir, dbfilename)
	rdb, err := persistence.LoadRDB(dir, dbfilename)
	if err != nil {
		return nil, fmt.Errorf("failed to load RDB: %w", err)
	}

	s := &Server{
		Port:   port,
		Addr:   addr,
		quitch: make(chan struct{}),
		Config: cfg,
		RDB:    rdb,
		role:   "master",
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

func (s *Server) Listen() error {
	addr := fmt.Sprintf("%s:%d", s.Addr, s.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %d: %w", s.Port, err)
	}
	defer l.Close()
	s.listener = l

	fmt.Printf("Server listening on %s\n", addr)
	go s.accept()
	<-s.quitch
	return nil
}

func (s *Server) accept() {
	for {
		conn, err := s.listener.Accept()
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
