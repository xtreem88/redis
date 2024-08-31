package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
)

type Server struct {
	listener net.Listener
	addr     string
	quitch   chan struct{}
}

func New(addr string) (*Server, error) {
	return &Server{
		addr:   addr,
		quitch: make(chan struct{}),
	}, nil
}

func (s *Server) Listen() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}
	defer l.Close()
	s.listener = l

	fmt.Printf("Server listening on %s\n", s.addr)
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
		handleCommand(conn, commandArgs)
	}
}

func handleCommand(conn net.Conn, s []string) {
	command := strings.ToUpper(s[0])
	cmd := handler.Commands[command]
	cmd(conn, s)
}
