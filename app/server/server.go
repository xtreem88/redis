package server

import (
	"errors"
	"fmt"
	"io"
	"net"
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
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			fmt.Printf("Client closed the connection: %s\n", conn.RemoteAddr())
			break
		} else if err != nil {
			fmt.Printf("Error reading from connection: %v\n", err)
			break
		}
		fmt.Printf("Received: %s", buf[:n])
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Printf("Error writing to connection: %v\n", err)
			break
		}
	}
}
