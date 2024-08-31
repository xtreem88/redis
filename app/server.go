package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	server := newServer()
	server.listen()
}

type Server struct {
	listener net.Listener
	quitch   chan struct{}
}

func newServer() *Server {
	return &Server{
		quitch: make(chan struct{}),
	}
}

func (s *Server) listen() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	s.listener = l
	go s.connect()
	<-s.quitch
}

func (s *Server) connect() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go s.respond(conn)
	}
}

func (s *Server) respond(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			fmt.Println("Client closed the connections:", conn.RemoteAddr())
			break
		} else if err != nil {
			fmt.Println("Error while reading the message")
		}
		conn.Write([]byte("+PONG\r\n"))
	}
}
