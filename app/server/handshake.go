package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/communicate"
)

func (s *Server) connectToMaster() {
	for {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", s.masterHost, s.masterPort))
		if err != nil {
			fmt.Printf("Failed to connect to master: %v. Retrying in 1 second...\n", err)
			time.Sleep(time.Second)
			continue
		}

		s.masterConn = conn
		fmt.Printf("Connected to master at %s:%d\n", s.masterHost, s.masterPort)

		if err := s.sendHandshake(conn); err != nil {
			fmt.Printf("Handshake failed: %v\n", err)
			conn.Close()
			continue
		}

		go s.handleSlaveConnection(conn)
		return
	}
}

func (s *Server) receiveRDBFile(conn net.Conn) error {
	reader := bufio.NewReader(conn)

	// Read the RDB file size
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB file size: %v", err)
	}

	size, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSuffix(line, "\r\n"), "$"))
	if err != nil {
		return fmt.Errorf("invalid RDB file size: %v", err)
	}

	// Read the RDB file content
	rdbContent := make([]byte, size)
	_, err = io.ReadFull(reader, rdbContent)
	if err != nil {
		return fmt.Errorf("failed to read RDB file content: %v", err)
	}

	// Process the RDB file (in this case, we're just ignoring it)
	fmt.Printf("Received RDB file of size %d bytes\n", size)

	return nil
}

func (s *Server) sendHandshake(conn net.Conn) error {
	// Send PING
	if err := s.SendCommand(conn, "PING"); err != nil {
		return fmt.Errorf("failed to send PING: %w", err)
	}
	if err := communicate.ReadResponse(conn, "PONG"); err != nil {
		return fmt.Errorf("failed to receive PONG after PING: %w", err)
	}
	fmt.Println("PING sent and PONG received")

	// Send first REPLCONF
	if err := s.SendCommand(conn, "REPLCONF", "listening-port", strconv.Itoa(s.Port)); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port: %w", err)
	}
	if err := communicate.ReadResponse(conn, "OK"); err != nil {
		return fmt.Errorf("failed to receive OK after REPLCONF listening-port: %w", err)
	}
	fmt.Println("REPLCONF listening-port sent and OK received")

	// Send second REPLCONF
	if err := s.SendCommand(conn, "REPLCONF", "capa", "eof", "capa", "psync2"); err != nil {
		return fmt.Errorf("failed to send REPLCONF capa: %w", err)
	}
	if err := communicate.ReadResponse(conn, "OK"); err != nil {
		return fmt.Errorf("failed to receive OK after REPLCONF capa: %w", err)
	}
	fmt.Println("REPLCONF capa eof capa psync2 sent and OK received")

	// Send PSYNC
	if err := s.SendCommand(conn, "PSYNC", "?", "-1"); err != nil {
		return fmt.Errorf("failed to send PSYNC: %w", err)
	}
	// We're ignoring the response for now, as per the instructions
	_, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response after PSYNC: %w", err)
	}
	fmt.Println("PSYNC sent and response received (ignored)")

	if err := s.receiveRDBFile(conn); err != nil {
		fmt.Printf("Failed to receive RDB file: %v\n", err)
	}

	fmt.Println("Handshake completed successfully")
	return nil
}

func (s *Server) SendCommand(conn net.Conn, args ...string) error {
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := conn.Write([]byte(cmd))
	return err
}
