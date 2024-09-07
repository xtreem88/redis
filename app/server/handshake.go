package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/communicate"
)

func (s *Server) ConnectToMaster() {
	masterConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", s.masterHost, s.masterPort))
	if err != nil {
		fmt.Printf("Failed to connect to master %v\n", err)
		os.Exit(1)
	}
	defer masterConn.Close()

	reader := s.Handshake(masterConn, s.Port)

	receiveRDB(reader)

	for {
		s.handleReplicaConnection(reader, masterConn)
	}
}

func (s *Server) Handshake(masterConn net.Conn, port int) *bufio.Reader {
	reader := bufio.NewReader(masterConn)
	masterConn.Write([]byte(communicate.EncodeStringArray([]string{"PING"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(communicate.EncodeStringArray([]string{"REPLCONF", "listening-port", strconv.Itoa(port)})))
	reader.ReadString('\n')
	masterConn.Write([]byte(communicate.EncodeStringArray([]string{"REPLCONF", "capa", "psync2"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(communicate.EncodeStringArray([]string{"PSYNC", "?", "-1"})))
	reader.ReadString('\n')
	return reader
}

func (s *Server) SendCommand(conn net.Conn, args ...string) error {
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := conn.Write([]byte(cmd))
	return err
}

func receiveRDB(reader *bufio.Reader) {
	response, _ := reader.ReadString('\n')
	if response[0] != '$' {
		fmt.Printf("Invalid response\n")
		os.Exit(1)
	}
	rdbSize, _ := strconv.Atoi(response[1 : len(response)-2])
	buffer := make([]byte, rdbSize)
	receivedSize, err := reader.Read(buffer)
	if err != nil {
		fmt.Printf("Invalid RDB received %v\n", err)
		os.Exit(1)
	}
	if rdbSize != receivedSize {
		fmt.Printf("Size mismatch - got: %d, want: %d\n", receivedSize, rdbSize)
	}

}
