package communicate

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func EncodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func EncodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func SendResponse(conn net.Conn, response string) error {
	if conn == nil {
		return nil
	}
	_, err := conn.Write([]byte(response))
	return err
}

func SendCommand(conn net.Conn, args ...string) error {
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := conn.Write([]byte(cmd))
	return err
}

func ReadResponse(conn net.Conn, expected string) error {
	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+"+expected) {
		return fmt.Errorf("expected +%s, got %q", expected, resp)
	}
	return nil
}
