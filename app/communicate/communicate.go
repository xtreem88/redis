package communicate

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func EncodeSimpleString(s string) string {
	return fmt.Sprintf("*1\r\n$%d\r\n%s\r\n", len(s), s)
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
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	response = strings.TrimSpace(response)
	if response != expected {
		return fmt.Errorf("unexpected response: got %s, want %s", response, expected)
	}
	return nil
}

func ParseCommands(token string, arrSize int, strSize int, cmd []string) ([]string, int, int) {
	switch token[0] {
	case '*':
		arrSize, _ = strconv.Atoi(token[1:])
	case '$':
		strSize, _ = strconv.Atoi(token[1:])
	default:
		if len(token) != strSize {
			fmt.Printf("[from master] Wrong string size - got: %d, want: %d\n", len(token), strSize)
			break
		}
		arrSize--
		strSize = 0
		cmd = append(cmd, token)
	}
	return cmd, arrSize, strSize
}

func EncodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
func EncodeInteger(number int) string {
	return fmt.Sprintf(":%d\r\n", number)
}

func EncodeStringArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += EncodeBulkString(s)
	}
	return result
}
