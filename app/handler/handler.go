package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

var Commands = map[string]func(conn net.Conn, args []string, cfg *config.Config) error{
	"PING":   ping,
	"ECHO":   echo,
	"SET":    set,
	"GET":    get,
	"CONFIG": configCmd,
}

func ping(conn net.Conn, args []string, cfg *config.Config) error {
	_, err := fmt.Fprint(conn, encodeSimpleString("PONG"))
	return err
}

func echo(conn net.Conn, args []string, cfg *config.Config) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'echo' command")
	}
	_, err := fmt.Fprint(conn, encodeSimpleString(args[1]))
	return err
}

func set(conn net.Conn, args []string, cfg *config.Config) error {
	if len(args) < 3 {
		return fmt.Errorf("ERR wrong number of arguments for 'set' command")
	}
	key, value := args[1], args[2]
	var expiry time.Duration

	if len(args) > 3 && strings.ToUpper(args[3]) == "PX" {
		if len(args) < 5 {
			return fmt.Errorf("ERR syntax error")
		}
		ms, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return fmt.Errorf("ERR value is not an integer or out of range")
		}
		expiry = time.Duration(ms) * time.Millisecond
	}

	store.SetWithExpiry(key, value, expiry)
	_, err := fmt.Fprint(conn, encodeSimpleString("OK"))
	return err
}

func get(conn net.Conn, args []string, cfg *config.Config) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'get' command")
	}
	value, ok := store.Get(args[1])
	if !ok {
		_, err := fmt.Fprint(conn, "$-1\r\n")
		return err
	}
	_, err := fmt.Fprint(conn, encodeBulkString(value))
	return err
}

func configCmd(conn net.Conn, args []string, cfg *config.Config) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'config' command")
	}

	subcommand := strings.ToUpper(args[1])
	if subcommand != "GET" {
		return fmt.Errorf("ERR unsupported CONFIG subcommand: %s", subcommand)
	}

	if len(args) < 3 {
		return fmt.Errorf("ERR wrong number of arguments for 'config get' command")
	}

	param := args[2]
	value := cfg.Get(param)

	// Encode the response as a RESP array
	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(param), param, len(value), value)
	_, err := fmt.Fprint(conn, response)
	return err
}

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
