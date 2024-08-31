package handler

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

var Commands = map[string]func(conn net.Conn, args []string) error{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

func ping(conn net.Conn, args []string) error {
	_, err := fmt.Fprint(conn, encodeSimpleString("PONG"))
	return err
}

func echo(conn net.Conn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'echo' command")
	}
	_, err := fmt.Fprint(conn, encodeSimpleString(args[1]))
	return err
}

func set(conn net.Conn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("ERR wrong number of arguments for 'set' command")
	}
	key, value := args[1], args[2]
	store.Set(key, value)
	_, err := fmt.Fprint(conn, encodeSimpleString("OK"))
	return err
}

func get(conn net.Conn, args []string) error {
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

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
