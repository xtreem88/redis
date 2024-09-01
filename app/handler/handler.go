package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

var Commands = map[string]func(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error{
	"PING":   ping,
	"ECHO":   echo,
	"SET":    set,
	"GET":    get,
	"CONFIG": configCmd,
	"KEYS":   keys,
}

func ping(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error {
	_, err := fmt.Fprint(conn, encodeSimpleString("PONG"))
	return err
}

func echo(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'echo' command")
	}
	_, err := fmt.Fprint(conn, encodeSimpleString(args[1]))
	return err
}

func set(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error {
	if len(args) < 3 || len(args) > 5 {
		return fmt.Errorf("ERR wrong number of arguments for 'set' command")
	}

	key := args[1]
	value := args[2]
	var expiry *time.Time

	if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
		ms, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return fmt.Errorf("ERR invalid expire time in 'set' command")
		}
		t := time.Now().Add(time.Duration(ms) * time.Millisecond)
		expiry = &t
	}

	rdb.Set(key, value, expiry)

	_, err := fmt.Fprint(conn, "+OK\r\n")
	return err
}

func get(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error {
	if len(args) != 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'get' command")
	}

	key := args[1]
	value, ok := rdb.Get(key)
	if !ok {
		_, err := fmt.Fprint(conn, "$-1\r\n")
		return err
	}

	response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	_, err := fmt.Fprint(conn, response)
	return err
}

func configCmd(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error {
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

func keys(conn net.Conn, args []string, cfg *config.Config, rdb *persistence.RDB) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'keys' command")
	}

	pattern := args[1]
	if pattern != "*" {
		return fmt.Errorf("ERR only '*' pattern is supported")
	}

	keys := rdb.GetKeys()
	response := fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}

	_, err := fmt.Fprint(conn, response)
	return err
}

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
