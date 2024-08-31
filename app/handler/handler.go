package handler

import (
	"fmt"
	"net"
)

var Commands = map[string]func(conn net.Conn, s []string) (int, error){
	"PING": ping,
	"ECHO": echo,
}

func ping(conn net.Conn, s []string) (int, error) {
	return fmt.Fprint(conn, encodeSimpleString("PONG"))
}
func echo(conn net.Conn, s []string) (int, error) {
	return fmt.Fprint(conn, encodeSimpleString(s[1]))
}

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}
