package handler

import (
	"net"
	"strings"
	"time"
)

type ResponseWriter struct {
	builder strings.Builder
}

func (w *ResponseWriter) Write(b []byte) (n int, err error) {
	return w.builder.Write(b)
}

func (w *ResponseWriter) Close() error {
	return nil
}

func (w *ResponseWriter) LocalAddr() net.Addr {
	return nil
}

func (w *ResponseWriter) RemoteAddr() net.Addr {
	return nil
}

func (w *ResponseWriter) SetDeadline(t time.Time) error {
	return nil
}

func (w *ResponseWriter) SetReadDeadline(t time.Time) error {
	return nil
}

func (w *ResponseWriter) SetWriteDeadline(t time.Time) error {
	return nil
}

func (w *ResponseWriter) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (w *ResponseWriter) String() string {
	return w.builder.String()
}
