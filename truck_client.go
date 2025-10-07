package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

type TruckConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialManager(addr string) (*TruckConn, error) {
	const (
		attempts = 50
		pause    = 100 * time.Millisecond
	)
	var lastErr error
	for i := 0; i < attempts; i++ {
		c, err := net.Dial("tcp", "127.0.0.1:9000")
		if err == nil {
			return &TruckConn{conn: c, r: bufio.NewReader(c)}, nil
		}
		lastErr = err
		time.Sleep(pause)
	}
	return nil, fmt.Errorf("dial manager failed: %w", lastErr)
}

func (tc *TruckConn) req(m Message) (Message, error) {
	// all requests get a reply
	if err := writeMsg(tc.conn, m); err != nil {
		return Message{}, err
	}
	return readMsg(tc.r)
}
