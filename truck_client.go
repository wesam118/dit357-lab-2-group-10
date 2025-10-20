package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

const managerRequestTimeout = 5 * time.Second

type TruckConn struct {
	addr string
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
		c, err := net.Dial("tcp", addr)
		if err == nil {
			return &TruckConn{addr: addr, conn: c, r: bufio.NewReader(c)}, nil
		}
		lastErr = err
		time.Sleep(pause)
	}
	return nil, fmt.Errorf("dial manager failed: %w", lastErr)
}

func (tc *TruckConn) reconnect() error {
	if tc.conn != nil {
		tc.conn.Close()
	}
	c, err := net.Dial("tcp", tc.addr)
	if err != nil {
		return err
	}
	tc.conn = c
	tc.r = bufio.NewReader(c)
	return nil
}

func (tc *TruckConn) req(m Message) (Message, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if tc.conn == nil {
			if err := tc.reconnect(); err != nil {
				return Message{}, fmt.Errorf("manager reconnect failed: %w", err)
			}
		}

		deadline := time.Now().Add(managerRequestTimeout)
		_ = tc.conn.SetDeadline(deadline)

		if err := writeMsg(tc.conn, m); err != nil {
			lastErr = err
			if attempt == 0 {
				if recErr := tc.reconnect(); recErr == nil {
					continue
				}
			}
			return Message{}, fmt.Errorf("manager write failed: %w", lastErr)
		}

		resp, err := readMsg(tc.r)
		if err == nil {
			_ = tc.conn.SetDeadline(time.Time{})
			return resp, nil
		}

		lastErr = err
		if attempt == 0 {
			// Prepare a fresh connection for the next request.
			_ = tc.reconnect()
		}
		break
	}
	return Message{}, fmt.Errorf("manager read failed: %w", lastErr)
}
