package main

import (
	"bufio"
	"fmt"
	"net"
)

// Run the manager as a TCP server
func runManagerTCP(manager map[string]interface{}) {
	listener, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}
	fmt.Println("[Manager] Listening on 127.0.0.1:9000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		go handleConnection(manager, conn)
	}
}

// Handle each connected truck
func handleConnection(manager map[string]interface{}, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		msg, err := readMsg(r)
		if err != nil {
			fmt.Println("[Manager] Conn closed:", err)
			return
		}

		// Temporary reply channel to capture manager’s answer
		replyCh := make(chan Message, 1)
		msg.Reply = replyCh

		// Reuse existing logic
		handleMessage(manager, msg)

		// Send reply over TCP
		select {
		case resp := <-replyCh:
			if resp.TS == nil {
				managerClock(manager).Stamp(&resp)
			}
			writeMsg(conn, resp)
		default:
			ack := Message{Type: msg.Type + "_ack", OK: pbool(true)}
			managerClock(manager).Stamp(&ack)
			writeMsg(conn, ack)
		}
	}
}
