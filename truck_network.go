package main

import (
	"bufio"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

type TruckNetwork struct {
	selfID     string
	listenAddr string

	peerAddrs map[string]string

	mu       sync.RWMutex
	peers    map[string]*peerConn
	knownIDs map[string]struct{}
	captain  string
	incoming chan Message

	listener net.Listener
}

type peerConn struct {
	id   string
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex
}

func newTruckNetwork(selfID, listenAddr string, peerAddrs map[string]string) (*TruckNetwork, error) {
	tn := &TruckNetwork{
		selfID:     selfID,
		listenAddr: listenAddr,
		peerAddrs:  peerAddrs,
		peers:      make(map[string]*peerConn),
		knownIDs:   map[string]struct{}{selfID: {}},
		captain:    selfID,
		incoming:   make(chan Message, 32),
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("truck %s listen failed: %w", selfID, err)
	}
	tn.listener = ln
	go tn.acceptLoop()
	go tn.dialPeers()
	return tn, nil
}

func (tn *TruckNetwork) acceptLoop() {
	for {
		conn, err := tn.listener.Accept()
		if err != nil {
			fmt.Printf("[Truck %s] accept error: %v\n", tn.selfID, err)
			return
		}
		go tn.registerPeerConn(conn, "")
	}
}

func (tn *TruckNetwork) dialPeers() {
	// Connect to peers with lexicographically smaller IDs to avoid duplicate links
	ids := make([]string, 0, len(tn.peerAddrs))
	for id := range tn.peerAddrs {
		if id == tn.selfID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		if id >= tn.selfID {
			continue
		}
		addr := tn.peerAddrs[id]
		go func(pid, paddr string) {
			for {
				conn, err := net.Dial("tcp", paddr)
				if err != nil {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				tn.registerPeerConn(conn, pid)
				return
			}
		}(id, addr)
	}
}

func (tn *TruckNetwork) registerPeerConn(conn net.Conn, expectedID string) {
	pc := &peerConn{
		conn: conn,
		r:    bufio.NewReader(conn),
	}

	go tn.readLoop(pc, expectedID)

	// Send hello immediately; both dialing and accepting sides do this for symmetry
	pc.send(Message{
		Type: "hello",
		From: tn.selfID,
		Info: tn.listenAddr,
	})
}

func (tn *TruckNetwork) addPeer(pc *peerConn) bool {
	if pc.id == "" {
		return false
	}
	tn.mu.Lock()
	defer tn.mu.Unlock()

	if existing, ok := tn.peers[pc.id]; ok {
		// Prefer the first connection and drop duplicates
		if existing != pc {
			pc.conn.Close()
			return false
		}
	}

	tn.peers[pc.id] = pc
	tn.knownIDs[pc.id] = struct{}{}
	if _, ok := tn.peerAddrs[pc.id]; !ok {
		tn.peerAddrs[pc.id] = pc.conn.RemoteAddr().String()
	}
	return true
}

func (tn *TruckNetwork) readLoop(pc *peerConn, expectedID string) {
	defer func() {
		tn.removePeer(pc)
	}()

	for {
		msg, err := readMsg(pc.r)
		if err != nil {
			fmt.Printf("[Truck %s] peer read error: %v\n", tn.selfID, err)
			return
		}
		switch msg.Type {
		case "hello":
			if msg.From == tn.selfID {
				continue
			}
			if expectedID != "" && expectedID != msg.From {
				fmt.Printf("[Truck %s] expected peer %s, got %s. closing.\n", tn.selfID, expectedID, msg.From)
				return
			}
			pc.id = msg.From
			tn.observeID(msg.From)
			if msg.Info != "" {
				tn.mu.Lock()
				if _, ok := tn.peerAddrs[msg.From]; !ok {
					tn.peerAddrs[msg.From] = msg.Info
				}
				tn.mu.Unlock()
			}
			if tn.addPeer(pc) {
				tn.recomputeCaptain(true)
			}

			// Reply with our hello in case the other side connected first
			pc.send(Message{
				Type: "hello_ack",
				From: tn.selfID,
				Info: tn.listenAddr,
			})
		case "hello_ack":
			if msg.From == tn.selfID {
				continue
			}
			if pc.id == "" {
				pc.id = msg.From
				tn.observeID(msg.From)
				if tn.addPeer(pc) {
					tn.recomputeCaptain(true)
				}
			}
			if msg.Info != "" {
				tn.mu.Lock()
				if _, ok := tn.peerAddrs[msg.From]; !ok {
					tn.peerAddrs[msg.From] = msg.Info
				}
				tn.mu.Unlock()
			}
		case "captain_announce":
			tn.handleCaptainAnnouncement(msg)
		case "status_update":
			tn.handleStatusUpdate(pc, msg)
		default:
			tn.deliver(msg)
		}
	}
}

func (tn *TruckNetwork) removePeer(pc *peerConn) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	if pc.id != "" {
		if existing, ok := tn.peers[pc.id]; ok && existing == pc {
			delete(tn.peers, pc.id)
			delete(tn.knownIDs, pc.id)
			go tn.recomputeCaptain(false)
		}
	}
	pc.conn.Close()
}

func (pc *peerConn) send(msg Message) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	writeMsg(pc.conn, msg)
}

func (tn *TruckNetwork) observeID(id string) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	if id == "" {
		return
	}
	if _, ok := tn.knownIDs[id]; !ok {
		tn.knownIDs[id] = struct{}{}
	}
}

func (tn *TruckNetwork) recomputeCaptain(propagate bool) {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	prev := tn.captain
	minID := tn.selfID
	for id := range tn.knownIDs {
		if id < minID {
			minID = id
		}
	}
	if prev == minID {
		return
	}
	tn.captain = minID
	if propagate {
		go tn.broadcast(Message{
			Type: "captain_announce",
			From: tn.selfID,
			Info: minID,
		})
	}
}

func (tn *TruckNetwork) handleCaptainAnnouncement(msg Message) {
	if msg.Info == "" {
		return
	}
	tn.observeID(msg.Info)
	tn.mu.Lock()
	changed := tn.captain != msg.Info
	tn.captain = msg.Info
	tn.mu.Unlock()
	if changed {
		go tn.broadcast(Message{
			Type: "captain_announce",
			From: tn.selfID,
			Info: msg.Info,
		})
	}
}

func (tn *TruckNetwork) handleStatusUpdate(pc *peerConn, msg Message) {
	tn.deliver(msg)
	if tn.IsCaptain() {
		tn.broadcastExcept(msg, msg.From)
	}
}

func (tn *TruckNetwork) deliver(msg Message) {
	select {
	case tn.incoming <- msg:
	default:
		// drop oldest to make room
		select {
		case <-tn.incoming:
		default:
		}
		tn.incoming <- msg
	}
}

func (tn *TruckNetwork) broadcast(msg Message) {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	for id, pc := range tn.peers {
		if id == tn.selfID {
			continue
		}
		pc.send(msg)
	}
}

func (tn *TruckNetwork) broadcastExcept(msg Message, exclude string) {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	for id, pc := range tn.peers {
		if id == exclude || id == tn.selfID {
			continue
		}
		pc.send(msg)
	}
}

func (tn *TruckNetwork) SendToCaptain(msg Message) {
	captain := tn.CurrentCaptain()
	if captain == "" {
		return
	}
	if captain == tn.selfID {
		tn.deliver(msg)
		tn.broadcastExcept(msg, tn.selfID)
		return
	}
	tn.mu.RLock()
	pc, ok := tn.peers[captain]
	tn.mu.RUnlock()
	if !ok {
		return
	}
	pc.send(msg)
}

func (tn *TruckNetwork) PublishStatus(msg Message) {
	msg.Type = "status_update"
	msg.From = tn.selfID
	if tn.IsCaptain() {
		tn.deliver(msg)
		tn.broadcastExcept(msg, tn.selfID)
		return
	}
	tn.SendToCaptain(msg)
}

func (tn *TruckNetwork) Incoming() <-chan Message {
	return tn.incoming
}

func (tn *TruckNetwork) CurrentCaptain() string {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	return tn.captain
}

func (tn *TruckNetwork) IsCaptain() bool {
	return tn.CurrentCaptain() == tn.selfID
}
