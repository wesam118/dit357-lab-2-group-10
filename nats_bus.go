package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	statusSubject = "trucks.status"
	peerTTL       = 5 * time.Second
	requestFormat = "trucks.%s.direct"
)

type TruckBus struct {
	id     string
	conn   *nats.Conn
	allIDs []string

	mu             sync.RWMutex
	lastSeen       map[string]time.Time
	requestHandler func(Message) Message

	statusCh   chan Message
	statusSub  *nats.Subscription
	requestSub *nats.Subscription
}

func newTruckBus(id string, peerIDs []string, url string) (*TruckBus, error) {
	opts := []nats.Option{
		nats.Name("truck-" + id),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.MaxReconnects(-1),
	}
	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("truck %s failed to connect to NATS: %w", id, err)
	}

	unique := make(map[string]struct{}, len(peerIDs)+1)
	for _, pid := range peerIDs {
		unique[pid] = struct{}{}
	}
	unique[id] = struct{}{}

	allIDs := make([]string, 0, len(unique))
	for pid := range unique {
		allIDs = append(allIDs, pid)
	}

	tb := &TruckBus{
		id:       id,
		conn:     conn,
		allIDs:   allIDs,
		lastSeen: make(map[string]time.Time),
		statusCh: make(chan Message, 32),
	}
	tb.lastSeen[id] = time.Now()

	statusSub, err := conn.Subscribe(statusSubject, tb.handleStatus)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("truck %s failed to subscribe status: %w", id, err)
	}
	tb.statusSub = statusSub

	subject := fmt.Sprintf(requestFormat, id)
	requestSub, err := conn.Subscribe(subject, tb.handleRequest)
	if err != nil {
		statusSub.Unsubscribe()
		conn.Close()
		return nil, fmt.Errorf("truck %s failed to subscribe requests: %w", id, err)
	}
	tb.requestSub = requestSub

	return tb, nil
}

func (tb *TruckBus) handleStatus(nm *nats.Msg) {
	var msg Message
	if err := json.Unmarshal(nm.Data, &msg); err != nil {
		fmt.Printf("[Truck %s] bad status payload: %v\n", tb.id, err)
		return
	}
	if msg.From == "" {
		return
	}
	now := time.Now()
	tb.mu.Lock()
	tb.lastSeen[msg.From] = now
	found := false
	for _, id := range tb.allIDs {
		if id == msg.From {
			found = true
			break
		}
	}
	if !found {
		tb.allIDs = append(tb.allIDs, msg.From)
	}
	tb.mu.Unlock()

	if msg.From == tb.id {
		return
	}
	tb.enqueueStatus(msg)
}

func (tb *TruckBus) handleRequest(nm *nats.Msg) {
	var msg Message
	if err := json.Unmarshal(nm.Data, &msg); err != nil {
		fmt.Printf("[Truck %s] bad request payload: %v\n", tb.id, err)
		return
	}

	handler := tb.getRequestHandler()
	var resp Message
	if handler == nil {
		resp = Message{Type: msg.Type + "_resp", OK: pbool(false), Info: "no handler"}
	} else {
		resp = handler(msg)
	}
	if resp.From == "" {
		resp.From = tb.id
	}
	if nm.Reply == "" {
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		fmt.Printf("[Truck %s] marshal reply error: %v\n", tb.id, err)
		return
	}
	if err := tb.conn.Publish(nm.Reply, data); err != nil {
		fmt.Printf("[Truck %s] publish reply error: %v\n", tb.id, err)
	}
}

func (tb *TruckBus) enqueueStatus(msg Message) {
	select {
	case tb.statusCh <- msg:
	default:
		select {
		case <-tb.statusCh:
		default:
		}
		tb.statusCh <- msg
	}
}

func (tb *TruckBus) getRequestHandler() func(Message) Message {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	return tb.requestHandler
}

func (tb *TruckBus) SetRequestHandler(handler func(Message) Message) {
	tb.mu.Lock()
	tb.requestHandler = handler
	tb.mu.Unlock()
}

func (tb *TruckBus) PublishStatus(msg Message) {
	msg.Type = "status_update"
	msg.From = tb.id
	if msg.Corr == "" {
		msg.Corr = fmt.Sprintf("%s-%d", tb.id, time.Now().UnixNano())
	}
	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("[Truck %s] marshal status error: %v\n", tb.id, err)
		return
	}
	if err := tb.conn.Publish(statusSubject, data); err != nil {
		fmt.Printf("[Truck %s] publish status error: %v\n", tb.id, err)
		return
	}
	tb.mu.Lock()
	tb.lastSeen[tb.id] = time.Now()
	tb.mu.Unlock()
}

func (tb *TruckBus) StatusFeed() <-chan Message {
	return tb.statusCh
}

func (tb *TruckBus) CurrentCaptain() string {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	now := time.Now()
	var chosen string
	for _, id := range tb.allIDs {
		if id == tb.id {
			if chosen == "" || id < chosen {
				chosen = id
			}
			continue
		}
		last, ok := tb.lastSeen[id]
		if !ok || now.Sub(last) > peerTTL {
			continue
		}
		if chosen == "" || id < chosen {
			chosen = id
		}
	}
	if chosen == "" {
		return tb.id
	}
	return chosen
}

func (tb *TruckBus) IsCaptain() bool {
	return tb.CurrentCaptain() == tb.id
}

func (tb *TruckBus) RequestTo(target string, msg Message, timeout time.Duration) (Message, error) {
	if target == "" {
		return Message{}, fmt.Errorf("no target provided")
	}
	if msg.From == "" {
		msg.From = tb.id
	}
	if msg.Corr == "" {
		msg.Corr = fmt.Sprintf("%s-%d", tb.id, time.Now().UnixNano())
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return Message{}, fmt.Errorf("marshal request: %w", err)
	}
	if target == tb.id {
		handler := tb.getRequestHandler()
		if handler == nil {
			return Message{}, fmt.Errorf("no handler on self")
		}
		resp := handler(msg)
		if resp.From == "" {
			resp.From = tb.id
		}
		return resp, nil
	}
	subject := fmt.Sprintf(requestFormat, target)
	reply, err := tb.conn.Request(subject, data, timeout)
	if err != nil {
		return Message{}, err
	}
	var resp Message
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return Message{}, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func (tb *TruckBus) RequestCaptain(msg Message, timeout time.Duration) (Message, error) {
	target := tb.CurrentCaptain()
	if target == "" {
		return Message{}, fmt.Errorf("no captain elected")
	}
	return tb.RequestTo(target, msg, timeout)
}

func (tb *TruckBus) Close() {
	if tb.requestSub != nil {
		tb.requestSub.Unsubscribe()
	}
	if tb.statusSub != nil {
		tb.statusSub.Unsubscribe()
	}
	close(tb.statusCh)
	tb.conn.Drain()
}
