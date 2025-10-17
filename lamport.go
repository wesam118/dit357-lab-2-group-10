package main

import "sync"

// LamportClock maintains a thread-safe Lamport logical clock.
type LamportClock struct {
	mu    sync.Mutex
	value int
}

// NewLamportClock constructs a Lamport clock initialized to zero.
func NewLamportClock() *LamportClock {
	return &LamportClock{}
}

// Send increments the clock for a local send/event and returns the new value.
func (lc *LamportClock) Send() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.value++
	return lc.value
}

// Receive merges an incoming timestamp (if present) and increments the clock.
func (lc *LamportClock) Receive(ts *int) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if ts != nil && *ts > lc.value {
		lc.value = *ts
	}
	lc.value++
	return lc.value
}

// Stamp updates the clock for a send event and attaches the timestamp to msg.
func (lc *LamportClock) Stamp(msg *Message) int {
	ts := lc.Send()
	msg.TS = pint(ts)
	return ts
}

// Observe merges a timestamp without incrementing (used for peeking).
func (lc *LamportClock) Observe(ts *int) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if ts != nil && *ts > lc.value {
		lc.value = *ts
	}
	return lc.value
}
