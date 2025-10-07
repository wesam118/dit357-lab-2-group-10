package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

type Message struct {
	Type string `json:"type"`
	From string `json:"from,omitempty"`
	Corr string `json:"corr,omitempty"`

	X          *int  `json:"x,omitempty"`
	Y          *int  `json:"y,omitempty"`
	NX         *int  `json:"nx,omitempty"`
	NY         *int  `json:"ny,omitempty"`
	Need       *int  `json:"need,omitempty"`
	TruckWater *int  `json:"truckWater,omitempty"`
	OK         *bool `json:"ok,omitempty"`

	Info      string `json:"info,omitempty"`
	Intensity *int   `json:"intensity,omitempty"`
	Granted   *int   `json:"granted,omitempty"`
	Spent     *int   `json:"spent,omitempty"`

	// Local-only: used with channels in Task 0/1, not serialized
	Reply chan Message `json:"-"`
}

func pint(v int) *int    { return &v }
func pbool(v bool) *bool { return &v }

// (These are for Task 1 sockets later; safe to keep now.)
func writeMsg(w io.Writer, m Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}
func readMsg(r *bufio.Reader) (Message, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return Message{}, err
	}
	var m Message
	if err := json.Unmarshal(line, &m); err != nil {
		return Message{}, fmt.Errorf("bad json: %w", err)
	}
	return m, nil
}
