package main

import (
	"flag"
	"fmt"
	"github.com/nats-iop/nats.go"
)

func newNatsClient() {
	truckID := flag.String("id", "truck-1", "Unique Id for the truck")
	flag.Parse()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	subject := "measurements" + truckID

	fmt.Println("Subscribing to ", truckID, subject)

	nc.Subscribe(subject)
}
