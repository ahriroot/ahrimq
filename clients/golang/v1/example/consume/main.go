package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	v1 "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
	amq, err := v1.NewAhrimq(v1.Config{
		Path: "/tmp/ahrimq.sock",
		// Host: "127.0.0.1",
		// Port: 60001,
		AccessKey:    "your_access_key",
		AccessSecret: "your_access_secret",
		Mode:         v1.Passive,
	})
	if err != nil {
		panic(err)
	}

	err = amq.Connect()
	if err != nil {
		panic(err)
	}

	err = amq.Consume("normal", func(message []byte) error {
		fmt.Printf("Received normal message: %s\n", message)
		return v1.ConsumeAck
	})
	if err != nil {
		panic(err)
	}

	err = amq.Consume("ordered", func(message []byte) error {
		fmt.Printf("Received ordered message: %s\n", message)
		return v1.ConsumeAck
	})
	if err != nil {
		panic(err)
	}

	err = amq.Consume("delay", func(message []byte) error {
		fmt.Printf("Received delay message: %s\n", message)
		return v1.ConsumeAck
	})
	if err != nil {
		panic(err)
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	// wait for exit
	<-exit
}
