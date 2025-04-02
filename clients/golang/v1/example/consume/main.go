package main

import (
	v1 "ahrimq/v1"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	amq := v1.NewAhrimq(v1.Config{
		Host: "127.0.0.1",
		Port: 60001,
	})

	err := amq.Connect()
	if err != nil {
		panic(err)
	}

	err = amq.Consume("normal", func(message []byte) error {
		fmt.Printf("Received normal message: %s\n", message)
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = amq.Consume("ordered", func(message []byte) error {
		fmt.Printf("Received ordered message: %s\n", message)
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = amq.Consume("delay", func(message []byte) error {
		fmt.Printf("Received delay message: %s\n", message)
		return nil
	})
	if err != nil {
		panic(err)
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	// wait for exit
	<-exit
}
