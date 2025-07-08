package main

import (
	"fmt"
	"time"

	v1 "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
	amq, err := v1.NewAhrimq(v1.Config{
		Host: "127.0.0.1",
		Port: 60001,
		Mode: v1.Passive,
	})
	if err != nil {
		panic(err)
	}

	err = amq.Connect()
	if err != nil {
		panic(err)
	}

	err = amq.Subscribe("topic", func(msg []byte) error {
		fmt.Printf("Received message: %s\n", msg)
		return nil
	})
	if err != nil {
		panic(err)
	}

	for {
		message := "Hello, world!"
		err = amq.Publish("topic", []byte(message))
		if err != nil {
			panic(err)
		}
		time.Sleep(1 * time.Second)
	}
}
