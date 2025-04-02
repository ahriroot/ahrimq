package main

import (
	v1 "ahrimq/v1"
	"fmt"
	"time"
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
