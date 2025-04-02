package main

import (
	v1 "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
	amq := v1.NewAhrimq(v1.Config{
		Host: "127.0.0.1",
		Port: 60001,
	})

	err := amq.Connect()

	message := "Hello, world!"

	err = amq.ProduceNormal("normal", []byte(message))
	if err != nil {
		panic(err)
	}

	err = amq.ProduceOrdered("ordered", []byte(message))
	if err != nil {
		panic(err)
	}

	err = amq.ProduceDelay("delay", []byte(message), 3)
	if err != nil {
		panic(err)
	}
}
