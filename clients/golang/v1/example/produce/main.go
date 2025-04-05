package main

import (
	v1 "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
	amq := v1.NewAhrimq(v1.Config{
		Host:         "127.0.0.1",
		Port:         60001,
		AccessKey:    "your_access_key",
		AccessSecret: "your_access_secret",
	})

	err := amq.Connect()
	if err != nil {
		panic(err)
	}

	err = amq.ProduceNormal("normal", []byte("normal message"))
	if err != nil {
		panic(err)
	}

	err = amq.ProduceOrdered("ordered", []byte("ordered message"))
	if err != nil {
		panic(err)
	}

	err = amq.ProduceDelay("delay", []byte("delay message"), 3)
	if err != nil {
		panic(err)
	}
}
