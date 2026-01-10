package main

import (
	"fmt"
	"time"

	v1 "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
	amq, err := v1.NewAhrimq(v1.Config{
		Path: "/tmp/ahrimq.sock",
		// Host: "127.0.0.1",
		// Port: 60001,
		AccessKey:    "your_access_key",
		AccessSecret: "your_access_secret",
		Mode:         v1.Active,
	})
	if err != nil {
		panic(err)
	}

	err = amq.Connect()
	if err != nil {
		panic(err)
	}

	ids := make([]uint64, 0)
	for {
		resp, err := amq.PullMessage("test", 10)
		if err != nil {
			panic(err)
		}

		for _, message := range resp.Messages {
			fmt.Print(string(message.Message))
			ids = append(ids, message.ID)
		}
		fmt.Println()
		time.Sleep(1 * time.Second)

		amq.AckMulti(resp.Topic, ids)
	}
}
