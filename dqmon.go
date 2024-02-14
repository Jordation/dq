package main

import (
	"time"

	"github.com/Jordation/dqmon/consumer"
	"github.com/Jordation/dqmon/server"
)

func main() {
	s, _ := server.NewServer()
	go s.Start()

	time.Sleep(time.Millisecond * 50)
	c, err := consumer.NewConsumer("3030")
	if err != nil {
		panic(err)
	}
	c.Consume()
	select {}
}
