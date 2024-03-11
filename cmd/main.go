package main

import (
	"context"
	"fmt"

	"github.com/Jordation/godq/consumer"
	"github.com/Jordation/godq/server"
	"github.com/sirupsen/logrus"
)

func main() {
	srv, _ := server.New(context.Background(), nil)
	waitChan, closeSrv := srv.Serve()
	defer closeSrv()

	cl, closeClient, err := consumer.NewConsumer(context.Background())
	if err != nil {
		panic(err)
	}
	defer closeClient()

	go func() {
		msgchan := cl.Consume(context.Background())
		for msg := range msgchan {
			fmt.Println(string(msg.Data))
		}
	}()

	<-waitChan
	logrus.Info("shutting down")
}
