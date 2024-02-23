package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Jordation/dqmon/consumer"
	"github.com/Jordation/dqmon/producer"
	"github.com/Jordation/dqmon/server"
	"github.com/Jordation/dqmon/store"
	v2 "github.com/Jordation/dqmon/v2"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func main() {
	// TODO: the offset is reversed from what it should be which is resulting in all messages being re-read when a new one comes in but otherwise it's pretty neat rn
	app := &cli.App{
		Name:  "distributed queue cli",
		Usage: "dq <port> <type>",
		Action: func(*cli.Context) error {
			if len(os.Args) < 3 {
				fmt.Println("Usage: dq <port> <type>")
				return nil
			}

			port := os.Args[1]
			typeOf := os.Args[2]
			switch typeOf {
			case "server":
				runServer(port)
			case "consoomer":
				runConsoomer(port)
			case "producer":
				runProducer(port)
			case "s2":
				runServerV2(port)
			case "c2":
				runConsumerV2(port)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runConsumerV2(port string) {
	if err := v2.Consume(context.Background()); err != nil {
		logrus.Fatal(err)
	}
}

func runConsoomer(port string) {
	c, err := consumer.NewConsumer(port, "default")
	if err != nil {
		panic(err)
	}

	out, err := c.Consume(context.Background())
	if err != nil {
		panic(err)
	}

	logrus.Info("starting consoomer aimed at :", port)
	for msg := range out {
		if _, err := fmt.Fprintf(os.Stdout, "message: %s\n", string(msg)); err != nil {
			logrus.Error(err)
		}
	}
}

func runServerV2(port string) {
	store, err := store.NewPartitionedStore("../store/partition/defaultstore")
	if err != nil {
		logrus.Fatal(err)
	}

	if err = v2.StartServer(context.Background(), store); err != nil {
		logrus.Fatal(err)
	}
}

func runServer(port string) {
	s, err := server.NewServer(port, "../store/partition/defaultstore")
	if err != nil {
		panic(err)
	}
	logrus.Info("starting server on :", port)
	if err := s.Start(context.Background()); err != nil {
		panic(err)
	}
}

func runProducer(port string) {
	p, err := producer.NewProducer(port, "default")
	if err != nil {
		panic(err)
	}

	if err = p.Start(context.Background()); err != nil {
		panic(err)
	}

	logrus.Info("starting producer aimed at :", port)
	logrus.Info("enter a message to submit data to the store")
	ioScanner := bufio.NewScanner(os.Stdin)
	for ioScanner.Scan() {
		if _, err := p.Write(context.TODO(), ioScanner.Bytes()); err != nil {
			logrus.Error(err)
		}
	}
}

/* func main() {
	s, _ := server.NewServer()
	go s.Start()

	time.Sleep(time.Millisecond * 50)
	c, err := consumer.NewConsumer("3030", "default")
	if err != nil {
		panic(err)
	}

	c.Consume()
	select {}
}
*/
