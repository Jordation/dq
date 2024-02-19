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
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runConsoomer(port string) {
	c, err := consumer.NewConsumer(port, "default")
	if err != nil {
		panic(err)
	}

	out := c.Consume()

	logrus.Info("starting consoomer aimed at :", port)
	for msg := range out {
		if _, err := fmt.Fprintf(os.Stdout, "message: %s\n", string(msg)); err != nil {
			logrus.Error(err)
		}
	}
}

func runServer(port string) {
	s, err := server.NewServer(port, "../store/partition/defaultstore")
	if err != nil {
		panic(err)
	}
	logrus.Info("starting server on :", port)
	s.Start()
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
		if _, err := p.Write([]byte(ioScanner.Text())); err != nil {
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
