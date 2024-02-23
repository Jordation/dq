package v2

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
)

func Consume(ctx context.Context) error {
	srv, err := net.Dial("tcp", ":3030")
	if err != nil {
		return err
	}

	server := newConnWrapper(ctx, srv)

	hsRequest := &ClientHandshakeRequest{
		Type:             "consumer",
		QueueName:        "default",
		StartOffset:      0,
		RequestChunkSize: 3,
	}

	if err := server.Write(ctx, hsRequest); err != nil {
		return err
	}

	msg := <-server.messageChannel

	handshakeResp, err := Unmarshal[HandshakeResponse](msg)
	if err != nil {
		return err
	} else if !handshakeResp.Ok {
		return fmt.Errorf("handshake failed: %s", handshakeResp.Message)
	}
	logrus.Info("handshake successful")

	offset := hsRequest.StartOffset

	kickoffRequest := &ConsumerRequest{
		Offset:    offset,
		BatchSize: 3,
	}
	if err := server.Write(ctx, kickoffRequest); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-server.messageChannel:
			consumerResp, err := Unmarshal[ConsumerResponse](msg)
			if err != nil {
				return err
			}

			if !consumerResp.Ok {
				return fmt.Errorf("consumer failed: %s", consumerResp.Message)
			} else if consumerResp.Message == "finished consuming" {
				logrus.Info("caught up")
				continue
			}

			fmt.Printf("consumerResp: %v\n", *consumerResp)

			time.Sleep(time.Second) // prentend to do some work

			offset += consumerResp.BatchSize

			nextReq := &ConsumerRequest{
				Offset:    offset,
				BatchSize: 3,
			}
			if err := server.Write(ctx, nextReq); err != nil {
				return err
			}
		}
	}
}
