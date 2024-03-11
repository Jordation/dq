package consumer

import (
	"context"
	"io"
	"time"

	"github.com/Jordation/godq"
	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	Client godq.GoDqClient

	Cfg    *Config
	log    logrus.FieldLogger
	offset int64
}

type Config struct {
	ServerAddr  string
	Queue       string
	BatchSize   int32
	StartOffset int64
}

func DefaultConfig() *Config {
	return &Config{
		ServerAddr:  "localhost:50051",
		Queue:       "default",
		BatchSize:   3,
		StartOffset: 0,
	}
}

func NewConsumer(ctx context.Context) (*Consumer, func(), error) {
	c := &Consumer{
		Cfg: DefaultConfig(),
		log: logrus.New().WithField("svc", "consumer"),
	}

	conn, err := grpc.DialContext(ctx,
		c.Cfg.ServerAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	c.Client = godq.NewGoDqClient(conn)

	cleanup := func() {
		if err := conn.Close(); err != nil {
			c.log.WithError(err).Error("failed to close connection")
		}
		c.log.Info("shutting down")
	}

	return c, cleanup, nil
}

func (c *Consumer) Consume(ctx context.Context) <-chan *godq.Message {
	msgChan := make(chan *godq.Message)

	go func() {
		defer close(msgChan)
		for {
			select {

			default:
				reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				batchClient, err := c.Client.ConsumeBatch(reqCtx, &godq.ConsumeRequest{
					Queue:     c.Cfg.Queue,
					Offset:    c.offset,
					BatchSize: c.Cfg.BatchSize,
				})
				if err != nil {
					c.log.WithError(err).Error("failed to consume batch")
					continue
				}

				c.handleBatch(batchClient, msgChan)
				return

			case <-ctx.Done():
				c.log.WithError(ctx.Err()).Error("consumer closing channel")
				return

			}
		}
	}()

	return msgChan
}

func (c *Consumer) handleBatch(batchClient godq.GoDq_ConsumeBatchClient, outChan chan<- *godq.Message) {
	for {
		msg, err := batchClient.Recv()
		if err == io.EOF {
			// TODO
			spew.Dump(batchClient.Trailer())

			c.log.Info("end of stream")
			return
		} else if err != nil {
			c.log.WithError(err).Error("failed to receive message")
			return
		}
		outChan <- msg
	}

}
