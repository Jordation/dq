package consumer

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/Jordation/dqmon/types"
	"github.com/Jordation/dqmon/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	srv           net.Conn
	queueName     string
	currentOffset int64
}

func NewConsumer(port string, queueName string) (*Consumer, error) {
	var (
		srv     net.Conn
		retries = 3
	)

	for i := range retries {
		conn, err := net.Dial("tcp", ":"+port)
		if err != nil && retries == 0 {
			return nil, err
		}

		if err == nil {
			logrus.Info("connected to server on :", port)
			srv = conn
			break
		}

		time.Sleep(time.Millisecond * 200 * time.Duration(i))
		logrus.Info("retrying")
		retries--
	}

	return &Consumer{
		srv:           srv,
		queueName:     queueName,
		currentOffset: 0,
	}, nil
}

// consume will close the out channel if there is an error
// the caller should read from the queue safely in order to not read a closed channel
func (c *Consumer) Consume() chan []byte {
	outChan := make(chan []byte, 2)
	msgChan := util.PollConnection(c.srv)

	firstOffset, err := c.handleConnectionHandshake(context.Background(), c.srv, msgChan)
	if err != nil {
		logrus.Error(err)
		logrus.Fatal("failed handshake")
	}

	go func(outChan chan<- []byte) {
		defer close(outChan)
		if err := requestRead(c.srv, c.queueName, firstOffset); err != nil {
			logrus.WithError(err).Error("error requesting first read")
			return
		}

		for in := range msgChan {
			msg, nextOffset := parseSrvMessage(in)

			fmt.Println("msg: ", string(msg))
			//outChan <- msg

			if err := requestRead(c.srv, c.queueName, nextOffset); err != nil {
				logrus.WithError(err).Errorf("error requesting next read at offset %d", nextOffset)
				return
			}
			c.currentOffset++
		}
	}(outChan)

	return outChan
}

func (c *Consumer) handleConnectionHandshake(ctx context.Context, conn net.Conn, msgChan <-chan []byte) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	_, err := fmt.Fprintf(conn, "%s:%s:%d\n", types.MessageTypeConsumerHanshake, c.queueName, c.currentOffset)
	if err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, fmt.Errorf("handshake failed: timeout")

	case handshakeResponse := <-msgChan:
		logrus.Info("SRV MESSAGE: ", string(handshakeResponse))

		if len(handshakeResponse) < 2 {
			return 0, fmt.Errorf("handshake failed: bad response (%s)", string(handshakeResponse))
		}

		if !bytes.Equal(handshakeResponse[:2], types.MessageHandshakeOK) {
			return 0, fmt.Errorf("handshake failed: bad response (%s)", string(handshakeResponse))
		}

		firstOffset, err := strconv.ParseInt(string(handshakeResponse[3:]), 0, 64)
		if err != nil {
			return 0, errors.Wrap(err, "handshake failed: parsing offset")
		}

		return firstOffset, nil
	}
}

func parseSrvMessage(msg []byte) ([]byte, int64) {
	offsetAsBytes, msg, found := bytes.Cut(msg, types.MessageDelim)
	if !found {
		return nil, 0
	}

	nextOffset, err := strconv.ParseInt(string(offsetAsBytes), 0, 64)
	if err != nil {
		logrus.WithError(err).Error("error parsing server offset response")
	}

	return msg, nextOffset
}

func requestRead(srv net.Conn, queueName string, off int64) error {
	_, err := fmt.Fprintf(srv, "read:%s:%d\n", queueName, off)
	return err
}
