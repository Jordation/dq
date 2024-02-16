package consumer

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/Jordation/dqmon/util"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	srv       net.Conn
	queueName string
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
		srv:       srv,
		queueName: queueName,
	}, nil
}

// consume will close the out channel if there is an error
// the caller should read from the queue safely in order to not read a closed channel
func (c *Consumer) Consume() chan []byte {
	outChan := make(chan []byte)
	msgChan := util.PollConnection(c.srv)

	requestRead(c.srv, c.queueName, 0)

	go func(outChan chan<- []byte) {
		for {
			in := <-msgChan
			msg, nextOffset := parseSrvMessage(in)

			fmt.Println("msg: ", string(msg))
			//outChan <- msg

			if err := requestRead(c.srv, c.queueName, nextOffset); err != nil {
				logrus.WithError(err).Errorf("error requesting next read at offset %d", nextOffset)
				return
			}
		}
	}(outChan)

	return outChan
}

func parseSrvMessage(msg []byte) ([]byte, int64) {
	offsetAsBytes, msg, found := bytes.Cut(msg, []byte{':'})
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
