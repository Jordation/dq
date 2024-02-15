package consumer

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/Jordation/dqmon/util"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	srv           net.Conn
	msgChan       chan []byte
	consumingFrom string
	currOffset    int64
}

func NewConsumer(addr string, consumesFrom string) (*Consumer, error) {
	conn, err := net.Dial("tcp", ":"+addr)
	if err != nil {
		return nil, err
	}

	msgChan := util.PollConnection(conn)

	return &Consumer{
		srv:           conn,
		msgChan:       msgChan,
		consumingFrom: consumesFrom,
	}, nil
}

// the out channel gets the buffers from the server
func (c *Consumer) Consume() chan []byte {
	outChan := make(chan []byte)
	requestRead(c.srv, "default", 0)

	go func(outChan chan<- []byte) {
		for {
			in := <-c.msgChan
			msg, nextOffset := parseSrvMessage(in)

			fmt.Println("msg: ", string(msg))
			//outChan <- msg

			if err := requestRead(c.srv, c.consumingFrom, nextOffset); err != nil {
				logrus.WithError(err).Errorf("error requesting next read at offset %d", nextOffset)
				return
			}
		}
	}(outChan)

	return outChan
}

func parseSrvMessage(msg []byte) ([]byte, int64) {
	offsetAsBytes, msg, _ := bytes.Cut(msg, []byte{':'})
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
