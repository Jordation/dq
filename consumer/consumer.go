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
	srv     net.Conn
	msgChan chan []byte
}

type ConsumeMode int8

const (
	FromLatest ConsumeMode = iota
	FromOffset
)

func NewConsumer(addr string) (*Consumer, error) {
	conn, err := net.Dial("tcp", ":"+addr)
	if err != nil {
		return nil, err
	}

	msgChan := util.PollConnection(conn)

	return &Consumer{
		srv:     conn,
		msgChan: msgChan,
	}, nil
}

func (pr *Consumer) Start(consumeMode ConsumeMode, offset ...int64) {
	switch consumeMode {
	case FromLatest:
	case FromOffset:

	}
}

func (c *Consumer) Consume() chan []byte {
	resp := make(chan []byte)
	requestRead(c.srv, 0)

	go func(outChan chan<- []byte) {
		for {
			in := <-c.msgChan
			msg, nextOffset := parseSrvMessage(in)

			fmt.Println("msg: ", string(msg))
			//outChan <- msg

			if err := requestRead(c.srv, nextOffset); err != nil {
				logrus.WithError(err).Errorf("error requesting next read at offset %d", nextOffset)
				return
			}
		}

	}(resp)

	return resp
}

func parseSrvMessage(msg []byte) ([]byte, int64) {
	segs := bytes.Split(msg, []byte{':'})
	nextOffset, err := strconv.ParseInt(string(segs[0]), 0, 64)
	if err != nil {
		logrus.WithError(err).Error("error parsing server offset response")
	}

	return bytes.Join(segs[1:], []byte{}), nextOffset
}

func requestRead(srv net.Conn, off int64) error {
	_, err := fmt.Fprintf(srv, "read:default:%d\n", off)
	return err
}