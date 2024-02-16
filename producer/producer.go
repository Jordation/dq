package producer

import (
	"fmt"
	"net"
)

type Producer struct {
	srv       net.Conn
	queueName string
}

func NewProducer(addr string, queueName string) (*Producer, error) {
	conn, err := net.Dial("tcp", ":"+addr)
	if err != nil {
		return nil, err
	}

	return &Producer{
		srv:       conn,
		queueName: queueName,
	}, nil
}

func (pr *Producer) Write(p []byte) (int, error) {
	return fmt.Fprintf(pr.srv, "write:%s:%s\n", pr.queueName, p)
}
