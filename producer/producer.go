package producer

import (
	"fmt"
	"net"
)

type Producer struct {
	srv net.Conn
}

func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", ":"+addr)
	if err != nil {
		return nil, err
	}

	return &Producer{
		srv: conn,
	}, nil
}

func (pr *Producer) Write(p []byte) (int, error) {
	return fmt.Fprintf(pr.srv, "write:%s", p)
}
