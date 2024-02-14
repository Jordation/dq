package producer

import (
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
	return pr.srv.Write(p)
}
