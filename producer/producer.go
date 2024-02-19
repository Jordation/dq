package producer

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Jordation/dqmon/types"
	"github.com/Jordation/dqmon/util"
	"github.com/sirupsen/logrus"
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

func (p *Producer) Start(ctx context.Context) error {
	msgChan := util.PollConnection(p.srv)

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	_, err := fmt.Fprintf(p.srv, "%s:%s\n", types.MessageTypeProducerHanshake, p.queueName)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("handshake failed: timeout")

	case handshakeResponse := <-msgChan:
		logrus.Info("SRV->PRD MESSAGE: ", string(handshakeResponse))

		if len(handshakeResponse) < 2 {
			return fmt.Errorf("handshake failed: bad response (%s)", string(handshakeResponse))
		}

		if !bytes.Equal(handshakeResponse[:2], types.MessageHandshakeOK) {
			return fmt.Errorf("handshake failed: not ok (%s)", string(handshakeResponse))
		}

		return nil
	}
}
