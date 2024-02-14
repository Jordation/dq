package client

import (
	"net"

	"github.com/Jordation/dqmon/util"
	"github.com/sirupsen/logrus"
)

type DQClient struct {
	consumesFrom string
	lag          int64
	srv          net.Conn
}

func NewClient(queue string, startOffset int64) (*DQClient, error) {
	return &DQClient{
		consumesFrom: queue,
		lag:          startOffset,
	}, nil
}

func (c *DQClient) Start() {
	conn, err := net.Dial("tcp", ":3030")
	if err != nil {
		logrus.Error("failed connection -----------------")
		panic(err)
	}

	respChan := util.PollConnection(conn)

	util.WriteMessage(conn, "read", "default:5\n")

	for msg := range respChan {
		logrus.Info(string(msg))
	}
}

func StartClient() {
	conn, err := net.Dial("tcp", ":3030")
	if err != nil {
		logrus.Error("failed connection -----------------")
		panic(err)
	}

	respChan := util.PollConnection(conn)

	util.WriteMessage(conn, "read", "default:5\n")

	for msg := range respChan {
		logrus.Info(string(msg))
	}
}
