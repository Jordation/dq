package server

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/Jordation/dqmon/store"
	"github.com/Jordation/dqmon/types"
	"github.com/Jordation/dqmon/util"
	"github.com/sirupsen/logrus"
)

type Server struct {
	Stores    map[string]store.Store
	Consumers []*types.Connection
}

func NewServer() (*Server, error) {
	Store, _ := store.NewPartionedStore("./store/partition/store")
	return &Server{
		Stores: map[string]store.Store{"default": Store},
	}, nil
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", ":3030")
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		logrus.Info("accepted listener")

		s.Consumers = append(s.Consumers, &types.Connection{
			Conn:         conn,
			ConsumesFrom: "default",
		})

		go func(c net.Conn) {
			if err := s.handleConnection(c); err != nil {
				logrus.Error("it's fucked", err)
			}
		}(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) error {
	connChan := util.PollConnection(conn)

	for msg := range connChan {
		typeOfMessage, content := parseMessage(msg)
		switch typeOfMessage {

		case "read":
			queueName, offset := parseReadMessage(content)
			store := s.Stores[queueName]

			/* 			if store.Messages() < offset {
				logrus.Info("offset greater than message count")
				return nil
			} */

			buf := make([]byte, 1024)

			n, err := store.ReadAt(buf, offset)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(conn, "%d:%v\n", offset+1, string(buf[:n]))
			if err != nil {
				return err
			}

		case "write":
			queueName, msg := parseWriteMessage(content)
			_, _ = s.Stores[queueName].Write(msg)

			s.NotifyConsumers(queueName, msg)

		default:
			logrus.Info("unhandled message type", typeOfMessage)
		}
	}

	logrus.Info("finished reading")
	return nil
}

// returns message type and message content
func parseMessage(msg []byte) (string, [][]byte) {
	chunks := bytes.Split(msg, []byte{':'})
	if len(chunks) < 2 {
		logrus.Error("failed to parse message", msg)
		return "", nil
	}

	if bytes.Equal(chunks[0], types.MessageTypeRead) {
		return "read", chunks[1:]
	}

	if bytes.Equal(chunks[0], types.MessageTypeWrite) {
		return "write", chunks[1:]
	}

	return "", nil
}

// returns queue name and offset to read at
func parseReadMessage(msg [][]byte) (string, int64) {
	offset, err := strconv.ParseInt(strings.TrimRight(string(msg[1]), "\n"), 0, 64)
	if err != nil {
		logrus.Error("failed to parse offset for read message")
		return "", 0
	}

	return string(msg[0]), offset
}

func parseWriteMessage(msg [][]byte) (string, []byte) {
	if len(msg) < 2 {
		logrus.Fatal("malformed write message: ", msg)
	}
	return string(msg[0]), msg[1]
}

func (s *Server) NotifyConsumers(queueName string, msg []byte) {
	// notify the consumers of queue that ther are new messages available
	for _, c := range s.Consumers {
		if c.ConsumesFrom == queueName {
			_, err := c.Conn.Write(msg)
			if err != nil {
				logrus.WithError(err).Errorf("can't notify consumer of %v", queueName)
				continue
			}
		}
	}
}
