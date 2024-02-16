package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/Jordation/dqmon/store"
	"github.com/Jordation/dqmon/types"
	"github.com/Jordation/dqmon/util"
	"github.com/sirupsen/logrus"
)

type Server struct {
	port string

	stores  map[string]store.Store
	clients []*types.QueueClient
}

func NewServer(port, defaultStorePath string) (*Server, error) {
	Store, err := store.NewPartionedStore(defaultStorePath, 1024)
	if err != nil {
		return nil, err
	}

	return &Server{
		stores: map[string]store.Store{"default": Store},
		port:   port,
	}, nil
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		logrus.Info("accepted listener")

		go func(c net.Conn) {
			s.clients = append(s.clients, &types.QueueClient{
				Conn:         c,
				ConsumesFrom: "default",
			})

			if err := s.handleConnection(c); err != nil {
				logrus.WithError(err).Error("it's fucked closed conn")
			}
		}(conn)
	}
}

// errors will terminate the connection
func (s *Server) handleConnection(conn net.Conn) error {
	connChan := util.PollConnection(conn)
	defer func() {
		if err := conn.Close(); err != nil {
			logrus.WithError(err).Error("problem closing connection on handler exit")
		}
	}()

	for msg := range connChan {
		typeOfMessage, queueName, content, err := parseMessage(msg)
		if err != nil {
			return err
		}

		store, ok := s.stores[queueName]
		if !ok {
			return errors.New("store not found for queue : " + queueName)
		}

		switch typeOfMessage {
		case "read":
			offset, err := parseReadMessage(content)
			if err != nil {
				return err
			}

			buf := make([]byte, 1024)

			n, err := store.ReadAt(buf, offset)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}

			if !errors.Is(err, io.EOF) {
				_, err = fmt.Fprintf(conn, "%d:%s\n", offset+1, buf[:n])
				if err != nil {
					return err
				}
			}

		case "write":
			// TODO: maybe swap the order around and change consumers to try consume n times for an offset before failing?
			//i.e. they all start asking for the new message and get it when the write completes

			if _, err := store.Write(content); err != nil {
				logrus.Error()
				return err
			}

			s.NotifyConsumers(queueName, content)

		default:
			logrus.Info("unhandled message type", typeOfMessage)
		}
	}

	logrus.Info("finished reading")
	return nil
}

// returns message type and message content
func parseMessage(msg []byte) (string, string, []byte, error) {
	msgType, nameAndContent, found := bytes.Cut(msg, []byte{':'})
	if !found {
		return "", "", nil, errors.New("no separator ':' found in message " + string(msg))
	}

	queueName, content, err := splitQueueName(nameAndContent)
	if err != nil {
		return "", "", nil, err
	}

	if bytes.Equal(msgType, types.MessageTypeRead) {
		return "read", queueName, content, nil
	} else if bytes.Equal(msgType, types.MessageTypeWrite) {
		return "write", queueName, content, nil
	}

	return string(msgType), queueName, nil, nil
}

// returns offset to read at
func parseReadMessage(msg []byte) (int64, error) {
	offset, err := strconv.ParseInt(string(msg), 0, 64)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func splitQueueName(nameAndContent []byte) (string, []byte, error) {
	queueNameBytes, otherBytes, found := bytes.Cut(nameAndContent, []byte{':'})
	if !found {
		return "", nil, errors.New("no separator ':' found in name and content" + string(nameAndContent))
	}

	return string(queueNameBytes), otherBytes, nil
}

func (s *Server) NotifyConsumers(queueName string, msg []byte) {
	// notify the consumers of queue that ther are new messages available
	for _, c := range s.clients {
		fmt.Fprintf(os.Stdout, "checking %v\n", c.ConsumesFrom)
		if c.ConsumesFrom == queueName {

			fmt.Fprintf(os.Stdout, "matched %v, writing %s\n", c.ConsumesFrom, msg)

			_, err := fmt.Fprintf(c.Conn, "%v:%s\n", s.stores[queueName].Messages(), msg)
			if err != nil {
				logrus.WithError(err).Errorf("can't notify consumer of %v", queueName)
			}
		}
	}
}
