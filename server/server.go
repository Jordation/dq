package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/Jordation/dqmon/store"
	"github.com/Jordation/dqmon/types"
	"github.com/Jordation/dqmon/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Server struct {
	port string

	stores  map[string]store.Store
	clients []*types.QueueClient
}

func NewServer(port, defaultStorePath string) (*Server, error) {
	Store, err := store.NewBasicStore(defaultStorePath)
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

func (s *Server) handleConnection(conn net.Conn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connChan := util.PollConnection(conn)

	store, queueName, err := s.handleConnectionHandshake(ctx, connChan, conn)
	if err != nil {
		return err
	}

	// handle the status messages
	// also implement them on consoomer...
	//go s.handleConnectionStatusPings(ctx, conn, store)

	for msg := range connChan {
		typeOfMessage, _, content, err := parseMessage(msg)
		if err != nil {
			return err
		}

		if typeOfMessage == "read" {
			offset, err := parseReadMessageContent(content)
			if err != nil {
				return err // flimsy - will kill the consumer
			}

			buf := make([]byte, 1024)

			n, err := store.ReadAt(buf, offset)
			if err != nil && !errors.Is(err, io.EOF) {
				return err // flimsy
			}

			if !errors.Is(err, io.EOF) {
				_, err := fmt.Fprintf(conn, "%d:%s\n", offset+1, buf[:n])
				if err != nil {
					return err // flimsy
				}
			}
		} else if typeOfMessage == "write" {
			if _, err := store.Write(content); err != nil {
				return err // flimsy
			}

			s.notifyConsumers(queueName, content)

		} else {
			logrus.Info("unhandled message type", typeOfMessage)
		}
	}

	logrus.Info("finished reading")
	return nil
}

func (s *Server) handleConnectionHandshake(ctx context.Context, msgChan <-chan []byte, conn net.Conn) (store.Store, string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil, "", fmt.Errorf("handshake failed: timeout")

	case handshakeRequest := <-msgChan:
		msgType, queueName, otherContent, err := parseMessage(handshakeRequest)
		if err != nil {
			return nil, "", err
		}

		logrus.Info("CON MESSAGE: ", string(handshakeRequest))

		store, ok := s.stores[queueName]
		if !ok {
			return nil, "", fmt.Errorf("handshake failed: store not found for queue (%s)", queueName)
		}

		if msgType == "chs" {
			if len(otherContent) == 0 {
				return nil, "", fmt.Errorf("handshake failed: no offset")
			}

			offset, err := strconv.ParseInt(string(otherContent), 0, 64)
			if err != nil {
				return nil, "", errors.Wrap(err, "handshake failed: parsing offset")
			}

			if offset > store.Messages() {
				return nil, "", fmt.Errorf("handshake failed: requested offset > messages")
			}

			_, err = fmt.Fprintf(conn, "%s:%d\n", types.MessageHandshakeOK, offset)
			if err != nil {
				return nil, "", errors.Wrap(err, "handshake failed")
			}

			return store, queueName, nil
		} else if msgType == "phs" {
			return store, queueName, nil
		}

		return nil, queueName, fmt.Errorf("handshake failed: unhandled message type (%s)", msgType)
	}
}

func (s *Server) handleConnectionStatusPings(ctx context.Context, conn net.Conn, store store.Store) {
	errs := 0
	// close the connection if there are too many errors
	// handle errors with an err channel or something,
	//close conns based off some data they hold for when server holds many conns? maybe map lole

	defer func() {
		if err := conn.Close(); err != nil {
			logrus.WithError(err).Error("problem closing connection on handler exit")
		}
	}()

	for {
		select {
		case <-ctx.Done():
		default:
			_, err := fmt.Fprintf(conn, "off:%d", store.Messages())
			if err != nil {
				logrus.WithError(err).Error("couldn't write status message to listner")
				errs++
			}

			if errs >= 3 {
				logrus.WithError(err).Error("closing status loop for handler")
				return
			}

			time.Sleep(time.Second)
		}
	}
}

// type:name:content...
func parseMessage(msg []byte) (string, string, []byte, error) {
	msgType, nameAndContent, found := bytes.Cut(msg, types.MessageDelim)
	if !found {
		return "", "", nil, fmt.Errorf("no separator ':' found in message %s", string(msg))
	}

	queueName, content, err := splitQueueName(nameAndContent)
	if err != nil {
		return "", "", nil, err
	}

	switch true {
	case bytes.Equal(msgType, types.MessageTypeRead):
		return "read", queueName, content, nil

	case bytes.Equal(msgType, types.MessageTypeWrite):
		return "write", queueName, content, nil

	case bytes.Equal(msgType, types.MessageTypeStatus):
		return "status", queueName, content, nil

	case bytes.Equal(msgType, types.MessageTypeConsumerHanshake):
		return "chs", queueName, content, nil

	case bytes.Equal(msgType, types.MessageTypeProducerHanshake):
		return "phs", queueName, content, nil
	}

	return string(msgType), queueName, nil, nil
}

func parseReadMessageContent(msg []byte) (int64, error) {
	offset, err := strconv.ParseInt(string(msg), 0, 64)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

//queuename:type:content
func splitQueueName(nameAndContent []byte) (string, []byte, error) {
	queueNameBytes, otherBytes, _ := bytes.Cut(nameAndContent, types.MessageDelim)
	/* 	if !found {
		return "", nil, fmt.Errorf("no separator found in name and content (%s)", string(nameAndContent))
	} */

	return string(queueNameBytes), otherBytes, nil
}

func (s *Server) notifyConsumers(queueName string, msg []byte) {
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
