package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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
	clients []*queueClient
}

type queueClient struct {
	conn       net.Conn
	store      store.Store
	clientType string
}

func NewServer(port, defaultStorePath string, storeConfigs ...store.StoreConfig) (*Server, error) {
	stores := map[string]store.Store{}

	for _, cfg := range storeConfigs {
		st, err := store.NewBasicStore(cfg.BasePath)
		if err != nil {
			return nil, errors.Wrap(err, "("+cfg.BasePath+") : ")
		}

		stores[cfg.KeyName] = st
	}

	return &Server{
		stores: stores,
		port:   port,
	}, nil
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			conn, err := ln.Accept()
			if err != nil {
				return err
			}

			logrus.Info("accepted listener")

			go func(c net.Conn) {
				thisClient := &queueClient{
					conn: c,
				}
				s.clients = append(s.clients, thisClient)

				if err := s.handleConnection(thisClient); err != nil {
					logrus.WithError(err).Error("it's fucked closed conn")
				}
			}(conn)
		}
	}
}

func (s *Server) handleConnection(client *queueClient) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connChan := util.PollConnection(client.conn)

	store, clientType, err := s.handleConnectionHandshake(ctx, connChan, client.conn)
	if err != nil {
		return err
	}

	client.store = store
	client.clientType = clientType

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
				_, err := fmt.Fprintf(client.conn, "%d:%s\n", offset+1, buf[:n])
				if err != nil {
					return err // flimsy
				}
			}
			continue
		}

		if typeOfMessage == "write" {
			if _, err := store.Write(content); err != nil {
				return err // flimsy
			}

			s.notifyConsumers(content)
			continue
		}

		logrus.Info("unhandled message type", typeOfMessage)
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

			_, err = fmt.Fprintf(conn, "%s:%d\n", types.MessageOk, offset)
			if err != nil {
				return nil, "consumer", errors.Wrap(err, "handshake failed")
			}

			return store, queueName, nil
		} else if msgType == "phs" {
			_, err = fmt.Fprintf(conn, "%s\n", types.MessageOk)
			if err != nil {
				return nil, "", errors.Wrap(err, "handshake failed")
			}

			return store, "producer", nil
		}

		return nil, "", fmt.Errorf("handshake failed: unhandled message type (%s)", msgType)
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

func (s *Server) notifyConsumers(msg []byte) {
	for _, c := range s.clients {
		if c.clientType != "consumer" {
			continue
		}

		if _, err := fmt.Fprintf(c.conn, "%v:%s\n", c.store.Messages(), msg); err != nil {
			logrus.WithError(err).Errorf("couldn't notify consumer of new message")
			// handle errors with some sort of retry / backoff scheme (v2)
		}
	}
}
