package v2

import (
	"context"
	"net"

	"github.com/Jordation/dqmon/store"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func StartServer(ctx context.Context, defaultStore store.Store) error {
	srv, err := net.Listen("tcp", ":3030")
	if err != nil {
		return err
	}

	for {
		clientConn, err := srv.Accept()
		if err != nil {
			return errors.Wrapf(err, "failed to accept connection")
		}

		go handleConn(ctx, clientConn, defaultStore)
	}
}

func handleConn(ctx context.Context, conn net.Conn, store store.Store) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()

	client := newConnWrapper(ctx, conn)

	handshakeBytes := <-client.messageChannel
	handshake, err := Unmarshal[ClientHandshakeRequest](handshakeBytes)
	if err != nil {
		logrus.Errorf("failed to unmarshal handshake: %s %s", err, handshakeBytes)
		return
	}

	if handshake.StartOffset > store.Messages() {
		logrus.Errorf("invalid start offset: %d", handshake.StartOffset)
		return
	}

	hsResp := &HandshakeResponse{Ok: true, Message: "success"}
	if err := client.Write(ctx, hsResp); err != nil {
		logrus.Errorf("failed to write handshake response: %s", err)
		return
	}
	logrus.Info("handshake successful")

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-client.messageChannel:
			req, err := Unmarshal[ConsumerRequest](msg)
			if err != nil {
				logrus.Errorf("failed to unmarshal request: %s", err)
				return
			}

			if req.Offset > store.Messages() {
				client.Write(ctx, &ConsumerResponse{Ok: true, Message: "finished consuming"})
				continue
			}

			storeResp := make([]byte, 1024*req.BatchSize)
			lenbatch, lenData, err := store.ReadAtWithCount(storeResp, req.Offset, req.BatchSize)
			if err != nil {
				logrus.Errorf("failed to read from store: %s", err)
				return
			}

			resp := &ConsumerResponse{
				Ok:        true,
				Message:   "Success",
				Data:      string(storeResp[:lenData]),
				BatchSize: int64(lenbatch),
			}
			if err := client.Write(ctx, resp); err != nil {
				logrus.Errorf("failed to write response: %s", err)
				return
			}
		}
	}
}
