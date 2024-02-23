package v2

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"

	"github.com/Jordation/dqmon/util"
)

// FINE i'll use json for now.. speed later this byte parsing shit is slowing me down
type ClientHandshakeRequest struct {
	Type             string // Client type
	QueueName        string // Client queue target
	StartOffset      int64  // only present on consumer requests
	RequestChunkSize int    // only present on consumer requests
}

type HandshakeResponse struct {
	Ok      bool
	Message string
}

type ConsumerRequest struct {
	Offset    int64
	BatchSize int64
}

type ConsumerResponse struct {
	Ok      bool
	Message string

	Data      string //json string
	BatchSize int64
}

type ProducerRequest struct {
	QueueName string
	Data      string
}

type ProducerResponse struct {
	Ok      bool
	Message string
}

func Unmarshal[T any](data []byte) (*T, error) {
	out := new(T)

	if err := json.Unmarshal(data, out); err != nil {
		return nil, err
	}

	return out, nil
}

func newConnWrapper(ctx context.Context, conn net.Conn) *connWrapper {
	wrapper := &connWrapper{}

	wrapper.writer = bufio.NewWriter(conn)
	wrapper.messageChannel = util.PollConnection(conn)
	wrapper.ctx = ctx

	return wrapper
}

type connWrapper struct {
	messageChannel chan []byte
	writer         *bufio.Writer
	ctx            context.Context
}

// not great. but greatly convenient
func (w *connWrapper) Write(_ context.Context, input any) error {
	var data []byte

	_, ok := input.([]byte)
	if ok {
		data = input.([]byte)
	} else {
		dataHere, err := json.Marshal(input)
		if err != nil {
			return err
		}

		data = dataHere
	}

	if !bytes.HasSuffix(data, []byte("\n")) {
		data = append(data, '\n')
	}

	w.writer.Write(data)

	return w.writer.Flush()
}
