package types

import (
	"context"
)

type Server interface {
	Start(ctx context.Context) error
}

type Producer interface {
	Write(ctx context.Context, p []byte) (int, error)
	Start(ctx context.Context) error
}

type Consumer interface {
	Consume(ctx context.Context) (<-chan []byte, error)
}

// maybe the client can customise the parisng of some of the message for auth i.e. replacing these with a flag
var (
	MessageTypeRead             = []byte{'r', 'e', 'a', 'd'}
	MessageTypeWrite            = []byte{'w', 'r', 'i', 't', 'e'}
	MessageTypeStatus           = []byte{'s', 't', 'a', 't', 'u', 's'}
	MessageTypeConsumerHanshake = []byte{'c', 'h', 's'}
	MessageTypeProducerHanshake = []byte{'p', 'h', 's'}

	MessageDelim = []byte{':'}

	MessageOk  = []byte{'o', 'k'}
	MessageNok = []byte{'n', 'o', 'k'}

	MessageBatchBegin = []byte{'b', 'e', 'g', 'i', 'n'}
	MessageBatchEnd   = []byte{'e', 'n', 'd'}
)
