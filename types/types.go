package types

import "net"

type StatusResponse struct {
	Success bool
}

type StatusRequest struct {
	Queue string
}

type QueueClient struct {
	Conn         net.Conn
	ConsumesFrom string
}

// maybe the client can customise the parisng of some of the message for auth i.e. replacing these with a flag
var (
	MessageTypeRead             = []byte{'r', 'e', 'a', 'd'}
	MessageTypeWrite            = []byte{'w', 'r', 'i', 't', 'e'}
	MessageTypeStatus           = []byte{'s', 't', 'a', 't', 'u', 's'}
	MessageTypeConsumerHanshake = []byte{'c', 'h', 's'}
	MessageTypeProducerHanshake = []byte{'p', 'h', 's'}
	MessageDelim                = []byte{':'}
	MessageHandshakeOK          = []byte{'o', 'k'}
)
