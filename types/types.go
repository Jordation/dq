package types

import "net"

type StatusResponse struct {
	Success bool
}

type StatusRequest struct {
	Queue string
}

type Connection struct {
	Conn         net.Conn
	ConsumesFrom string
}

var (
	MessageTypeRead  = []byte{'r', 'e', 'a', 'd'}
	MessageTypeWrite = []byte{'w', 'r', 'i', 't', 'e'}
)
