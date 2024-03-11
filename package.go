package godq

import (
	"time"

	"google.golang.org/protobuf/proto"
)

//go:generate protoc -I. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative goqueue.proto

func (mb *MessageBatch) AddMessage(data []byte) {
	ts := time.Now().UnixNano()

	msg := &Message{
		Timestamp: ts,
		Data:      data,
	}
	msgB, _ := proto.Marshal(msg)

	mb.Messages = append(mb.Messages, msgB)
	mb.LastTimestamp = ts
	mb.OffsetDelta += 1
}
