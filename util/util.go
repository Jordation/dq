package util

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
)

func PollConnection(conn net.Conn) chan []byte {
	connChan := make(chan []byte)
	r := bufio.NewReader(conn)

	go func(c net.Conn) {
		for {
			buff, err := r.ReadBytes('\n')
			if err != nil {
				fmt.Printf("error reading from conn on queue: %v\n", err)
			}

			if len(buff) > 0 {
				logrus.Info("read", len(buff), "bytes")
				connChan <- buff
			} else {
				time.Sleep(time.Millisecond * 250)
			}
		}
	}(conn)

	return connChan
}
