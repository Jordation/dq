package util

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

func PollConnection(conn net.Conn) chan []byte {
	connChan := make(chan []byte)
	r := bufio.NewReader(conn)

	go func() {
		for {
			buff, err := r.ReadBytes('\n')
			if err != nil {
				fmt.Printf("error reading from conn on queue: %v\n", err)
			}

			if len(buff) > 0 {
				connChan <- buff
			} else {
				time.Sleep(time.Millisecond * 250)
			}
		}
	}()

	return connChan
}
