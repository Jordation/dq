package util

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// the caller is responsible for closing the channel
func PollConnection(conn net.Conn) chan []byte {
	connChan := make(chan []byte)

	go func(c net.Conn) {
		r := bufio.NewReader(c)

		// TODO: benchmark and test alternate "buffed" readers/writers provided by std lib
		// s := bufio.NewScanner(c)

		for {
			buff, err := r.ReadBytes('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				fmt.Printf("utils.PollConnection : error reading from conn on queue : %v\n", err)
			}

			if len(buff) > 0 {
				connChan <- buff[:len(buff)-1]
			} else {
				time.Sleep(time.Millisecond * 250)
			}
		}

	}(conn)

	return connChan
}
