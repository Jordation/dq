package util

import (
	"bufio"
	"errors"
	"io"
	"net"
	"time"

	"github.com/sirupsen/logrus"
)

// the caller is responsible for closing the channel
func PollConnection(conn net.Conn) chan []byte {
	connChan := make(chan []byte)

	go func(c net.Conn) {
		defer close(connChan)
		defer conn.Close()

		r := bufio.NewReader(c)
		// TODO: benchmark and test alternate "buffed" readers/writers provided by std lib
		// s := bufio.NewScanner(c)

		for {
			buff, err := r.ReadSlice('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				logrus.Errorf("utils.PollConnection : error reading from conn on queue : %v\n", err)
				return
			}

			if len(buff) == 0 {
				time.Sleep(time.Millisecond * 250)
				continue
			}

			connChan <- buff[:len(buff)-1]
		}

	}(conn)

	return connChan
}
