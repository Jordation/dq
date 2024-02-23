package util

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/sirupsen/logrus"
)

// the caller is responsible for closing the channel
func PollConnection(conn net.Conn) chan []byte {
	connChan := make(chan []byte, 2)

	go func(c net.Conn) {
		defer close(connChan)

		r := bufio.NewReader(c)
		// TODO: benchmark and test alternate "buffed" readers/writers provided by std lib
		// s := bufio.NewScanner(c)

		for {
			buff, err := r.ReadSlice('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				logrus.Errorf("utils.PollConnection : error reading from conn on queue : %v\n", err)
				return
			}

			buff = bytes.TrimSuffix(buff, []byte("\n"))

			if len(buff) == 0 {
				logrus.Info("empty read")
				time.Sleep(time.Millisecond * 250)
				continue
			}

			fmt.Printf("sending (%s)\n", buff)

			connChan <- buff
		}

	}(conn)

	return connChan
}
