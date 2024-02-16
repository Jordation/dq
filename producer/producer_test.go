package producer

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	bslice := []byte{'h', 'e', 'l', 'l', 'o'}

	srv, err := net.Listen("tcp", ":3030")
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(time.Second)
		conn, err := net.Dial("tcp", ":3030")
		if err != nil {
			panic(err)
		}

		out := make([]byte, 100)
		conn.Read(out)
		fmt.Println(string(out))
	}()

	for {
		c, _ := srv.Accept()
		fmt.Fprintf(c, "write:%s\n", bslice)
	}
}
