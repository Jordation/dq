package producer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestProducer(t *testing.T) {
	bslice := []byte("hello")

	buff := bytes.NewBuffer([]byte{})
	fmt.Fprintf(buff, "write:%s", bslice)

	spew.Dump(buff.Bytes())
	fmt.Println(buff.Bytes())
}
