package store

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"syreclabs.com/go/faker"
)

func TestStore(t *testing.T) {
	path := "./partition/store"
	ps, _ := NewPartionedStore(path, 1024)
	defer cleanup(ps.(*partitionedStore).f, false)
	stat, _ := ps.(*partitionedStore).f.Stat()
	fmt.Println(stat.Size() / ps.(*partitionedStore).entrySize)

	//	writeShit(ps.(*partitionedStore))

	/* 	mySlice := make([]byte, 512)
	   	ps.ReadAt(mySlice, 15)
	   	fmt.Println(string(mySlice)) */

}

func BenchmarkBufferStuff(b *testing.B) {
	size := 1024 * 128
	nTests := 100_00

	type test struct {
		name string
		fn   func(b *testing.B)
	}

	writeData := faker.Lorem().Sentence(1000)

	tests := []test{
		{
			name: "buffer used",
			fn: func(b *testing.B) {
				buf := make([]byte, size)
				buffed := bytes.NewBuffer(buf)
				for range nTests {
					buffed.Write([]byte(writeData))
					_, _ = buffed.Read(nil)
					buffed.Reset()
				}
			},
		},
		{
			name: "new slice each time",
			fn: func(b *testing.B) {
				for range nTests {
					buf := make([]byte, size)
					copy(buf, []byte(writeData))
					lenData := len(writeData)
					_ = buf[:lenData]
				}
			},
		},
	}

	for _, test := range tests {
		if !b.Run(test.name, test.fn) {
			logrus.Error("wtf")
		}
	}

}

func cleanup(file *os.File, clear bool) {
	if clear {
		file.Truncate(0)
	}
	file.Close()
}

func writeShit(ps *partitionedStore) {
	for i := 0; i < 25; i++ {
		ps.Write([]byte(fmt.Sprintf("ENTRY: %d", i)))
	}
}
