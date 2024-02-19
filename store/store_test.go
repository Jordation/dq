package store

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
	"syreclabs.com/go/faker"
)

func TestStore(t *testing.T) {
	path := "./partition/store"
	ps, _ := NewBasicStore(path)

	defer cleanup(ps.(*basicStore).f, false)
	stat, _ := ps.(*basicStore).f.Stat()
	fmt.Println(stat.Size() / ps.(*basicStore).cfg.fixedEntrySize)

	//	writeShit(ps.(*partitionedStore))

	/* 	mySlice := make([]byte, 512)
	   	ps.ReadAt(mySlice, 15)
	   	fmt.Println(string(mySlice)) */

}

func TestBufioUsage(t *testing.T) {
	fName := "./testdata/teststore"
	f, err := os.OpenFile(fName, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	r := bufio.NewScanner(f)

	f.Close()
	fmt.Printf("r.Scan(): %v\n", r.Scan())
	fmt.Printf("r.Err(): %v\n", r.Err())

	out := r.Bytes()
	spew.Dump(out)

}

func TestPartitionManager(t *testing.T) {
	pm := &partitionManager{
		rootDir: "./testdata/",
		cfg: &partitionedStoreConfig{
			fixedEntrySize: 16,
			maxFileSize:    16,
			testOverride:   true,
		},
	}

	scanner, err := pm.getScannerFor(0)
	if err != nil {
		panic(err)
	}

	for scanner.Scan() {
		fmt.Println(scanner.Text())
		if scanner.Err() != nil {
			logrus.Error(scanner.Err())
		}
	}
	logrus.Error(scanner.Err())
}

func TestFreedom(t *testing.T) {
	go func() {
		srv, err := net.Listen("tcp", ":8080")
		if err != nil {
			panic(err)
		}
		conn, err := srv.Accept()
		if err != nil {
			panic(err)
		}

		betterConR := bufio.NewReaderSize(conn, 2048)
		betterConW := bufio.NewWriterSize(conn, 2048)

		myguy := bufio.NewReadWriter(betterConR, betterConW)
		sizeOfw := unsafe.Sizeof(*myguy.Writer)
		fmt.Printf("size of myguyw: %d\n", sizeOfw)
		sizeOfr := unsafe.Sizeof(*myguy.Reader)
		fmt.Printf("size of myguyr: %d\n", sizeOfr)
		sizeOfint64 := unsafe.Sizeof(int64(0))
		fmt.Printf("size of int64: %d\n", sizeOfint64)
		_ = myguy
	}()

	time.Sleep(time.Millisecond * 50)

	conn2, err := net.Dial("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	conn2.Write([]byte("hello\n"))
	select {}
}

func BenchmarkBufferStuff(b *testing.B) {
	size := 1024 * 128
	nTests := 100_00

	type test struct {
		name string
		fn   func(b *testing.B)
	}

	writeData := faker.Lorem().Sentence(1000)

	benchmarks := []test{
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

	for _, bm := range benchmarks {
		if !b.Run(bm.name, bm.fn) {
			logrus.Error("wtf")
		}
	}

}

func BenchmarkAppendVsAssign(b *testing.B) {
	// over lots of diff arrangements, append won out i.e. arr := make([]T, 0, n) and arr = append(arr, T)
	/* 	benchmarks := []benchmark{
		{
			name: "assign",
			fn: func(b *testing.B) {
				ch := newWriter()
				for range nTests {
					out, _ := getBatch(context.Background(), ch, ARR_SIZE)
					_ = bytes.Join(out, []byte{})
				}
			},
		},
		{
			name: "no pre-alloc",
			fn: func(b *testing.B) {
				ch := newWriter()
				for range nTests {
					out, _ := getBatchWithNoPreAlloc(context.Background(), ch, ARR_SIZE)
					_ = bytes.Join(out, []byte{})
				}
			},
		},
		{
			name: "append",
			fn: func(b *testing.B) {
				ch := newWriter()
				for range nTests {
					out, _ := getBatchWithAppend(context.Background(), ch, ARR_SIZE)
					_ = bytes.Join(out, []byte{})
				}
			},
		},
	} */
}

func cleanup(file *os.File, clear bool) {
	if clear {
		file.Truncate(0)
	}
	file.Close()
}

func writeShit(ps *basicStore) {
	for i := 0; i < 25; i++ {
		ps.Write([]byte(fmt.Sprintf("ENTRY: %d", i)))
	}
}
