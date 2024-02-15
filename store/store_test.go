package store

import (
	"fmt"
	"os"
	"testing"
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
