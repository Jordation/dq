package store

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type partitionManager struct {
	cfg        *partitionedStoreConfig
	rootDir    string
	writeQueue chan []byte
	fakeData   [][]byte
}

type partitionedStoreConfig struct {
	fixedEntrySize int64
	maxFileSize    int64 // calculated as n * fixedEntrySize
	batchTimeout   time.Duration
	batchSize      int
	testOverride   bool
}

var defaultConfig = &partitionedStoreConfig{
	fixedEntrySize: 128, // 128 kb per entry
	maxFileSize:    16,  // 16 entries per file
	batchTimeout:   time.Second,
	batchSize:      4,
}

func NewPartitionedStore(rootDir string) (Store, error) {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir = rootDir + "/"
	}

	p := &partitionManager{
		rootDir:    rootDir,
		cfg:        defaultConfig,
		writeQueue: make(chan []byte, 16),
		fakeData:   getDummyData(),
	}

	//go p.startBatchWrites()
	return p, nil
}

func getDummyData() [][]byte {
	res := make([][]byte, 0, 50)
	for i := 0; i < 50; i++ {
		res = append(res, []byte("data num: ("+fmt.Sprint(i)+")"))
	}
	return res
}

func (pm *partitionManager) ReadAt(_ []byte, _ int64) (int, error) {
	panic("not implemented") // TODO: Implement
}

func (pm *partitionManager) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, fmt.Errorf("should not write empty message")
	}

	// validation could live here

	pm.writeQueue <- p

	return len(p), nil
}

func (pm *partitionManager) Cleanup() error {
	panic("not implemented") // TODO: Implement
}

func (pm *partitionManager) Messages() int64 {
	return int64(len(pm.fakeData))
}

func (pm *partitionManager) ReadAtWithCount(dest []byte, offset int64, count int64) (int, error) {
	out := bytes.Join(pm.fakeData[offset:offset+count], []byte{'\n'})
	n := copy(dest, out)
	return n, nil
}

func (pm *partitionManager) getScannerFor(offset int64) (*bufio.Scanner, error) {
	var fName string
	fNum := offset / pm.cfg.maxFileSize

	if pm.cfg.testOverride {
		fName = pm.rootDir + "teststore"
	} else {
		fName = pm.rootDir + fmt.Sprint(fNum)
	}

	f, err := os.OpenFile(fName, os.O_RDONLY, 0444)
	if err != nil {
		return nil, err
	}

	s := bufio.NewScanner(f)
	s.Split(pm.splitFunc)

	return s, nil
}

func (pm *partitionManager) splitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	return int(pm.cfg.fixedEntrySize), data[:pm.cfg.fixedEntrySize], nil
}

func (pm *partitionManager) startBatchWrites() {
	for {
		n, batchSize, err := pm.batchWrite()
		if err != nil {
			logrus.WithError(err).Error("error writing batch")
			continue
		}
		logrus.Infof("wrote %d bytes batch of %d", n, batchSize)
	}
}

func (pm *partitionManager) batchWrite() (int, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pm.cfg.batchTimeout)
	defer cancel()

	msgs, batchSize := getBatch(ctx, pm.writeQueue, pm.cfg.batchSize)

	n, err := pm.Write(bytes.Join(msgs, []byte{'\n'}))
	if err != nil {
		return n, batchSize, err
		// handle err gracefully at some point,  maybe place the messages back on the queue?
		// this will bleed into needing a way to write in not just appends, will need to know the offset of lost msgs
		// or halt the write loop but that could block if its a data problem so pref avoiding that
		// separate retry queue ? cringe
	}

	return n, batchSize, nil
}

// blocks for the timeout of ctx or after n messages
func getBatch[T any](ctx context.Context, src chan T, n int) ([]T, int) {
	msgs := make([]T, 0, n)

	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return msgs, i
		case msg := <-src:
			msgs = append(msgs, msg)
		}
	}

	return msgs, n
}
