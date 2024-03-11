package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type partitionManager struct {
	cfg        *partitionedStoreConfig
	rootDir    string
	writeQueue chan []byte
	file       *os.File
	message    *atomic.Int64
}

type partitionedStoreConfig struct {
	entrySize      int64
	entriesPerFile int64 // calculated as n * fixedEntrySize
	batchTimeout   time.Duration
	batchSize      int64
	testOverride   bool
}

var defaultConfig = &partitionedStoreConfig{
	entrySize:      1024, // 128 kb per entry
	entriesPerFile: 16,   // 16 entries per file
	batchTimeout:   time.Second * 2,
	batchSize:      4,
}

func NewPartitionedStore(rootDir string) (Store, error) {
	/* 	if !strings.HasSuffix(rootDir, "/") {
		rootDir = rootDir + "/"
	} */

	file, err := os.OpenFile(rootDir, os.O_RDONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	p := &partitionManager{
		rootDir:    rootDir,
		cfg:        defaultConfig,
		writeQueue: make(chan []byte, 16),
		message:    &atomic.Int64{},
		file:       file,
	}

	stat, err := p.file.Stat()
	if err != nil {
		return nil, err
	}

	p.message.Add(stat.Size() / p.cfg.entrySize)

	go p.startBatchWrites()

	return p, nil
}

func (pm *partitionManager) ReadAt(dest []byte, offset int64) (int, error) {
	stat, err := pm.file.Stat()
	if err != nil {
		return 0, err
	}

	res := make([]byte, pm.cfg.entrySize)

	readAt := (stat.Size() / pm.cfg.entrySize) - offset
	if _, err := pm.file.ReadAt(res, readAt*pm.cfg.entrySize); err != nil {
		return 0, err
	}

	copy(dest, res)

	return bytes.Index(dest, []byte{0}), nil
}

func (pm *partitionManager) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, fmt.Errorf("should not write empty message")
	}

	if len(pm.writeQueue) == cap(pm.writeQueue) {
		return 0, fmt.Errorf("write queue is full")
	}

	// validation could live here

	pm.writeQueue <- p

	return len(p), nil
}

func (pm *partitionManager) Cleanup() error {
	panic("not implemented") // TODO: Implement
}

func (pm *partitionManager) Messages() int64 {
	return pm.message.Load()
}

func (pm *partitionManager) ReadAtWithCount(dest []byte, offset int64, count int64) (lenBatch int, lenData int, err error) {
	if offset < 0 {
		return 0, 0, fmt.Errorf("shouldn't read store if no offset")
	}

	collection := make([][]byte, 0, count)

	for i := range count {
		thisBuf := make([]byte, pm.cfg.entrySize)

		n, err := pm.ReadAt(thisBuf, offset+i)
		if err != nil {
			return 0, 0, err
		}

		thisBuf = thisBuf[:n]

		collection = append(collection, thisBuf)
	}

	combined := bytes.Join(collection, []byte{0})

	copy(dest, combined)

	return int(count), len(combined), nil

	/* 	msgs := pm.Messages()
	   	if offset+count > msgs {
	   		if offset == msgs {
	   			copy(dest, pm.fakeData[offset])
	   			return 1, len(pm.fakeData[offset]), nil
	   		}

	   		for i := range count {
	   			if offset+i >= pm.Messages()-1 {
	   				out := bytes.Join(pm.fakeData[offset:offset+i], []byte{0})
	   				copy(dest, out)
	   				return int(i) + 1, len(out), nil
	   			}
	   		}
	   	}

	   	out := bytes.Join(pm.fakeData[offset:offset+count], []byte{0})
	   	copy(dest, out)
	   	return int(count), len(out), nil */
}

func (pm *partitionManager) write(entries [][]byte) (int, error) {
	// check current file size
	// write all entries if there is space
	// if not space, calculate how many can fit before next file needed
	// write to current file, then next file, then next file, etc
	newEntries := len(entries)

	for i, entry := range entries {
		if int64(len(entry)) > pm.cfg.entrySize { // woulld want to validate this sooner than the write stage
			return i, fmt.Errorf("line longer than max entry len")
		}

		padded := make([]byte, pm.cfg.entrySize)
		copy(padded, entry)

		_, err := pm.file.Write(padded)
		if err != nil {
			logrus.WithError(err).Error("error writing to file")
			return i, err
		}
		pm.message.Add(1)
	}

	return newEntries, nil
}

func (pm *partitionManager) startBatchWrites() {
	for {
		batchSize, err := pm.batchWrite()
		if err != nil {
			logrus.WithError(err).Error("error writing batch")
			continue
		} else if batchSize == 0 {
			continue
		}
		logrus.Infof("wrote batch of %d", batchSize)
	}
}

func (pm *partitionManager) batchWrite() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pm.cfg.batchTimeout)
	defer cancel()

	msgs, batchSize := getBatch(ctx, pm.writeQueue, pm.cfg.batchSize)

	if _, err := pm.write(msgs); err != nil {
		return batchSize, err
		// handle err gracefully at some point,  maybe place the messages back on the queue?
		// this will bleed into needing a way to write in not just appends, will need to know the offset of lost msgs
		// or halt the write loop but that could block if its a data problem so pref avoiding that
		// separate retry queue ? cringe
	}

	return batchSize, nil
}

// blocks for the timeout of ctx or after n messages
func getBatch[T any](ctx context.Context, src chan T, n int64) ([]T, int64) {
	msgs := make([]T, 0, n)

	for i := int64(0); i < n; i++ {
		select {
		case <-ctx.Done():
			return msgs, i
		case msg := <-src:
			msgs = append(msgs, msg)
		}
	}

	return msgs, n
}
