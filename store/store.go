package store

import (
	"bytes"
	"errors"
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type Store interface {
	ReadAt([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Cleanup() error
	Messages() int64
}

type partitionedStore struct {
	f         *os.File
	maxOffset *atomic.Int64
	entrySize int64
}

func NewPartionedStore(filePath string, entrySize int64) (Store, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fella := &atomic.Int64{}
	if stat.Size() > 0 {
		fella.Add(stat.Size() / entrySize)
	}

	return &partitionedStore{
		f:         file,
		entrySize: entrySize,
		maxOffset: fella,
	}, nil
}

func (ps *partitionedStore) Write(p []byte) (int, error) {
	if int64(len(p)) > ps.entrySize {
		return 0, errors.New("line longer than max entry len")
	}

	padded := make([]byte, ps.entrySize)
	copy(padded, p)

	_, err := ps.f.Write(padded)
	if err != nil {
		logrus.WithError(err).Error("error writing to file")
		return 0, err
	}

	ps.maxOffset.Add(1)

	return len(p), nil
}

func (ps *partitionedStore) ReadAt(p []byte, off int64) (int, error) {
	n, err := ps.f.ReadAt(p, off*ps.entrySize)
	if err != nil {
		return n, err
	}

	n = bytes.Index(p, []byte{0})

	return n, nil
}

func (ps *partitionedStore) Cleanup() error  { return nil }
func (ps *partitionedStore) Messages() int64 { return ps.maxOffset.Load() }
