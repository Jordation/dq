package store

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type Store interface {
	ReadAt([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Cleanup() error
	Messages() int64
	ReadAtWithCount([]byte, int64, int64) (int, error)
}

type basicStore struct {
	f         *os.File
	maxOffset *atomic.Int64
	cfg       *partitionedStoreConfig
}

type StoreConfig struct {
	BasePath, KeyName string
}

func NewBasicStore(filePath string) (Store, error) {
	cfg := &partitionedStoreConfig{
		fixedEntrySize: 1024,
	}

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
		fella.Add(stat.Size() / cfg.fixedEntrySize)
	}

	return &basicStore{
		f:         file,
		cfg:       cfg,
		maxOffset: fella,
	}, nil
}

func (ps *basicStore) Write(p []byte) (int, error) {
	if int64(len(p)) > ps.cfg.fixedEntrySize {
		return 0, fmt.Errorf("line longer than max entry len")
	}

	padded := make([]byte, ps.cfg.fixedEntrySize)
	copy(padded, p)

	_, err := ps.f.Write(padded)
	if err != nil {
		logrus.WithError(err).Error("error writing to file")
		return 0, err
	}

	ps.maxOffset.Add(1)

	return len(p), nil
}

func (ps *basicStore) ReadAt(p []byte, off int64) (int, error) {
	n, err := ps.f.ReadAt(p, off*ps.cfg.fixedEntrySize)
	if err != nil {
		return n, err
	}

	n = bytes.Index(p, []byte{0})

	return n, nil
}

func (ps *basicStore) Cleanup() error  { return nil }
func (ps *basicStore) Messages() int64 { return ps.maxOffset.Load() }

func (ps *basicStore) ReadAtWithCount([]byte, int64, int64) (int, error) { panic("unimplemented") }
