package store

import (
	"errors"
	"os"

	"github.com/sirupsen/logrus"
)

type Store interface {
	ReadAt([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Cleanup() error
	Messages() int64
}

type partitionedStore struct {
	f              *os.File
	maxOffset      int64
	maxEntryLength int
}

func NewPartionedStore(filePath string) (Store, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &partitionedStore{
		f:              file,
		maxEntryLength: 1024,
	}, nil
}

func (ps *partitionedStore) Write(p []byte) (int, error) {
	if len(p) > ps.maxEntryLength {
		return 0, errors.New("line longer than max entry len")
	}

	padded := make([]byte, ps.maxEntryLength)
	copy(padded, p)

	_, err := ps.f.Write(padded)
	if err != nil {
		logrus.WithError(err).Error("error writing to file")
		return 0, err
	}

	ps.maxOffset += 1

	return len(p), nil
}

func (ps *partitionedStore) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	_, err := ps.f.ReadAt(p, off*int64(ps.maxEntryLength))
	if err != nil {
		return n, err
	}

	for _, b := range p {
		if b == 0 {
			break
		}
		n++
	}

	return n, nil
}

func (ps *partitionedStore) Cleanup() error  { return nil }
func (ps *partitionedStore) Messages() int64 { return ps.maxOffset }

/* 			[]byte(fmt.Sprintf(
	`{"name": "%v", "birthday": "%v", "address": "%v"}%v`,
	faker.Name(),
	faker.Date().Birthday(18, 80),
	faker.Address(),
	"\n",
))) */
