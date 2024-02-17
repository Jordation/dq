package store

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type partitionManager struct {
	cfg     *partitionedStoreConfig
	rootDir string
}

type partitionedStoreConfig struct {
	fixedEntrySize int64
	maxFileSize    int64 // calculated as n * fixedEntrySize
	testOverride   bool
}

var defaultConfig = &partitionedStoreConfig{
	fixedEntrySize: 128, // 128 kb per entry
	maxFileSize:    16,  // 16 entries per file
}

func NewPartitionedStore(rootDir string) (Store, error) {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir = rootDir + "/"
	}

	return &partitionManager{
		rootDir: rootDir,
		cfg:     defaultConfig,
	}, nil
}

func (pm *partitionManager) ReadAt(_ []byte, _ int64) (int, error) {
	panic("not implemented") // TODO: Implement
}

func (pm *partitionManager) Write(_ []byte) (int, error) {
	panic("not implemented") // TODO: Implement
}

func (pm *partitionManager) Cleanup() error {
	panic("not implemented") // TODO: Implement
}

func (pm *partitionManager) Messages() int64 {
	panic("not implemented") // TODO: Implement
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
