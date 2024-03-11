package store

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/Jordation/godq"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Typically there is a cluster designated per queue
type Cluster struct {
	partitions []*mockPartition
	queueLogs  *LogStore
}

type LogStore struct{}

type mockPartition struct {
	identity    uuid.UUID
	Close       func()
	ID          string
	batchBuffer *godq.MessageBatch
	f           *os.File
}

func NewMockPartition() (*mockPartition, error) {
	m := &mockPartition{}

	randomID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	m.identity = randomID

	tmpDir := RelPath("../data/tmp/" + randomID.String() + "/")
	err = os.MkdirAll(tmpDir, 0755)
	if err != nil {
		return nil, err
	}

	dataFile, err := os.OpenFile(tmpDir+"/1", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	m.f = dataFile

	m.Close = func() {
		m.f.Close()
		os.RemoveAll(tmpDir)
	}

	return m, nil
}

func (p *mockPartition) Push(msg []byte) {
	p.batchBuffer.AddMessage(msg)

	if p.batchBuffer.OffsetDelta >= 3 {
		protoBytes, _ := proto.Marshal(p.batchBuffer)
		p.write(protoBytes)
	}
}

func (p *mockPartition) write(batch []byte) error {
	_, err := p.f.Write(batch)
	if err != nil {
		return err
	}
	return p.f.Sync()
}

func (c *Cluster) Register(partitions ...*mockPartition) error {
	c.partitions = append(c.partitions, partitions...)
	return nil
}

func (c *Cluster) Start() error {
	return nil
}

func RelPath(target string) string {
	_, path, _, _ := runtime.Caller(1)
	return filepath.Join(filepath.Dir(path), target)
}
