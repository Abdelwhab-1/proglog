package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth uint64 = 8
)

type store struct {
	File *os.File
	mu   sync.Mutex
	buff *bufio.Writer
	size uint64
}

func NewStore(file *os.File) (*store, error) {
	stat, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	store := store{
		File: file,
		size: uint64(size),
		buff: bufio.NewWriter(file),
	}
	return &store, err
}

func (store *store) Append(b []byte) (uint64, uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	position := store.size

	if err := binary.Write(store.buff, enc, uint64(len(b))); err != nil {
		return 0, 0, err
	}
	n, err := store.buff.Write(b)
	writenBytesNum := uint64(n) + lenWidth
	if err != nil {
		return 0, 0, nil
	}

	store.size += writenBytesNum
	return writenBytesNum, position, nil
}

func (store *store) Read(position uint64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	err := store.buff.Flush()
	if err != nil {
		return nil, err
	}
	start_pos := make([]byte, lenWidth)

	_, err = store.File.ReadAt(start_pos, int64(position))
	if err != nil {
		return nil, err
	}

	record := make([]byte, enc.Uint64(start_pos))

	_, err = store.File.ReadAt(record, int64(position+lenWidth))

	if err != nil {
		return nil, err
	}
	return record, nil
}

func (store *store) ReadAt(b []byte, off int64) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.buff.Flush(); err != nil {
		return 0, err
	}
	n, err := store.File.ReadAt(b, off)
	return n, err
}

func (store *store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.buff.Flush(); err != nil {
		return err
	}

	return store.File.Close()
}
