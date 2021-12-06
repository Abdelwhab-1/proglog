package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/abdelwhab-1/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	Index                  *index
	Store                  *store
	baseOffset, nextOffset uint64
	conf                   Config
}

func newSegment(dir string, baseOffset uint64, conf Config) (*segment, error) {
	segment := &segment{
		baseOffset: baseOffset,
		conf:       conf,
	}

	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(storeFile)
	if err != nil {
		return nil, err
	}
	segment.Store = store
	idxFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}
	idx, err := newIndex(idxFile, segment.conf)
	if err != nil {
		return nil, err
	}

	segment.Index = idx

	if off, _, err := segment.Index.Read(-1); err != nil {
		segment.nextOffset = baseOffset
	} else {
		segment.nextOffset = baseOffset + uint64(off) + 1
	}
	return segment, nil
}

func (seg *segment) Append(record *api.Record) (uint64, error) {
	curr := seg.nextOffset
	record.Offset = curr
	b, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := seg.Store.Append(b)
	if err != nil {
		return 0, err
	}

	err = seg.Index.Write(pos, uint32(curr-uint64(seg.baseOffset)))
	if err != nil {
		return 0, err
	}

	seg.nextOffset++
	return curr, nil
}

func (seg *segment) Read(offSet uint64) (*api.Record, error) {
	_, pos, err := seg.Index.Read(int64(offSet - seg.baseOffset))
	if err != nil {
		return nil, err
	}

	b, err := seg.Store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{Value: b}
	err = proto.Unmarshal(b, record)
	return record, err
}

func (seg *segment) isMaxedOut() bool {
	return seg.Store.size >= seg.conf.Sagment.MaxStoreBytes || seg.Index.size >= seg.conf.Sagment.MaxIndexBytes
}

func (seg *segment) Close() error {
	if err := seg.Store.Close(); err != nil {
		return err
	}
	if err := seg.Index.Close(); err != nil {
		return err
	}
	return nil
}

func (seg *segment) Remove() error {
	if err := seg.Close(); err != nil {
		return nil
	}
	if err := os.Remove(seg.Index.Name()); err != nil {
		return err
	}

	if err := os.Remove(seg.Store.File.Name()); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
