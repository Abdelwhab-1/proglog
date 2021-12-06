package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	idxWidth    uint64 = 4
	recPosition uint64 = 8
	entWidth           = idxWidth + recPosition
)

type index struct {
	File *os.File
	size uint64
	mmap gommap.MMap
}

func newIndex(file *os.File, conf Config) (*index, error) {
	idx := &index{
		File: file,
	}
	fileInfo, err := idx.File.Stat()
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fileInfo.Size())
	if err = os.Truncate(file.Name(), int64(conf.Sagment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	idx.mmap, err = gommap.Map(idx.File.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return idx, nil

}

func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.File.Sync(); err != nil {
		return err
	}
	if err := idx.File.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.File.Close()
}

func (idx *index) Write(pos uint64, off uint32) error {
	if uint64(len(idx.mmap)) < idx.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(idx.mmap[idx.size:idx.size+idxWidth], off)
	enc.PutUint64(idx.mmap[idx.size+idxWidth:idx.size+entWidth], pos)
	idx.size += entWidth
	return nil
}

func (idx *index) Read(indexPos int64) (out uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}
	if indexPos == -1 {
		out = uint32((idx.size / entWidth) - 1)

	} else {
		out = uint32(indexPos)
	}

	pos = uint64(out) * entWidth
	if idx.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(idx.mmap[pos : pos+idxWidth])
	pos = enc.Uint64(idx.mmap[pos+idxWidth : pos+entWidth])
	return out, pos, nil
}

func (idx *index) Name() string {
	return idx.File.Name()
}
