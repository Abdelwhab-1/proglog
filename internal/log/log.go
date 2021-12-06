package log

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/abdelwhab-1/proglog/api/v1"
)

type log struct {
	mu            sync.RWMutex
	segments      []*segment
	activeSegment *segment
	Dir           string
	Config        Config
}

func NewLog(dir string, con Config) (*log, error) {
	if con.Sagment.MaxIndexBytes == 0 {
		con.Sagment.MaxIndexBytes = 1024
	}
	if con.Sagment.MaxStoreBytes == 0 {
		con.Sagment.MaxStoreBytes = 1024
	}
	log := &log{
		Dir:    dir,
		Config: con,
	}
	return log, log.setup()
}

func (log *log) setup() error {
	fsInfo, err := ioutil.ReadDir(log.Dir)
	if err != nil {
		return err
	}
	var baseOffset []uint64
	for _, fInfo := range fsInfo {
		offStr := strings.TrimSuffix(fInfo.Name(), path.Ext(fInfo.Name()))
		off, err := strconv.ParseUint(offStr, 10, 0)
		if err != nil {
			return err
		}
		baseOffset = append(baseOffset, off)
	}
	sort.Slice(baseOffset, func(i, j int) bool { return baseOffset[i] < baseOffset[j] })
	for i := 0; i < len(baseOffset); i++ {
		if err := log.newSegment(baseOffset[i]); err != nil {
			return err
		}
		i++
	}
	if log.segments == nil {
		if err := log.newSegment(log.Config.Sagment.InitialOffset); err != nil {
			return err
		}

	}
	return nil
}

func (log *log) newSegment(off uint64) error {
	seg, err := newSegment(log.Dir, off, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, seg)
	log.activeSegment = seg
	return nil
}

func (log *log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if log.activeSegment.isMaxedOut() {
		err = log.newSegment(off + 1)
	}
	return off, err
}

func (log *log) Read(off uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	var s *segment
	for _, seg := range log.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			s = seg
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{OffSet: off}
	}
	return s.Read(off)
}

func (log *log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, seg := range log.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (log *log) Remove() error {

	if err := log.Close(); err != nil {
		return err
	}
	return os.Remove(log.Dir)
}

func (log *log) Reset() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *log) LowestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.segments[0].baseOffset, nil
}

func (log *log) HighestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	off := log.segments[len(log.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (log *log) Truncate(off uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	var segments []*segment
	for _, seg := range log.segments {
		if seg.nextOffset <= off+1 {
			if err := seg.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, seg)
	}
	log.segments = segments
	return nil
}

func (log *log) Reader() io.Reader {
	log.mu.RLock()
	defer log.mu.RUnlock()
	readers := make([]io.Reader, len(log.segments))
	for i, seg := range log.segments {
		readers[i] = &originReader{seg.Store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
