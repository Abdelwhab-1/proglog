package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/abdelwhab-1/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {

	dir, _ := ioutil.TempDir("", "segment_test")
	defer os.Remove(dir)
	want := &api.Record{Value: []byte("hello world")}
	c := Config{}
	c.Sagment.MaxStoreBytes = 1024
	c.Sagment.MaxIndexBytes = entWidth * 3
	seg, err := newSegment(dir, uint64(16), c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), seg.baseOffset, seg.nextOffset)
	require.False(t, seg.isMaxedOut())
	for i := uint64(0); i < uint64(3); i++ {
		off, err := seg.Append(want)
		require.NoError(t, err)
		require.Equal(t, uint64(16)+i, off)
		res, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, res.Value, want.Value)
	}
	_, err = seg.Append(want)
	require.Equal(t, io.EOF, err)
	require.True(t, seg.isMaxedOut())
	c.Sagment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Sagment.MaxIndexBytes = 1024

	seg, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, seg.isMaxedOut())

	err = seg.Remove()
	require.NoError(t, err)
	seg, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, seg.isMaxedOut())
}
