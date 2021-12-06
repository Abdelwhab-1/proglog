package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	defer os.Remove(tempFile.Name())
	conf := Config{}
	conf.Sagment.MaxIndexBytes = 1024
	idx, err := newIndex(tempFile, conf)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, tempFile.Name(), idx.Name())
	entris := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 1},
	}

	for _, want := range entris {
		err = idx.Write(want.Pos, want.Off)
		require.NoError(t, err)
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
		_, _, err = idx.Read(int64(len(entris)))
		require.Equal(t, io.EOF, err)

	}

	idx.Close()

	f, _ := os.OpenFile(tempFile.Name(), os.O_RDWR, 0600)

	idx, err = newIndex(f, conf)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entris[1].Pos, pos)
	require.Equal(t, uint32(1), off)
}
