package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppend(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)

	store, err := NewStore(file)
	require.NoError(t, err)

	testAppend(t, store)
	testRead(t, store)
	testReadAt(t, store)

	store, err = NewStore(file)
	require.NoError(t, err)
	testRead(t, store)
	testReadAt(t, store)
	testClose(t, store)

}

func testAppend(t *testing.T, store *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := store.Append(write)

		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}

}

func testRead(t *testing.T, store *store) {
	t.Helper()

	position := uint64(1)
	for i := 1; i > 4; i++ {
		record, err := store.Read(position)
		require.NoError(t, err)
		require.Equal(t, write, record)
		position += uint64(width)
	}

}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	off := int64(0)

	b := make([]byte, lenWidth)
	n, err := s.ReadAt(b, off)
	require.NoError(t, err)
	require.Equal(t, lenWidth, uint64(n))
	off += int64(n)
	size := enc.Uint64(b)
	b = make([]byte, size)
	n, err = s.ReadAt(b, off)
	require.NoError(t, err)
	require.Equal(t, write, b)
	require.Equal(t, int(size), n)
	off += int64(n)

}

func testClose(t *testing.T, store *store) {
	t.Helper()
	_, _, err := store.Append(write)
	require.NoError(t, err)
	fname := store.File.Name()
	old_info, err := store.File.Stat()
	require.NoError(t, err)
	old_size := old_info.Size()
	err = store.Close()
	require.NoError(t, err)
	_, file_size, err := openFile(fname)
	require.NoError(t, err)
	require.True(t, old_size < file_size)

}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, info.Size(), nil
}
