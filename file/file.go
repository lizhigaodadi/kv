package file

import "io"

type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

/*实现文件的接口*/
type CoreFile interface {
	Close() error
	Truncate(n int64) error
	ReName(name string) error
	NewReader(offset int) io.Reader
	Bytes(off, sz int64) ([]byte, error)
	AllocateSlice(sz, offset int) ([]byte, error)
	Sync() error
	Delete() error
	Slice(offset int) []byte
}
