//go:build linux
// +build linux

package mmap

func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

func Munmap(b []byte) error {
	return munmap(b)
}

func Madvise(b []byte, readahead bool) error {
	return madvise(b, readahead)
}

func Msync(b []byte) error {
	return msync(b)
}
func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}
