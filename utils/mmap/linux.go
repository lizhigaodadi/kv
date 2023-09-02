//go:build linux
// +build linux

package mmap

import (
	"golang.org/x/sys/unix"
	"os"
	"reflect"
	"unsafe"
)

/*mmap关于Linux下的实现*/
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}

	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

/*直接修改底层的SliceHeader结构体来实现remap*/
func mremap(data []byte, size int) ([]byte, error) {
	const MREMAP_MAYMOVE = 0x1

	header := (*reflect.SliceHeader)(unsafe.Pointer(&data[0]))
	mmapAddr, _, err := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,
		uintptr(header.Len),
		uintptr(size),
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if err != 0 {
		return nil, err
	}

	header.Data = mmapAddr
	header.Cap = size
	header.Len = size
	return data, nil
}

func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func madvise(b []byte, readahead bool) error {
	flags := unix.MADV_NORMAL
	if !readahead {
		flags = unix.MADV_NORMAL
	}
	return unix.Madvise(b, flags)
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
