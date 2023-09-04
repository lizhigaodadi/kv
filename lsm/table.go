package lsm

import "kv/file"

type Table struct {
	ss *file.SSTable
	/*TODO: levelManager*/
	fid uint64
	ref int32 /*ref次数，来实现垃圾回收*/
}
