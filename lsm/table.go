package lsm

type Table struct {
	fid uint64
	ref int32 /*ref次数，来实现垃圾回收*/
}