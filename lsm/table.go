package lsm

import (
	"kv/file"
	"sync"
)

type Table struct {
	ss *file.SSTable
	/*TODO: levelManager*/
	fid uint64
	ref int32        /*ref次数，来实现垃圾回收*/
	m   sync.RWMutex /*读写锁*/
	lm  *levelManager
}

func OpenTable(opt *file.Options) *Table {
	/*创建一个sst文件*/
	sst := file.OpenSStable(opt)

	return &Table{
		ss:  sst,
		fid: opt.FID,
		ref: 1,
		m:   sync.RWMutex{},
	}
}

func (t *Table) Sync() error {
	return t.ss.Sync()
}

func (t *Table) Bytes(sz, offset int) ([]byte, error) {
	return t.ss.Bytes(sz, offset)
}

/*返回该Table中最大的key*/
func (t *Table) MaxKey() []byte {
	return t.ss.MaxKey()
}

/*返回该Table中最小的key*/
func (t *Table) MinKey() []byte {
	return t.ss.MinKey()
}

func (t *Table) Size() uint32 {
	return t.ss.Size()
}
func (t *Table) Close() {
	/*关闭该文件占用的内存资源，方便我们逐个迭代，减少oom的风险*/
	t.ss.Close()
}
