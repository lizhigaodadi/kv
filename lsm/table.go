package lsm

import (
	"kv/file"
	"kv/utils"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Table struct {
	ss  *file.SSTable
	opt *Options
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

func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func OpenTableByBuilder(lm *levelManager, tableName string, builder *TableBuilder) *Table {
	/*请确保builder不为nil*/
	if builder != nil {
		return nil
	}

	sstSize := builder.Done().size
	t, err := builder.Flush(lm, tableName)
	if err != nil {
		utils.Err(err)
		return nil
	}

	t.IncrRef()
	err = t.ss.Init()
	if err != nil {
		utils.Err(err)
		return nil
	}

	return t
}

func (t *Table) CheckSum() []byte {
	return t.ss.CheckSum()
}

/*TODO: 思路不明确，待之后完善*/
func (t *Table) StaleDataSize() uint32 {
	return 0
}

func (t *Table) GetCreateAt() time.Time {
	return t.ss.GetCreateAt()
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

func (t *Table) ReName(newName string) {
	/*TODO:兼容接口*/
}

type TableIterator struct {
	t     *Table
	kr    KeyRange /*用于给TableIterator进行排序使用*/
	idx   int      /*表示当前的迭代器位置*/
	iters []*BlockIterator
}

/*请确保Table是已经初始化好了的*/
func NewTableIterator(t *Table) *TableIterator {

	return nil
}

func (ti *TableIterator) Next() { /*移动到下一个位置*/
	/*判断迭代器是否生效*/
	if !ti.Valid() { /*迭代器失效了*/
		log.Fatal("TableIterator Invalid")
		return
	}
	/*判断当前分迭代器是否已经到达了尽头*/
	if ti.iters[ti.idx].HasNext() { /*判断是否还有下一个*/
		if ti.idx < len(ti.iters)-1 { /*移动到下一个位置*/
			ti.idx++
			ti.iters[ti.idx].Rewind() /*移动到起始位置*/
		} else { /*已经到了尽头*/
			return
		}
	}

	/*移动到下一个位置*/
	ti.iters[ti.idx].Next()
}

func (ti *TableIterator) Item() utils.Item {
	if !ti.Valid() {
		return &Item{
			E: nil,
		}
	}
	return ti.iters[ti.idx].Item()
}

func (ti *TableIterator) Valid() bool {
	if len(ti.iters) == 0 {
		return false
	}
	return ti.idx >= 0 && ti.idx < len(ti.iters)
}

func (ti *TableIterator) Rewind() {
	/*对所有的迭代器都进行一次rewind*/
	for _, iter := range ti.iters { /*rewind*/
		iter.Rewind()
	}
	ti.idx = 0
}

func (ti *TableIterator) Close() {
	for _, iter := range ti.iters { /*rewind*/
		iter.Close()
	}
}

func (ti *TableIterator) Seek(key []byte) {
	/*TODO:暂时没什么头绪有一个比较好的解决方案*/
}
