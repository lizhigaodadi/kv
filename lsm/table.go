package lsm

import (
	"kv/file"
	"kv/utils"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Table struct {
	ss  *file.SSTable
	opt *utils.Options
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

func (t *Table) DecrDef() {
	atomic.AddInt32(&t.ref, -1)
}

func OpenTableByBuilder(lm *levelManager, tableName string, builder *TableBuilder) *Table {
	/*请确保builder不为nil*/
	if builder != nil {
		return nil
	}

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
	err   error
	kr    KeyRange /*用于给TableIterator进行排序使用*/
	idx   int      /*表示当前的迭代器位置*/
	iters []*BlockIterator
}

/*请确保Table是已经初始化好了的*/
func (t *Table) NewTableIterator() (*TableIterator, error) {
	/*读取它身上的所有块迭代器出来*/
	t.IncrRef()
	/*计算有多少个block*/
	bc := t.ss.BlockCount()
	blocks := make([]*Block, 0)
	biters := make([]*BlockIterator, 0)
	for i := 0; i < bc; i++ {
		/*反序列化block*/
		block, err := t.ss.ReadBlock(i)
		if err != nil {
			t.DecrDef()
			/*TODO:把之前创建的资源全部Close(之后完善)*/

			return nil, err
		}
		biter := block.NewBlockIterator(i)
		biters = append(biters, biter)
		blocks = append(blocks, block)
	}

	/*创建相应的迭代器*/
	ti := &TableIterator{
		t:     t,
		kr:    KeyRange{},
		idx:   0,
		iters: biters,
	}

	return ti, nil
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

func (ti *TableIterator) SeekToFirst() {
	/*移动到第一个位置*/
	ti.idx = 0
	/*全部设置到初始的模式*/
	for _, iter := range ti.iters {
		iter.Rewind()
	}
}

func (ti *TableIterator) SeekToEnd() {
	/*设置到最后*/
	ti.idx = len(ti.iters) - 1

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
	/*找到每一个块的最小Key*/
	idx := sort.Search(ti.t.ss.BlockCount(), func(idx int) bool {
		/*获取idx代表的block*/
		if idx == len(ti.iters) {
			return true
		}
		return utils.CompareKeys(ti.t.ss.Indexs().GetOffsets()[idx].GetKey(), key) > 0
	})
	if idx == 0 {
		ti.idx = 0
	} else {
		ti.idx = idx - 1
	}

}

/*从当前指向的block中搜索数据出来*/
func (ti *TableIterator) seekHelper(key []byte) {
	cbi := ti.iters[ti.idx]
	cbi.Seek(key)
	ti.err = cbi.Err()
}

func (t *Table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrDef()
	/*先通过布隆过滤器判断一下该数据是否在在这个表*/
	if t.ss.HasBloomFilter() {
		bloomFilter := utils.Filter(t.ss.BloomFilter())

		if !bloomFilter.MayContains(key) {
			return nil, utils.KeyNotFoundErr
		}
	}
	/*有大概率该Key存在该文件中，我们开始换页读取磁盘*/
	err = t.LoadDiskResource()
	defer t.CloseDiskResource()

	/*创建相关的迭代器*/
	var ti *TableIterator
	if ti, err = t.NewTableIterator(); err != nil {
		/*TODO:清除资源的一些操作*/
		return nil, err
	}
	ti.Seek(key)
	if !ti.Valid() {
		/*TODO:清除资源的一些操作*/
		return nil, utils.KeyNotFoundErr
	}

	entry = ti.Item().Entry()
	/*判断一下是否是真的相等*/
	if utils.SameKey(key, entry.Key) {
		if version := utils.ParseTs(entry.Key); *maxVs < version {
			*maxVs = version
			return entry, nil
		}
	}
	return nil, utils.KeyNotFoundErr
}

/*关闭紫盘资源，只留下我们需要的*/
func (t *Table) CloseDiskResource() {
	/*我们只需要关闭映射就好了，内存中的其他资源要保留,特别是文件句柄要留好*/
	_ = t.ss.CloseDiskResource()
}

/*加载磁盘资源,并更新内存中的数据*/
func (t *Table) LoadDiskResource() error {
	if t.isLoad() {
		return nil
	}

	/*映射一下fim中的磁盘资源*/
	if err := t.ss.LoadDiskToMemory(); err != nil {
		return err
	}
	/*索引资源一般是提前加载好了的不用再加载了*/
	if t.ss.Indexs() == nil {
		/*第一次加载，需要拉取索引下来*/
		err := t.ss.Init()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) isLoad() bool {
	return false
}
