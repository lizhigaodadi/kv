package lsm

import (
	"kv/file"
	"kv/utils"
)

type LSM struct {
	opt                 Options
	mem                 *memTable
	memTables           []*memTable
	maxFid              uint64 /*用于原子的递增fid*/
	levelSteppedUpBasis int    /*level递增基数*/
	lm                  *levelManager
	closer              *utils.Closer
	/*TODO: 待完善*/
}

func (lsm *LSM) StartCompacter() {
	n := lsm.opt.NumCompactors
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.lm.runCompacter(n)
	}
}

func (lsm *LSM) Set(entry *utils.Entry) error {
	/*判断一下是否为空*/
	if entry == nil || len(entry.Key) == 0 {
		return utils.EntryNilErr
	}
	/*检测当前表大小是否超出*/
	if lsm.mem.estimateSz()+utils.EstimateWalCodecSize(entry) >
		lsm.opt.MaxMemTableSize {
		/*超出了*/
		lsm.Rotate()
	}
	/*存储entry到内存和wal日志中*/
	err := lsm.mem.Set(entry)
	if err != nil {
		return err
	}
	/*检查是否需要刷盘*/
	for _, table := range lsm.memTables {
		err = lsm.lm.flush(table)
		if err != nil {
			return err
		}
		/*TODO:这里实际应该是通过减少标记的方式close*/
		err = table.Close()
		utils.Panic(err)
	}

	if len(lsm.memTables) != 0 {
		/*将其置空*/
		lsm.memTables = make([]*memTable, 0)
	}
	return err
}

func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.EntryNilErr
	}
	lsm.closer.Add(1) /*保证优雅的关闭数据库*/
	defer lsm.closer.Done()

	var (
		entry *utils.Entry
		err   error
	)

	/*先从内存表中去找*/
	if entry, err = lsm.mem.Get(key); err != nil {
		return nil, err
	}
	/*再从备用的内存表中去寻找*/
	for _, t := range lsm.memTables {
		entry, err = t.Get(key)
		if err == nil { /*成功找到了*/
			return entry, err
		}
		/*继续找*/
	}

	/*内存中没有匹配的数据，现在从磁盘中寻找*/
	entry, err = lsm.lm.Get(key)
	if err != nil {
		return nil, err
	}

	return entry, err
}

func (lsm *LSM) Rotate() {
	lsm.memTables = append(lsm.memTables, lsm.mem)
	/*创建一个新的memTable*/
	lsm.mem = lsm.NewMemTable()
}

func (lm *levelManager) flush(t *memTable) error {
	fid := t.wal.Fid()
	sstName := utils.FileNameSSTable(t.wal.Dir(), fid)

	/*获取跳表的迭代器*/
	builder := NewTableBuilder(lm.opt)
	skipIterator := t.sl.NewSkipListIterator()
	for skipIterator.Rewind(); skipIterator.Valid(); skipIterator.Next() {
		entry := skipIterator.Item().Entry()
		builder.Add(entry)
	}

	//创建一个table对象
	table := OpenTableByBuilder(lm, sstName, builder)
	err := lm.mf.AddTableMeta(0, &file.TableMeta{
		Id:       fid,
		CheckSum: []byte{'m', 'o', 'c', 'k'},
	})

	if err != nil {
		return err
	}
	lm.handlers[0].Add(table)

	return nil
}
