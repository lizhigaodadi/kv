package lsm

import "kv/utils"

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

	return nil
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
	table := OpenTable(lm.opt)

	return nil
}
