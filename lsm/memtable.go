package lsm

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"kv/file"
	"kv/utils"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

type LSM struct {
	opt       Options
	memTables []*memTable
	mem       *memTable
	maxFid    uint64 /*用于原子的递增fid*/
	/*TODO: 待完善*/
}

/*从磁盘中读取到我们内存表中*/
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	/*新建一个*/
	files, err := ioutil.ReadDir(lsm.opt.workDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}

	var fids []uint64
	var maxFid uint64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), utils.FileSuffixNameWalLog) {
			/*这是wal文件*/
			fLen := len(file.Name())
			fid, err := strconv.ParseInt(file.Name()[:fLen-len(utils.FileSuffixNameWalLog)], 10, 64)
			if uint64(fid) > maxFid {
				maxFid = uint64(fid)
			}
			if err != nil {
				utils.Panic(err)
				return nil, nil
			}
			fids = append(fids, uint64(fid))
		}
	}
	/*对它们进行排序*/
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	lsm.maxFid = maxFid /*更新一个最大的值*/
	var memTables []*memTable

	for _, walId := range fids {
		/*初始化相应的wal日志文件*/
		mt, err := lsm.OpenMemTable(walId)
		if err != nil {
			return nil, nil
		}
		/*更新相应的SkipList上来，因为这是一个恢复的函数*/
		err = mt.UpdateSkipList()
		if err != nil {
			return nil, nil
		}
		memTables = append(memTables, mt)
	}
	lsm.memTables = memTables

	return lsm.NewMemTable(), memTables

}

func (lsm *LSM) NewMemTable() *memTable {
	/*原子递增一个fid*/
	fid := atomic.AddUint64(&lsm.maxFid, 1)
	opt := &file.Options{
		FID:      fid,
		Dir:      lsm.opt.workDir,
		MaxSz:    int(lsm.opt.maxSSTableSize),
		FileName: utils.FileNameWalLog(lsm.opt.workDir, fid),
	}
	wal, err := file.OpenWalFile(opt)
	if err != nil {
		return nil
	}

	return &memTable{
		sl:  utils.NewSkipList(1 << 20),
		lsm: lsm,
		wal: wal,
	}
}

func (mt *memTable) replayFunction(opt *Options) func(entry *utils.Entry) error {
	return func(entry *utils.Entry) error {
		/*TODO:其他逻辑待补充*/
		mt.sl.Add(entry)
		return nil
	}
}

type memTable struct {
	sl         *utils.SkipList
	wal        *file.WalFile
	lsm        *LSM
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lsm *LSM) OpenMemTable(fid uint64) (*memTable, error) {
	opt := &file.Options{
		FID:      fid,
		Dir:      lsm.opt.workDir,
		MaxSz:    int(lsm.opt.maxSSTableSize),
		FileName: utils.FileNameWalLog(lsm.opt.workDir, fid),
	}
	/*生成文件名*/
	wf, err := file.OpenWalFile(opt)

	if err != nil {
		return nil, err
	}

	sl := utils.NewSkipList(1 << 20)
	mt := &memTable{
		sl:  sl,
		wal: wf,
		lsm: lsm,
		buf: &bytes.Buffer{},
	}
	return mt, nil
}

func (mt *memTable) UpdateSkipList() error {
	/*提前的安全检查跳表和wal日志是否已经加载了*/
	if mt.wal == nil || mt.sl == nil {
		log.Fatalf("MemTable Has Not Init\n")
		return nil
	}

	endOff, err := mt.wal.Iterate(true, mt.replayFunction(nil))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", mt.wal.Name()))
	}
	/*保证wal日志中没有其他的冗余内容*/
	err = mt.wal.Truncate(endOff)

	return err
}

func (mt *memTable) MemSize() uint32 {
	/*返回内存表占用的大致内存大小*/
	return mt.sl.MemSize()
}

func (mt *memTable) Set(entry *utils.Entry) error {
	/*追加写入一个对象到mt的wal日志中去*/
	err := mt.wal.Write(entry)
	if err != nil {
		return err
	}
	/*加入到内存表中*/
	mt.sl.Add(entry)
	return nil
}

func (mt *memTable) Get(key []byte) (*utils.Entry, error) {
	/*从内存表中搜索数据*/
	vs := mt.sl.Get(key)
	if len(vs.Value) == 0 {
		return nil, errors.New("can not found the key")
	}
	e := &utils.Entry{
		Key:      key,
		Value:    vs.Value,
		ExpireAt: vs.ExpiresAt,
		Version:  vs.Version,
		Meta:     vs.Meta,
	}
	return e, nil
}

func (mt *memTable) Sync() error {
	/*将文件刷盘*/
	return mt.wal.Sync()
}

/*关闭文件前刷盘*/
func (mt *memTable) Close() error {
	/*先确保刷盘成功了*/
	if err := mt.wal.Sync(); err != nil {
		return nil
	}
	return mt.wal.Close()
}
