package lsm

import (
	"github.com/hardcore-os/corekv/file"
	"io/ioutil"
	"kv/utils"
	"os"
	"sort"
	"strconv"
	"strings"
)

type LSM struct {
	opt       Options
	memTables []*memTable
	mem       *memTable
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

	for _, file := range files {
		if strings.HasSuffix(file.Name(), utils.FileSuffixNameWalLog) {
			/*这是wal文件*/
			fLen := len(file.Name())
			fid, err := strconv.ParseInt(file.Name()[:fLen-len(utils.FileSuffixNameWalLog)], 10, 64)
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

	mt := &memTable{}
	mt.sl = utils.NewSkipList(1 << 20)
	return nil, nil
}

type memTable struct {
	sl  *utils.SkipList
	wal *file.WalFile
	lsm *LSM
}

func OpenMemTable(dir string, fid uint64) (*memTable, error) {
	/*生成文件名*/
	fileName := utils.FileNameSSTable(dir, fid)
	fd, err := os.OpenFile(fileName, utils.DefaultOpenFileFlag, utils.DefaultFileAclMask)
	if err != nil {
		return nil, err
	}
	mt := &memTable{}
	mt.sl = utils.NewSkipList(1 << 20)
	return mt, nil
}
