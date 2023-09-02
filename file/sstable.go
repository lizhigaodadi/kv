package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"kv/pb"
	"kv/utils"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
)

type SSTable struct {
	fim            *fileInMemory
	lock           *sync.RWMutex
	idxTable       *pb.TableIndex
	maxKey         []byte
	minKey         []byte
	hasBloomFilter bool
	idxStart       uint32
	indexLen       uint32
	readPos        uint64 /*方便读取文件数据的指针位置*/
	fid            uint64
	createAt       time.Time
}

func OpenSStable(opt *Options) *SSTable {
	fim, err := newFileInMemory(opt.FileName, os.O_RDWR|os.O_CREATE)
	if err != nil {
		log.Fatal(err)
	}
	return &SSTable{fim: fim, fid: opt.FID, lock: &sync.RWMutex{}}
}

func (ss *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = ss.initTable(); err != nil {
		return err
	}
	/*获取文件创建时间*/
	fileInfo, err := ss.fim.fd.Stat()
	if err != nil {
		return err
	}
	creationTime := fileInfo.Sys().(*syscall.Win32FileAttributeData).CreationTime
	/* 将 FILETIME 转换为 time.Time 类型*/
	nanoseconds := (int64(creationTime.HighDateTime) << 32) + int64(creationTime.LowDateTime)
	unixNanoSeconds := (nanoseconds - 116444736000000000) // 1970-01-01 到 1601-01-01 的纳秒数
	creationDate := time.Unix(0, unixNanoSeconds)
	ss.createAt = creationDate
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	ss.maxKey = minKey
	return nil
}

func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.fim.Data)
	readPos -= 4 /*读取一部分信息*/
	buf := ss.readCheckError(readPos, 4)
	checkSumLen := int(utils.BytesToU32(buf))
	if checkSumLen < 0 {
		return nil, errors.New("checkSumLen less zero ")
	}
	/*读取checkSum*/
	readPos -= checkSumLen
	exceptedChk := ss.readCheckError(readPos, checkSumLen)
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.indexLen = utils.BytesToU32(buf)
	readPos -= int(ss.indexLen)
	data := ss.readCheckError(readPos, int(ss.indexLen))
	err = utils.VerifyCheckSum(data, exceptedChk)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table")
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTable = indexTable
	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index offset is nil")
}

func (ss *SSTable) read(offset, sz int) ([]byte, error) {
	/*判断是否超出*/
	bytes, err := ss.fim.Bytes(offset, sz)
	return bytes, err

}

func (ss *SSTable) readCheckError(readPos, size int) []byte {
	buf, err := ss.read(readPos, size)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) SetMaxKey(maxKey []byte) {
	ss.maxKey = maxKey
}

func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}
func (ss *SSTable) SetMinKey(minKey []byte) {
	ss.minKey = minKey
}

func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTable
}

func (ss *SSTable) FID() uint64 {
	return ss.fid
}
func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}
func (ss *SSTable) Close() {
	ss.fim.Close()
}

func (ss *SSTable) GetCreatedAt() time.Time {
	return ss.createAt
}
func (ss *SSTable) SetCreatedAt(time time.Time) {
	ss.createAt = time
}
func (ss *SSTable) Truncate(size uint64) error {
	return ss.fim.Truncate(int(size))
}
func (ss *SSTable) Delete() error {
	return ss.fim.Delete()
}
