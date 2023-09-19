package file

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"kv/lsm"
	"kv/pb"
	"kv/utils"
	"log"
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
	fim, err := NewFileInMemory(opt.FileName)
	if err != nil {
		log.Fatal(err)
	}
	return &SSTable{fim: fim, fid: opt.FID, lock: &sync.RWMutex{}}
}

func (ss *SSTable) Size() uint32 {
	return uint32(ss.fim.DataSize)
}

func (ss *SSTable) BlockCount() int {
	ti := ss.idxTable
	return len(ti.Offsets)
}

/*TODO:请确保该方法执行前sst被加载到了内存当中*/
func (ss *SSTable) GetBlockOffset(idx int) *pb.BlockOffset {
	/*判断是否超出范围*/
	if idx < 0 || idx >= ss.BlockCount() {
		return nil
	}

	return ss.idxTable.GetOffsets()[idx]
}

/*初始化数据*/
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
	/*读取最后一个Block的最后一个，然后释放资源，这就是当前SSTable中最大的Key*/
	end := len(ss.idxTable.Offsets) - 1
	block, err := ss.ReadBlock(end)
	if err != nil {
		/*TODO:一些释放资源的操作*/
		return err
	}
	/*创建相关的迭代器出来*/
	bi := block.NewBlockIterator(end)
	bi.SeekToEnd()
	ss.minKey = minKey
	ss.maxKey = bi.Item().Entry().Key
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
func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
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

func (ss *SSTable) SetCreatedAt(time time.Time) {
	ss.createAt = time
}
func (ss *SSTable) Truncate(size uint64) error {
	return ss.fim.Truncate(int(size))
}
func (ss *SSTable) Delete() error {
	return ss.fim.Delete()
}

/*映射切片返回*/
func (ss *SSTable) Bytes(sz, offset int) ([]byte, error) {
	return ss.fim.Bytes(sz, offset)
}

func (ss *SSTable) Sync() error {
	return ss.fim.Sync()
}

func (ss *SSTable) CheckSum() []byte {
	return ss.fim.CheckSum()
}

func (ss *SSTable) GetCreateAt() time.Time {
	return ss.createAt
}

func (ss *SSTable) ReadBlock(idx int) (*lsm.Block, error) {
	/*检查是否超出了我们读取的范围*/
	ti := ss.idxTable
	blockCount := len(ti.Offsets)
	if idx >= blockCount {
		return nil, utils.OverRangeErr
	}

	tb := ti.Offsets[idx]
	baseKey := tb.Key
	blockBuffer, err := ss.fim.Bytes(int(tb.Length), int(tb.Offset))
	if err != nil {
		return nil, err
	}

	/*开始反序列化*/
	curBlock := BlockUnMarshal(blockBuffer)
	curBlock.SetBaseKey(baseKey)

	return curBlock, nil
}

func BlockUnMarshal(buffer []byte) *lsm.Block {
	readPos := len(buffer)

	chkLen := buffer[readPos-1]
	utils.CondPanic(chkLen != 8, utils.UnMarshalParseErr)
	readPos -= 1
	chk := binary.BigEndian.Uint64(buffer[readPos-int(chkLen) : readPos])
	readPos -= int(chkLen)
	/*offsetLen长度为4*/
	offsetLen := binary.BigEndian.Uint32(buffer[readPos-4 : readPos])
	readPos -= 4
	utils.CondPanic(offsetLen%4 != 0, utils.UnMarshalParseErr)

	offsetBuf := buffer[readPos-int(offsetLen) : readPos]
	readPos -= int(offsetLen)
	entryOffsets := make([]uint32, 0)
	for i := 0; i < int(offsetLen); i += 4 {
		eo := utils.BytesToU32(offsetBuf[i : i+4])
		entryOffsets = append(entryOffsets, eo)
	}
	data := buffer[0:readPos]
	/*计算当前的校验和*/
	curChk := utils.CalculateChecksum(data)
	utils.CondPanic(curChk != chk, utils.UnMarshalParseErr)

	return lsm.NewBlock(0, readPos, int(chkLen), data, nil)
}

func (ss *SSTable) BloomFilter() []byte {
	return ss.idxTable.BloomFilter
}

func (ss *SSTable) IsLoad() bool {
	/*检查fim的dataSize大小即可*/
	return ss.fim.IsLoad()
}

func (ss *SSTable) LoadDiskToMemory() error {
	/*确保文件句柄不为空*/
	if ss.fim.IsLoad() {
		return utils.RepeatLoadErr
	}
	/*开始加载*/
	fd := ss.fim.fd
	fim, err := LoadFileToMemory(fd)
	if err != nil {
		return err
	}
	ss.fim = fim /*转换为新的*/

	return nil
}

func (ss *SSTable) CloseDiskResource() error {
	return ss.fim.CloseDiskResource()
}
