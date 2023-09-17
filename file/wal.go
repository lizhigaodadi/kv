package file

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"kv/utils"
	"os"
	"sync"
)

const (
	headPrevLen = 12
)

type WalFile struct {
	opt     *Options
	fim     *fileInMemory /*自己封装的一个伪mmap对象*/
	rwLock  sync.RWMutex
	writeAt uint32
}

type WalHeader struct {
	key       []byte
	val       []byte
	expiresAt uint64
}

func newWalHeader(key []byte, val []byte, expiresAt uint64) *WalHeader {
	return &WalHeader{
		key:       key,
		val:       val,
		expiresAt: expiresAt,
	}
}

/*请确保该WalFile已经存在了*/
func OpenWalFile(option *Options) (*WalFile, error) {
	/*打开加载这个文件文件*/
	fim, err := NewFileInMemory(option.FileName)
	if err != nil {
		return nil, err
	}
	fileInfo, err := os.Stat(option.FileName)
	writeAt := fileInfo.Size()

	return &WalFile{
		opt:     option,
		fim:     fim,
		rwLock:  sync.RWMutex{},
		writeAt: uint32(writeAt),
	}, nil
}

func (wh *WalHeader) encode() ([]byte, int) {
	var written int
	/*将数据写入到字节数组中*/
	size := wh.encodeSize()
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[written:written+4], uint32(len(wh.key)))
	written += 4
	binary.BigEndian.PutUint32(buf[written:written+4], uint32(len(wh.key)))
	written += 4
	keyLen := len(wh.key)
	valLen := len(wh.val)
	utils.CondPanic(copy(buf[written:written+keyLen], wh.key) == keyLen,
		fmt.Errorf("copy for key failed"))
	written += keyLen
	utils.CondPanic(copy(buf[written:written+valLen], wh.val) == valLen,
		fmt.Errorf("copy for val failed"))

	/*计算校验和*/
	crc := crc32.Checksum(buf[0:written], utils.CastageoliCrcTable)
	crcBuf := utils.U32ToBytes(crc)
	utils.CondPanic(copy(buf[written:written+4], crcBuf) == 4,
		fmt.Errorf("copy for val failed"))
	written += 4

	return buf, written
}

func (wh *WalHeader) encodeSize() int {
	return 4 /*keyLen*/ + 4 /*valLen*/ + 4 /*expiresAt*/ + len(wh.key) + len(wh.val) + 4 /*crc校验和*/
}

/*追加写入*/
func (wf *WalFile) Write(entry *utils.Entry) error {
	/*追加写入*/
	header := &WalHeader{
		key:       entry.Key,
		val:       entry.Value,
		expiresAt: entry.ExpireAt,
	}

	buf, _ := header.encode()
	err := wf.fim.AppendBuffer(wf.writeAt, buf)
	n := uint32(len(buf))
	wf.writeAt += n
	if err != nil {
		return err
	}

	return nil
}

func (wf *WalFile) ReadEntry(offset uint32) (*utils.Entry, int, error) {
	/*从映射中读取一个entry*/
	readAt := int(offset)
	if int(offset)+headPrevLen > wf.Size() {
		return nil, 0, errors.New("Over Size")
	}
	prevBytes, err := wf.fim.Bytes(headPrevLen, readAt)
	if err != nil {
		wf.Truncate(int(offset))
		return nil, 0, err
	}
	readAt += headPrevLen

	keyLen := utils.BytesToU32(prevBytes[0:4])
	valueLen := utils.BytesToU32(prevBytes[4:8])
	expireAt := utils.BytesToU64(prevBytes[8:16])

	if int(offset)+headPrevLen+int(keyLen+valueLen)+4 > wf.Size() {
		return nil, 0, errors.New("Over Size")
	}
	bytes, err := wf.fim.Bytes(int(keyLen+valueLen+4), readAt)
	if err != nil {
		/*这里进行一次截断*/
		wf.Truncate(int(offset))
		return nil, 0, err
	}
	value := make([]byte, valueLen)
	key := make([]byte, keyLen)
	utils.CondPanic(copy(key, bytes[0:keyLen]) == int(keyLen),
		fmt.Errorf("copy failed"))
	utils.CondPanic(copy(value, bytes[keyLen:keyLen+valueLen]) == int(valueLen),
		fmt.Errorf("copy failed"))

	crc := utils.BytesToU32(bytes[keyLen+valueLen : 4+keyLen+valueLen])
	bytes = append(prevBytes, bytes...)
	/*计算校验和*/
	if err = utils.VerifyCrc32(bytes, crc); err != nil {
		return nil, 0, err
	}

	return &utils.Entry{
		Key:      key,
		Value:    value,
		ExpireAt: expireAt,
	}, int(offset) + headPrevLen + int(keyLen+valueLen) + 4, nil
}

func (wf *WalFile) Truncate(size int) error {
	return wf.fim.Truncate(size)
}

func (wf *WalFile) Iterate(readOnly bool, fn utils.EntryHandle) (int, error) {
	/*TODO:通过readOnly来判断加什么锁遍历整个WalFile并执行相应的fn方法*/
	var written int
	wf.lock(readOnly)
	defer wf.unlock(readOnly)
	for {
		entry, n, err := wf.ReadEntry(uint32(written))
		if err != nil { /*读到了底部*/
			break
		}
		written += n
		if err = fn(entry); err != nil { /*这个是真的出了error*/
			return 0, err
		}
	}
	return written, nil
}

func (wf *WalFile) Size() int {
	return wf.fim.DataSize
}

func (wf *WalFile) lock(readOnly bool) {
	if readOnly {
		wf.rwLock.RLock()
	} else {
		wf.rwLock.Lock()
	}
}

func (wf *WalFile) Name() string {
	return wf.opt.FileName
}

func (wf *WalFile) unlock(readOnly bool) {
	if readOnly {
		wf.rwLock.RUnlock()
	} else {
		wf.rwLock.Unlock()
	}
}

func (wf *WalFile) Fid() uint64 {
	return wf.opt.FID
}
func (wf *WalFile) Sync() error {
	return wf.fim.Sync()
}

func (wf *WalFile) Close() error {
	if err := wf.fim.Sync(); err != nil {
		return err
	}
	wf.fim.Close()
	return nil
}

func (wf *WalFile) EstimateSz() int {
	return wf.fim.DataSize
}

func (wf *WalFile) Dir() string {
	return wf.opt.Dir
}
