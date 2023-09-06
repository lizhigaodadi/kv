package file

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"kv/utils"
	"log"
	"os"
)

const (
	bufferSize = 1 << 12
	oneGB      = 1 << 30
	writeGap   = 1 << 10
)

type fileInMemory struct {
	Data     []byte   /*从磁盘中读取到的数据，或者即将写入到磁盘的数据*/
	fd       *os.File /*文件指针*/
	DataSize int
}

func LoadFileToMemory(fd *os.File, DataSize int) (*fileInMemory, error) {

	if DataSize < bufferSize {
		DataSize = bufferSize
	}
	fim := &fileInMemory{
		fd:       fd,
		Data:     make([]byte, DataSize),
		DataSize: 0, /*实际文件写入到的位置*/
	}
	fim.fd = fd
	/*从文件中读取所有的数据出来*/
	buffer := make([]byte, bufferSize)
	var written int
	/*创建一个带有缓冲区的读取器*/
	reader := bufio.NewReader(fd)
	for {
		n, err := reader.Read(buffer)
		if err != nil && err.Error() != "EOF" {
			log.Fatal(err)
		}
		if n == 0 { /*已经读取完毕*/
			break
		}
		/*判断目前的数组大小是否够用*/
		if cap(fim.Data) <= n+written { /*不够用了需要扩容*/
			newSize := 2 * cap(fim.Data)
			newBuffer := make([]byte, newSize)
			utils.CondPanic(copy(newBuffer[:], fim.Data[:written]) == written, fmt.Errorf("write failed"))
			fim.Data = newBuffer
		}

		utils.CondPanic(copy(fim.Data[written:], buffer[:]) == n, fmt.Errorf("write failed"))
		fim.DataSize += n
	}
	return fim, nil
}

func newFileInMemory(fileName string, flag int) (*fileInMemory, error) {
	/*创建一个新的文件*/
	fd, err := os.OpenFile(fileName, flag, 0666)
	if err != nil {
		log.Fatalf("create file %s failed", fileName)
	}
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		log.Fatalf("get file stat %s failed", fileName)
	}
	if DataSize := fileInfo.Size(); DataSize > 0 { /*文件非空*/
		return LoadFileToMemory(fd, int(DataSize))
	}

	return &fileInMemory{fd: fd}, nil
}

/*直接在数组原本的基础上追加*/
func (fim *fileInMemory) Append(buf []byte) error {
	/*TODO: 判断是否需要扩容以应对追加数据*/
	cap := cap(fim.Data)
	needSize := len(buf)
	if fim.DataSize+needSize > cap { /*需要扩展*/
		growBy := cap
		if cap+growBy < fim.DataSize+needSize { /*仍然小于*/
			growBy = fim.DataSize + needSize - cap
		}
		/*开始扩容*/
		newBuf := make([]byte, cap+growBy)
		/*复制数据过去*/
		utils.CondPanic(copy(newBuf, fim.Data[:fim.DataSize]) == fim.DataSize,
			errors.New("copy failed"))
		fim.Data = newBuf
	}

	/*追加数据的逻辑*/
	utils.CondPanic(copy(fim.Data[fim.DataSize:fim.DataSize+needSize], buf) == needSize,
		errors.New("copy failed"))

	fim.DataSize += needSize
	return nil
}

/*加入数据进入*/
func (fim *fileInMemory) AppendBuffer(offset uint32, buf []byte) error {
	curSize := uint32(cap(fim.Data))
	needSize := offset + uint32(len(buf))

	if curSize < needSize {
		growBy := curSize
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < uint32(len(buf)) {
			growBy = uint32(len(buf))
		}

		newBuf := make([]byte, curSize+growBy)
		utils.CondPanic(copy(newBuf[:], fim.Data[:curSize]) == int(curSize), fmt.Errorf("copy failed"))
		fim.Data = newBuf
	}

	utils.CondPanic(copy(fim.Data[offset:], buf[:]) == len(buf), fmt.Errorf("copy failed"))
	if int(offset)+len(buf) > fim.DataSize {
		fim.DataSize = int(offset) + len(buf)
	}

	return nil
}

func (fim *fileInMemory) Close() {
	fim.fd.Close()
}

func (fim *fileInMemory) NewReader() io.Reader {
	return fim.NewFileInMemoryReader()
}
func (fim *fileInMemory) Bytes(sz, offset int) ([]byte, error) {
	needSize := sz + offset
	if needSize > fim.DataSize {
		/*超出预期了*/
		return nil, fmt.Errorf("over buffer size")
	}

	return fim.Data[offset : offset+sz], nil
}

/*该方法只是为了兼容接口*/
func (fim *fileInMemory) ReName(name string) error {
	return nil
}

/*AllocateSlice对应的读取操作*/
func (fim *fileInMemory) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(fim.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > fim.DataSize {
		return []byte{}
	}
	res := fim.Data[next:]

	return res
}
func (fim *fileInMemory) Delete() error {
	/*删除当前的这个文件*/
	fd := fim.fd
	err := fd.Truncate(0)
	if err != nil {
		return err
	}
	err = fd.Sync()
	if err != nil {
		return err
	}
	err = fd.Close()
	return err
}

/*将数据写入到磁盘当中*/
func (fim *fileInMemory) Sync() error {
	/*将fileInMemory 中的数据写入到文件中*/
	/*调整文件读取指针位置*/
	fd := fim.fd
	_, err := fd.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	/*重新写入文件*/
	var written int
	for {
		if fim.DataSize-written < writeGap {
			n, err := fd.Write(fim.Data[written:fim.DataSize])
			if err != nil {
				return err
			}
			written = n
			break
		}

		n, err := fd.Write(fim.Data[written : writeGap+written])
		if err != nil || n != writeGap {
			return err
		}
		written += writeGap
	}
	err = fim.fd.Sync()
	if err != nil {
		return err
	}
	err = fim.fd.Truncate(int64(written)) /*保证没有多余的数据被写入*/
	if err != nil {
		return err
	}
	return nil
}

func (fim *fileInMemory) Truncate(size int) error {
	/*直接修改DataSize即可*/
	fim.DataSize = size
	return fim.fd.Truncate(int64(size))
}

func (fim *fileInMemory) AllocateSlice(sz, offset int) ([]byte, error) {
	start := offset + 4

	if start+sz > len(fim.Data) {
		/*需要进行扩容操作*/
		growBy := len(fim.Data)
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
	}

	err := fim.Truncate(start + sz)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(fim.Data[offset:], uint32(sz))

	return fim.Data[start : start+sz], nil
}

type fileInMemoryReader struct {
	fim     *fileInMemory
	readPos int
}

func (fim *fileInMemory) NewFileInMemoryReader() *fileInMemoryReader {
	return &fileInMemoryReader{fim, 0}
}

func (fimr *fileInMemoryReader) Read(p []byte) (int, error) {
	/*实现io.Reader的接口*/
	needSize := len(p)
	dataSize := int(fimr.fim.DataSize)
	if dataSize-fimr.readPos < needSize { /*读取不了那么多*/
		utils.CondPanic(dataSize-fimr.readPos == copy(p[:], fimr.fim.Data[fimr.readPos:dataSize]), fmt.Errorf("copy failed"))
		n := dataSize - fimr.readPos
		fimr.readPos = dataSize
		return n, nil
	}

	utils.CondPanic(needSize == copy(p[:], fimr.fim.Data[fimr.readPos:fimr.readPos+needSize]), fmt.Errorf("copy failed"))
	fimr.readPos += needSize
	return needSize, nil

}
