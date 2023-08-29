package lsm

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"kv/pb"
	"kv/utils"
	"kv/utils/cache"
	"log"
	"math"
	"unsafe"
)

const (
	checkSumLen = 8
)

type TableBuilder struct {
	blockList  []*Block
	tableSize  uint64
	curBlock   *Block
	options    *Options
	keyCount   uint32
	keysHash   []uint32
	MaxVersion uint64
}

type Options struct {
	maxSSTableSize           uint64
	BlockSize                uint64 /*表示我们这个Builder的Block序列化后的最大大小*/
	BloomFilterFalsePositive float64
}

type Block struct {
	offset       int      /*TODO: 作用未知*/
	end          int      /*表示当前data已经使用的大小*/
	chkLen       int      /*checkSum的大小*/
	estimateSz   int64    /*用于预估添加数据后大小*/
	entryOffsets []uint32 /*存储Entry的索引的数组*/
	data         []byte   /*实际存储的数据都被序列化在data中*/
	checkSum     []byte   /*用于对数据做安全检查保证数据的可靠和安全*/
	baseKey      []byte   /*用于压缩Keys*/
}

/*该数据结构是用于内存中存储要被持久化的数据内容*/
type BuilderData struct {
	blockList []*Block
	index     []byte
	checkSum  []byte
	size      int
}

type Header struct {
	Overlap uint16
	diff    uint16
}

const headerSize = uint16(unsafe.Sizeof(Header{}))

func (h Header) Encode() []byte {
	var buf [4]byte
	*((*Header)(unsafe.Pointer(&buf[0]))) = h
	return buf[:]
}

/*TODO: Warning: 这里可能存在问题*/
func (h *Header) Decode(head []byte) {
	*h = *((*Header)(unsafe.Pointer(&head[0])))
}

func NewTableBuilderWithSize(opt *Options, size uint64) *TableBuilder {
	return &TableBuilder{
		options:   opt,
		tableSize: size,
	}
}
func NewTableBuilder(opt *Options) *TableBuilder {
	return &TableBuilder{
		options:   opt,
		tableSize: opt.maxSSTableSize,
	}
}

func (tb *TableBuilder) Add(e *utils.Entry) {
	key := e.Key
	vs := utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpireAt,
		Version:   e.Version,
	}

	/*判断以下当前的block是否还足够我们使用*/
	if tb.TryFinishBlock(e) {
		/*发现不够用了，先将当前的block封装好了*/
		tb.FinishBlock()
		tb.curBlock = &Block{
			data: make([]byte, tb.options.BlockSize),
		}
	}
	/*开始写入*/

	tb.keyCount++
	tb.keysHash = append(tb.keysHash, cache.Hash(key))

	var diffKey []byte
	var overLap uint16
	if len(tb.curBlock.baseKey) == 0 { /*不存在basekey*/
		overLap = 0
		diffKey = key
	} else {
		diffKey, overLap = GetDiffKey(key, tb.curBlock.baseKey)
	}

	header := Header{
		Overlap: overLap,
		diff:    uint16(len(key)) - overLap,
	}
	tb.append(header.Encode())
	tb.append(diffKey)

	if version := utils.ParseTs(key); version >= tb.MaxVersion {
		tb.MaxVersion = version
	}

	/*这一部分的长度我们是不确定的，我们应该放在最后配合下一个block的Offset来确定大小*/
	vs.EncodeValue(tb.curBlock.data[tb.curBlock.end:])
	tb.curBlock.end += int(vs.EncodeSize())

}

/*判断当前的TableBuilder是否需要扩容*/
func (tb *TableBuilder) TryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true /*为nil当然需要重新扩容*/
	}

	/*计算以下当前的容量是否够用*/
	utils.CondPanic(!(uint32((len(tb.curBlock.entryOffsets)+1)*4+4+8+4) < math.MaxUint32), errors.New("Integer Overflow"))
	entriesOffsetSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + /*size of list*/
		8 + /*Sum64 in checkSum proto*/
		4) /*checkSum length*/
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetSize

	return tb.curBlock.estimateSz < int64(tb.options.BlockSize)
}

/*一个扩容内存大小的函数*/
func (tb *TableBuilder) Allocate(need int) []byte {
	curBlock := tb.curBlock
	if len(curBlock.data[:curBlock.end]) < need {
		sz := 2 * len(curBlock.data)
		if curBlock.end+need > sz {
			sz = curBlock.end + need
		}
		tmp := make([]byte, sz)
		copy(tmp, curBlock.data)
		curBlock.data = tmp
	}

	/*不需要扩容，或者已经成功扩容了*/
	curBlock.end += need
	return curBlock.data[curBlock.end-need : curBlock.end]
}

func (tb *TableBuilder) keyDiff(key []byte) []byte {
	buf, _ := GetDiffKey(key, tb.curBlock.baseKey)
	return buf
}

func GetDiffKey(key []byte, baseKey []byte) ([]byte, uint16) {
	var i int
	for i = 0; i < len(key) && i < len(baseKey); i++ {
		if key[i] != baseKey[i] {
			break
		}
	}
	return key[i:], uint16(i)
}

/*将当前的Block封装起来然后加入到list中，并创建一个新的Block*/
func (tb *TableBuilder) FinishBlock() {
	/*计算当前校验和*/
	checkSumU64 := utils.CalculateChecksum(tb.curBlock.data)
	tb.curBlock.checkSum = tb.CalculateChecksum(checkSumU64)
	/*计算偏移量数组的大小*/
	offsetsLen := uint32(len(tb.curBlock.entryOffsets) * 4) /*这个数据长度为4*/
	for _, e := range tb.curBlock.entryOffsets {
		tb.append(utils.U32ToBytes(e))
	}
	tb.append(utils.U32ToBytes(offsetsLen)) /*写入offsets 的长度*/
	tb.append(tb.curBlock.checkSum)
	tb.addByte(byte(checkSumLen))

	/*将当前curBlock加入到list中去*/
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.curBlock = nil /*置空而不是新建是为了节省空间，有可能之后就不需要再新建了*/

}

func (tb *TableBuilder) append(data []byte) {
	utils.CondPanic(len(data) == copy(tb.curBlock.data[tb.curBlock.end:], data), fmt.Errorf("data copy failed"))
	tb.curBlock.end += len(data)
}
func (tb *TableBuilder) addByte(data byte) {
	tb.curBlock.data[tb.curBlock.end] = data
	tb.curBlock.end++
}

func CalculateCheckSum(checkSum []byte) []byte {
	checkSumU64 := utils.CalculateChecksum(checkSum)
	return utils.U64ToBytes(checkSumU64)
}

func (tb *TableBuilder) CalculateChecksum(checkSum uint64) []byte {
	return utils.U64ToBytes(checkSum)
}

func (tb *TableBuilder) Done() BuilderData {
	bd := BuilderData{}

	var f cache.Filter
	if tb.options.BloomFilterFalsePositive > 0 {
		bf := cache.NewBloomFilter(int(tb.keyCount), tb.options.BloomFilterFalsePositive)
		bf.AppendBatch(tb.keysHash)
		f = bf.Bitmap
	}

	/*TODO: 构建sst的索引*/
	index, dataSize := tb.BuildIndex(f)
	bd.index = index
	bd.blockList = tb.blockList
	bd.checkSum = CalculateCheckSum(index)
	bd.size = int(dataSize) + len(index) + len(bd.checkSum) + 4 + 4 /*TODO:这两个4不清楚到底是那一部分的大小*/
	return bd
}

func (tb *TableBuilder) BuildIndex(filter []byte) ([]byte, uint64) {
	tableIndex := &pb.TableIndex{}
	if len(filter) != 0 {
		tableIndex.BloomFilter = filter
	}
	tableIndex.KeyCount = tb.keyCount

	tableIndex.Offsets = tb.WriteOffsets()
	var dataSize uint64
	for _, block := range tb.blockList {
		dataSize += uint64(block.end)
	}

	tableIndex.MaxVersion = tb.MaxVersion

	indexBuf, err := proto.Marshal(tableIndex)
	if err != nil {
		log.Fatalln("failed to marshal")
	}
	return indexBuf, dataSize
}

func (tb *TableBuilder) WriteOffsets() []*pb.BlockOffset {
	var startOffset int
	var blockOffsets []*pb.BlockOffset
	for _, block := range tb.blockList {
		blockOffset := &pb.BlockOffset{}
		blockOffset.Offset = uint32(startOffset)
		blockOffset.Length = uint32(block.end)
		blockOffset.Key = block.baseKey

		blockOffsets = append(blockOffsets, blockOffset)

		startOffset += block.end
	}

	return blockOffsets
}

/*TODO: 写入ssTable*/
func (tb *TableBuilder) Flush() {

}

/*将整个ssttable序列化*/
func (tb *BuilderData) Copy(dst []byte) int {
	var written int

	for _, block := range tb.blockList {
		written += copy(dst[written:], block.data[:block.end])
	}

	written += copy(dst[written:], tb.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(tb.index))))
	written += copy(dst[written:], tb.checkSum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(tb.checkSum))))

	return written
}
