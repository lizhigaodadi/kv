package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	MaxNodeSize = 32
	/*nodeAligns代表了node内存对齐时最大的填补偏移量*/
	nodeAlign   = uint32(unsafe.Sizeof(uint64(0))) - 1
	maxNodeSize = unsafe.Sizeof(node{})
)

type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
	mm         sync.Mutex
}

func newArena(bufSize uint32) *Arena {
	return &Arena{
		n:          1,
		buf:        make([]byte, bufSize),
		shouldGrow: false,
	}
}

/*返回我们内存分配的起始偏移量*/
func (a *Arena) allocate(sz uint32) uint32 {
	/*更新一下分配的内存*/
	offset := atomic.AddUint32(&a.n, sz)

	/*判断一下内存是否够*/
	if len(a.buf)-int(offset) < MaxNodeSize {
		//a.mm.Lock()
		//if len(a.buf)-int(offset) < MaxNodeSize {

		/*将内存翻倍一下*/
		growBy := uint32(len(a.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}

		newBuf := make([]byte, int(growBy)+len(a.buf))
		/*开始全量复制*/
		AssertTrue(len(a.buf) == copy(newBuf, a.buf))

		a.buf = newBuf
		//}
		//a.mm.Unlock()
	}

	return offset - sz
}

func (a *Arena) putKey(key []byte) uint32 {
	/*存储一个Key对象进内存池中*/
	keySize := len(key)
	keyOffset := a.allocate(uint32(keySize))
	buf := a.buf[keyOffset : keyOffset+uint32(keySize)]
	/*全量复制*/
	AssertTrue(copy(buf, key) == keySize)

	return keyOffset
}

func (a *Arena) putNode(height uint16) uint32 {
	sizeOffset := unsafe.Sizeof(uint32(0))
	unusedSize := int(sizeOffset) * (int(maxLevel) - int(height))

	l := MaxNodeSize - uint32(unusedSize) + uint32(nodeAlign) /*要分配的内存大小，+nodeAlign是为了考虑到内存对齐最差的情况*/

	n := a.allocate(l)

	m := (n + nodeAlign) &^ nodeAlign /*将n向下取整取得nodeAlign 的整数倍*/
	return m
}

func (a *Arena) getNode(nodeOffset uint32) *node {
	if nodeOffset == 0 {
		return nil
	}
	/*通过偏移量来找到目标节点*/
	n := (*node)(unsafe.Pointer(&a.buf[nodeOffset]))
	return n
}

func (a *Arena) putVal(v ValueStruct) uint32 {
	/*在内存池中存储相应的节点并将偏移量返回*/

	vSize := v.EncodeSize() /*存储valueStruct 所需要的大小*/
	offset := a.allocate(vSize)

	v.EncodeValue(a.buf[offset:])

	return offset
}

func (a *Arena) getNodeOffset(node *node) uint32 {
	/* node即为byte数组的某一个元素的地址，我们获取byte的首个地址，然后相减即使偏移量*/
	bufOffset := uintptr(unsafe.Pointer(&a.buf[0]))
	nodeOffset := uintptr(unsafe.Pointer(node))

	return uint32(nodeOffset - bufOffset)
}

func AssertTrue(b bool) {
	if !b {
		log.Fatal("", errors.Errorf("Assert failed"))
	}
}

func (arena *Arena) getKey(keyOffset uint32, keySize uint16) []byte {
	/*通过key偏移量和key大小在内存池中取出key并返回*/
	return arena.buf[keyOffset : keyOffset+uint32(keySize)]
}

func (arena *Arena) getVal(valOffset uint32, valSize uint32) (ret ValueStruct) {
	/*根据偏移量和大小在内存池中获取Value并将其封装为ValueStruct*/
	//fmt.Printf("valSize: %d\n", valSize)
	ret.DecodeValue(arena.buf[valOffset : valOffset+valSize])

	return
}

/*返回已经占用的内存大小*/
func (a *Arena) Size() uint32 {
	return atomic.LoadUint32(&a.n)
}
