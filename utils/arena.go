package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
)

const (
	MaxNodeSize = 32
)

type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

func newArena(bufSize uint32) *Arena {
	return &Arena{
		n:          1,
		buf:        make([]byte, bufSize),
		shouldGrow: false,
	}
}

func (a *Arena) allocate(sz uint32) uint32 {
	/*更新一下分配的内存*/
	offset := atomic.AddUint32(&a.n, sz)

	/*判断一下内存是否够*/
	if len(a.buf)-int(offset) < MaxNodeSize {
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
	}
}

func (arena *Arena) getNode(nodeOffset uint32) *node {
	/*TODO:通过偏移量来找到目标节点*/
	return &node{}
}

func (arena *Arena) putVal(v ValueStruct) uint32 {
	/*TODO: 在内存池中存储相应的节点并将偏移量返回*/
	return 0
}

func (arena *Arena) getNodeOffset(node *node) uint32 {
	/*TODO: 通过节点在内存池中获取相应的偏移量*/

	return 0
}

func AssertTrue(b bool) {
	if !b {
		log.Fatal("%+v", errors.Errorf("Assert failed"))
	}
}

func (arena *Arena) getKey(keyOffset uint32, keySize uint16) []byte {
	/*TODO: 通过key偏移量和key大小在内存池中取出key并返回*/
	return make([]byte, keySize)
}

func (arena *Arena) getVal(valOffset uint32, valSize uint32) ValueStruct {
	/*TODO: 根据偏移量和大小在内存池中获取Value并将其封装为ValueStruct*/
	return ValueStruct{}
}
