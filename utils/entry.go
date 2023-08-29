package utils

import (
	"encoding/binary"
)

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64
	Version   uint64
}

type Entry struct {
	Key      []byte
	Value    []byte
	ExpireAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int
	ValThreshold int
}

func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarInt(uint64(e.ExpireAt))
	enc += sizeVarInt(uint64(e.Meta))
	return uint32(sz + enc)
}

func (e *Entry) getValue() []byte {
	return e.Value
}
func (e *Entry) getKey() []byte {
	return e.Key
}

/*计算n持久化所需要多少字节位置*/
func sizeVarInt(n uint64) int {
	i := 1
	for {
		n >>= 7
		if n == 0 {
			return i
		}
		i++
	}
}

/*valueStruct 所需要持久化的只有value 和 过期时间*/
func (v *ValueStruct) EncodeSize() uint32 {
	sz := len(v.Value)
	/*计算时间戳的长度*/
	enc := sizeVarInt(v.ExpiresAt)

	return uint32(sz + enc)
}

func (v *ValueStruct) EncodeValue(b []byte) uint32 {
	sz := binary.PutUvarint(b[:], v.ExpiresAt)
	n := copy(b[sz:], v.Value)
	//fmt.Printf("sz: %d n: %d\n", sz, n)

	return uint32(sz + n)
}

func (v *ValueStruct) DecodeValue(b []byte) {
	//v.Meta = b[0]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b)
	//fmt.Printf("decode: sz:%d b len: %d", sz, len(b))
	v.Value = b[sz:]
}

func newNode(arena *Arena, key []byte, v ValueStruct, h int) *node {
	/*在内存池中开辟新的空间存储value*/
	nodeOffset := arena.putNode(uint16(h))
	keyOffset := arena.putKey(key)
	keySize := len(key)
	valOffset := arena.putVal(v)
	//fmt.Printf("valOffset: %d\n", valOffset)
	valSize := v.EncodeSize()
	value := encodeValue(valOffset, uint32(valSize))

	node := arena.getNode(nodeOffset)
	node.value = value
	node.keySize = uint16(keySize)
	node.height = uint16(h)
	node.keyOffset = keyOffset

	return node
}
