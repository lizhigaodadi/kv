package utils

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
		n <<= 7
		if n == 0 {
			return i
		}
		i++
	}
}

/*valueStruct 所需要持久化的只有value 和 过期时间*/
func (v *ValueStruct) EncodeSize() uint32 {
	sz := len(v.Value) + 1
	/*计算时间戳的长度*/
	enc := sizeVarInt(v.ExpiresAt)

	return uint32(sz + enc)
}

func newNode(arena *Arena, v ValueStruct, h int) *node {
	/*TODO: 在内存池中开辟新的空间存储value*/
	return &node{}
}
