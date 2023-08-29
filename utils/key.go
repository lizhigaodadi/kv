package utils

import (
	"encoding/binary"
	"math"
)

func SameKey(key1, key2 []byte) bool {
	/*TODO: 判断这两个key是否相等*/
	return true
}

func ParseTs(key []byte) uint64 {
	/*解析key来获取到时间戳*/
	if len(key) <= 8 {
		return 0
	}

	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}
