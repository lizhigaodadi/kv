package utils

import (
	"encoding/binary"
	"errors"
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

func ParseKey(key []byte) []byte {
	CondPanic(len(key) <= 8, KeyFormatErr)
	return key[:len(key)-8]
}

func KeyWithTs(key []byte, ts uint64) []byte {
	size := len(key) + 8 //实际需要的长度大小
	buf := make([]byte, size)
	CondPanic(copy(buf[0:len(key)], key) != len(key), errors.New("Copy Error"))
	binary.BigEndian.PutUint64(buf[len(key):], math.MaxUint64-ts)
	return buf
}
