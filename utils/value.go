package utils

import "encoding/binary"

/*使用大端字节的方式来进行解码*/
func ByteToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

/*通过大端字节的方式来编码序列化*/
func U64ToBytes(data uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], data)
	return buf[:]
}

func BytesToU32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func U32ToBytes(data uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], data)
	return buf[:]
}
