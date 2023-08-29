package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

func CompareKeys(key1, key2 []byte) int {
	if len(key1) <= 8 || len(key2) <= 8 {
		panic(fmt.Errorf("%s,%s len < 8 ", string(key1), string(key2)))
	}

	cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8])
	if cmp != 0 {
		return cmp
	}

	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])

}

func VerifyCheckSum(actual []byte, expected []byte) error {
	actualCheckSum := uint64(crc32.Checksum(actual, CastageoliCrcTable))
	expectedCheckSum := ByteToU64(expected)

	if actualCheckSum != expectedCheckSum {
		return fmt.Errorf("Verify CheckSum Failed")
	}
	return nil /*校验成功*/
}

func CalculateChecksum(b []byte) uint64 {
	return uint64(crc32.Checksum(b, CastageoliCrcTable))
}
