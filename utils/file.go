package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
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
func SyncDir(dir string) error {
	/*同步某个目录，用于删除或新增某个文件后*/
	fp, err := OpenDir(dir)
	defer fp.Close()
	if err != nil {
		return err
	}
	if err = fp.Sync(); err != nil {
		return err
	}
	return nil
}

func OpenDir(dir string) (*os.File, error) {
	/*打开一个文件夹*/
	return os.Open(dir)
}

func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%5d.%s", id, FileSuffixNameSSTable))
}
func FiNameWalLog(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%5d.%s", id, FileSuffixNameWalLog))
}
