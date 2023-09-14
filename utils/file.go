package utils

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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
	var expectedCheckSum uint64
	if len(expected) == 8 {
		expectedCheckSum = BytesToU64(expected)
	} else {
		expectedCheckSum = uint64(BytesToU32(expected))
	}

	if actualCheckSum != expectedCheckSum {
		return errors.New("Verify CheckSum Failed")
	}
	return nil /*校验成功*/
}

func VerifyCrc32(actual []byte, expectedCrc32 uint32) error {
	actualCrc32 := crc32.Checksum(actual, CastageoliCrcTable)
	if actualCrc32 == expectedCrc32 {
		return nil
	}
	return errors.New("Verify Crc32 Failed")
}

func CalculateChecksum(b []byte) uint64 {
	return uint64(crc32.Checksum(b, CastageoliCrcTable))
}

func CalculateCheckSumU32(b []byte) uint32 {
	return crc32.Checksum(b, CastageoliCrcTable)
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
	return filepath.Join(dir, fmt.Sprintf("%05d.%s", id, FileSuffixNameSSTable))
}
func FileNameWalLog(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.%s", id, FileSuffixNameWalLog))
}

func FileNameTemp(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.%s", id, FileSuffixNameTemp))
}

func FID(name string) uint64 {
	name = path.Base(name) /*删除路径*/
	if !strings.HasSuffix(name, ".sst") {
		return 0 /*根本不是sst文件，无效操作*/
	}
	/*删除后缀名*/
	name = strings.TrimSuffix(name, ".sst")
	/*转化为普通的id*/
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}
