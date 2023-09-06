package utils

import (
	"hash/crc32"
	"os"
)

var (
	CastageoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

/*manifest 相关的常量*/
const (
	ManifestFileName                  = "manifest"
	ManifestRewriteName               = "remanifest"
	DefaultFileAclMask                = 0666
	DefaultDeletionsRewriteThreshold  = 1 << 8
	DefaultOpenFileFlag               = os.O_RDWR | os.O_CREATE
	MagicVersion                      = 1
	MagicText                         = 7838
	MagicLen                          = 8
	ManifestDeletionsRewriteThreshold = 1000
	ManifestDeletionRatio             = 10
)

/*文件拓展名相关的常量*/
const (
	FileSuffixNameSSTable = ".sst"
	FileSuffixNameWalLog  = ".wal"
)
