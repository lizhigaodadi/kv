package utils

import (
	"github.com/hardcore-os/corekv/utils"
	"hash/crc32"
)

type EntryHandle func(e *Entry) error

func EstimateWalCodecSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 + crc32.Size + utils.MaxHeaderSize
}
