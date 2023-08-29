package utils

import "hash/crc32"

var (
	CastageoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
