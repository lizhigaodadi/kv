package utils

import (
	"bytes"
	"fmt"
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
