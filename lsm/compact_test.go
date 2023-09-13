package lsm

import (
	"fmt"
	"testing"
)

func TestGetMergeIteratorCount(t *testing.T) {
	count := GetMergeIteratorCount(34)
	fmt.Printf("count :%d\n", count)
}
