package utils

import (
	"fmt"
	"testing"
	"unsafe"
)

type A struct {
	a struct{}
	b uint64
	c uint64
}

type B struct {
	b uint64
	a struct{}
	c uint64
}

type C struct {
	b uint64
	c uint64
	a struct{}
}

type D struct {
	b uint32
	c uint32
	a struct{}
}

func TestNullStructAlign(t *testing.T) {
	fmt.Printf("sizeof A : %d\n", unsafe.Sizeof(A{}))
	fmt.Printf("sizeof B : %d\n", unsafe.Sizeof(B{}))
	fmt.Printf("sizeof C : %d\n", unsafe.Sizeof(C{}))
	fmt.Printf("sizeof D : %d\n", unsafe.Sizeof(D{}))

}

func TestArenaMemoryAlignment(t *testing.T) {
	/*Test Memory Align*/
	structSize := unsafe.Sizeof(node{})
	filedSize := 8 + 4 + 2 + 2 + 4*maxLevel

	fmt.Printf("structSize: %d\tfiledSize: %d\n", structSize, filedSize)

}
