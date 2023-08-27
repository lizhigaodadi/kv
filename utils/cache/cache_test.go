package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheBasicCRUD(t *testing.T) {
	cache := NewCache(5)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
		fmt.Printf("set %s: %s\n", key, cache)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		if ok {
			fmt.Printf("get %s: %s\n", key, cache)
			assert.Equal(t, val, res)
			continue
		}
		assert.Equal(t, res, nil)
	}
	fmt.Printf("at last: %s\n", cache)
}

type A struct {
	a int
}

func (a *A) Show() {
	fmt.Printf("HelloWorld")
}
func (a A) ShowAddress() {
	fmt.Printf("pointer address: %p\n", &a)
}
func (a *A) ShowAddressPtr() {
	fmt.Printf("pointer address: %p\n", a)
}

func TestString(t *testing.T) {
	a := A{}
	ap := &a
	a.ShowAddress()
	ap.ShowAddress()
	fmt.Printf("------------------------\n")
	a.ShowAddressPtr()
	ap.ShowAddressPtr()

}
