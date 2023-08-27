package cache

import (
	"container/list"
	"fmt"
)

/*WindowsLRU对象*/
type WindowsLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	stage    int /*表示这是哪个段的数据*/
	key      uint64
	conflict uint64
	Value    interface{}
}

func NewWindowsLRU(cap int, data map[uint64]*list.Element) *WindowsLRU {
	return &WindowsLRU{
		cap:  cap,
		data: data,
		list: list.New(),
	}
}

func (lru *WindowsLRU) Get(item *list.Element) {
	/*判断是否在其中的操作由外部完成*/
	/*直接移动到前面即可*/
	lru.list.MoveToFront(item)
}

/*将数据添加到LRU中，首先要检查是否已经在LRU里面了*/
func (lru *WindowsLRU) Add(item storeItem) (eItem storeItem, evicted bool) {
	if lru.list.Len() < lru.cap { /*直接添加即可*/
		lru.data[item.key] = lru.list.PushFront(&item)
		return storeItem{}, false
	}

	/*取出最后一个,abandonedItem必然不为nil所以不用判断*/
	abandonedItem := lru.list.Back()
	abandonedStoreItem := abandonedItem.Value.(*storeItem)

	delete(lru.data, abandonedStoreItem.key)
	eItem, *abandonedStoreItem = *abandonedStoreItem, item /*交换赋值，减少内存的开辟*/

	/*现在添加新的元素进去*/
	lru.data[item.key] = abandonedItem

	return eItem, true
}

func (lru *WindowsLRU) String() string {
	var s string
	for e := lru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).Value)
	}

	return s
}
