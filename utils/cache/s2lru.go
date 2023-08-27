package cache

import (
	"container/list"
	"fmt"
)

type SegmentedLRU struct {
	data                       map[uint64]*list.Element
	stageOneCap, stageTwoCap   uint64
	stageOneList, stageTwoList *list.List
}

const ( /*iota是用于定义枚举的,STAGE_TWO自动赋值为2*/
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func NewSegmentedLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap uint64) *SegmentedLRU {
	return &SegmentedLRU{
		data:         data,
		stageOneCap:  stageOneCap,
		stageTwoCap:  stageTwoCap,
		stageOneList: list.New(),
		stageTwoList: list.New(),
	}
}

func (lru *SegmentedLRU) Add(newItem storeItem) (evictItem storeItem, evcited bool) {
	if lru.stageOneList.Len() < int(lru.stageOneCap) || lru.Len() < lru.stageOneCap+lru.stageTwoCap { /*这种情况下我们直接进行插入*/
		lru.data[newItem.key] = lru.stageOneList.PushFront(&newItem)

		return storeItem{}, false
	}

	/*此时说明stageOne 或者 stageTwo和stageOne 满了*/
	/*淘汰stageOne中的最后一个元素*/
	lastItem := lru.stageOneList.Back()
	item := lastItem.Value.(*storeItem)

	/*从map中删除*/
	delete(lru.data, item.key)

	/*将指针指向的变量更改*/
	evictItem, *item = *item, newItem
	/*在map中添加新的元素*/
	lru.data[item.key] = lastItem
	lru.stageOneList.MoveToFront(lastItem)

	return evictItem, true
}

func (lru *SegmentedLRU) Get(item *list.Element) {
	sItem := item.Value.(*storeItem)
	/*判断一下是哪个区间的*/
	if sItem.stage == STAGE_TWO { /*直接移动到前面即可*/
		lru.stageTwoList.MoveToFront(item)
		return
	}
	/*此时item 是 STAGE_ONE 判断一下stageTwo是否已经满了*/
	if uint64(lru.stageTwoList.Len()) < lru.stageTwoCap { /*直接往里头添加即可*/
		e := lru.stageOneList.Back()
		targetItem := e.Value.(*storeItem)
		/*从第一阶段删除*/
		lru.stageOneList.Remove(e)
		lru.data[targetItem.key] = lru.stageTwoList.PushFront(targetItem)
		return
	}

	/*发现stageTwoList已经满了，我们进行交换操作*/
	e1 := lru.stageOneList.Back()
	e2 := lru.stageTwoList.Back()

	item1 := e1.Value.(*storeItem)
	item2 := e2.Value.(*storeItem)

	*item1, *item2 = *item2, *item1
	/*重新更新map*/
	lru.data[item1.key] = e1
	lru.data[item2.key] = e2

	/*更新stage状态*/
	item1.stage = STAGE_ONE
	item2.stage = STAGE_TWO

	/*二者都移动到最前端*/
	lru.stageOneList.MoveToFront(e1)
	lru.stageTwoList.MoveToFront(e2)

}

/* 这个并没有在链表中真实的删除，这是为了后续添加元素不用重复分配链表节点的内存空间了*/
func (lru *SegmentedLRU) victim() *storeItem {
	if lru.Len() < lru.stageOneCap+lru.stageTwoCap {
		return nil
	}

	v := lru.stageOneList.Back()
	if v.Value == nil {
		fmt.Printf("Hello World")

	}
	return v.Value.(*storeItem)
}

func (lru *SegmentedLRU) String() string {
	var s string
	//fmt.Printf("stageOneList len : %d", lru.stageOneList.Len())
	for e := lru.stageOneList.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).Value)
	}

	s += "|"
	//fmt.Printf("stageTwoList len : %d", lru.stageTwoList.Len())

	for e := lru.stageTwoList.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).Value)
	}

	return s
}

func (lru *SegmentedLRU) Len() uint64 {
	return uint64(lru.stageOneList.Len() + lru.stageTwoList.Len())
}
