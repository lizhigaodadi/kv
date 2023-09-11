package lsm

import (
	"github.com/pkg/errors"
	"kv/utils"
	"log"
)

type Iterator struct {
	it    Item
	iters []utils.Iterator
}

type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}

type TableIterator struct {
	t     *Table
	kr    KeyRange /*用于给TableIterator进行排序使用*/
	idx   int      /*表示当前的迭代器位置*/
	iters []*BlockIterator
}

func NewTableIterator(t *Table) *TableIterator {
	/*TODO:暂时不要进行初始化，因为可能会导致初始化过多的oom*/

}

func (ti *TableIterator) Next() { /*移动到下一个位置*/
	/*判断迭代器是否生效*/
	if !ti.valid() { /*迭代器失效了*/
		log.Fatal("TableIterator Invalid")
		return
	}
	/*判断当前分迭代器是否已经到达了尽头*/
	if ti.iters[ti.idx].HasNext() { /*判断是否还有下一个*/
		if ti.idx < len(ti.iters)-1 { /*移动到下一个位置*/
			ti.idx++
			ti.iters[ti.idx].Rewind() /*移动到起始位置*/
		} else { /*已经到了尽头*/
			return
		}
	}

	/*移动到下一个位置*/
	ti.iters[ti.idx].Next()
}

func (ti *TableIterator) Item() utils.Item {
	if !ti.valid() {
		return &Item{
			e: nil,
		}
	}
	return ti.iters[ti.idx].Item()
}

func (ti *TableIterator) valid() bool {
	if len(ti.iters) == 0 {
		return false
	}
	return ti.idx >= 0 && ti.idx < len(ti.iters)
}

func (ti *TableIterator) Rewind() {
	/*对所有的迭代器都进行一次rewind*/
	for _, iter := range ti.iters { /*rewind*/
		iter.Rewind()
	}
	ti.idx = 0
}

func (ti *TableIterator) Close() {
	for _, iter := range ti.iters { /*rewind*/
		iter.Close()
	}
}

func (ti *TableIterator) Seek(key []byte) {
	/*TODO:暂时没什么头绪有一个比较好的解决方案*/
}

type LevelIterator struct {
	idx   int
	kr    KeyRange /*表示整个层级中包含的key范围*/
	iters []*TableIterator
}

/*合并时，使用的迭代器*/
type MergeIterator struct {
	lastKey []byte /*用于记录该迭代器中获取的上一个迭代器*/
	left    node
	right   node
	small   *node
	err     error /*因为要符合接口的规范，我们异常只能嵌入到迭代器中*/
}

type node struct {
	valid     bool /*判断该节点是否有效*/
	e         *utils.Entry
	iter      utils.Iterator
	mergeIter *MergeIterator
}

func (mi *MergeIterator) Next() {
	/*判断两个节点中哪一个更小*/
	if mi.small == nil || !mi.small.valid { /*已经到了底部了*/
		mi.err = errors.New("Not Found")
		return
	}

	if *mi.small == mi.right { /*右边执行*/
		mi.right.iter.Next()
	} else {
		mi.left.iter.Next()
	}

	mi.fix()
}

func (mi *MergeIterator) Item() Item {
	/*获取到当前的item元素*/
	/*判断一下是否有效*/
	if mi.small != nil && mi.small.valid {
		return Item{
			e: mi.small.e,
		}
	}

	return Item{
		e: nil,
	}
}

func (mi *MergeIterator) Rewind() {
	if mi.left.valid {
		mi.left.iter.Rewind()
	}
	if mi.right.valid {
		mi.right.iter.Rewind()
	}
	mi.fix() /*重新确定当前的最小的值*/
}

func (mi *MergeIterator) fix() {
	/*分别获取两个node的当前的item元素*/
	/*判断是否有效*/
	var leftItem, rightItem *utils.Entry
	if mi.left.valid {
		leftItem = mi.left.iter.Item().Entry()
	}
	if mi.right.valid {
		rightItem = mi.right.iter.Item().Entry()
	}

	/*比较key的大小*/
	cmp := utils.CompareKeys(leftItem.Key, rightItem.Key)
	if cmp >= 0 { /*我们优先返回更小的*/
		/*设置新的迭代器*/
		mi.small = &mi.right
	} else {
		mi.small = &mi.left
	}

}

func (mi *MergeIterator) Close() {
	if mi.left.valid {
		mi.left.iter.Close()
	}
	if mi.right.valid {
		mi.right.iter.Close()
	}
}
