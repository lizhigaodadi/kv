package lsm

import (
	"github.com/pkg/errors"
	"kv/utils"
)

type Iterator struct {
	it    Item
	iters []utils.Iterator
}

type Item struct {
	E *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.E
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

type CompactIterator struct {
	/*TODO:这是真正的存储数据的迭代器，支持多种不同迭代器*/
	iter utils.Iterator
	ti   *TableIterator /*三者中只有一个不为nil*/
	li   *LevelIterator
	bi   *BlockIterator
}

func NewCompactIterator(iter utils.Iterator) *CompactIterator {
	ci := &CompactIterator{
		iter: iter,
	}
	ti, ok := iter.(*TableIterator)
	if ok {
		ci.ti = ti
	}
	li, ok := iter.(*LevelIterator)
	if ok {
		ci.li = li
	}
	bi, ok := iter.(*BlockIterator)
	if ok {
		ci.bi = bi
	}

	return ci
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
	mi.lastKey = mi.Item().Entry().Key /*更新一下上次使用的Key*/

	if *mi.small == mi.right { /*右边执行*/
		mi.right.iter.Next()
	} else {
		mi.left.iter.Next()
	}

	mi.fix()
}

func (mi *MergeIterator) Item() utils.Item {
	/*获取到当前的item元素*/
	/*判断一下是否有效*/
	if mi.small != nil && mi.small.valid {

		//for mi.small.valid {
		//	e := mi.small.e
		//	/*判断一下这个entry是否失效了*/
		//	lastKey := utils.ParseKey(mi.lastKey)
		//	thisKey := utils.ParseKey(e.Key)
		//	if bytes.Compare(lastKey, thisKey) == 0 {
		//		mi.Next()
		//	} else {
		//		mi.lastKey = e.Key
		//		return &Item{
		//			e: e,
		//		}
		//	}
		//}
	}

	return &Item{
		E: nil,
	}
}

func (mi *MergeIterator) LastKey() []byte {
	return mi.lastKey
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

func NewNode(i utils.Iterator) *node {
	n := &node{}
	mi, ok := i.(*MergeIterator)
	if ok {
		n.mergeIter = mi
	}
	/*TODO:这里可能还有其他的一些种类的迭代器需要被补充*/
	return n
}

func (n *node) addIterator(i utils.Iterator) {
	/*判断一下这个是什么类型的迭代器*/
	n.iter = i
	mi, ok := i.(*MergeIterator)
	if ok {
		n.mergeIter = mi
	}
	ci, ok := i.(*MergeIterator)
}

/*并不设置左右节点*/
func NewMergeIterator() *MergeIterator {
	return &MergeIterator{
		err:     nil,
		lastKey: make([]byte, 0),
		left: node{
			valid: true,
		},
		right: node{
			valid: true,
		},
	}
}

func (mi *MergeIterator) Valid() bool {
	/*TODO:判断一下当前迭代器是否可以继续工作*/
	return false
}

func (mi *MergeIterator) Seek(key []byte) {
	/*TODO:对于这个方法暂时不确定如何实现比较合适，先用于兼容接口作用*/
}

func (n *node) setKey() {
	switch {
	case n.mergeIter != nil: /*这只是单纯的一个传递数据的迭代器*/
		n.valid = n.mergeIter.small.valid
		if n.valid {
			n.e = n.mergeIter.small.e
		}
		/*TODO:一些真正存储数据迭代器的逻辑以后继续补充*/
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.e = n.iter.Item().Entry()
		}

	}
}

/*TODO:---------------------------以下方法后续完善-------------------------------*/
func (li *LevelIterator) Next() {
	/*判断一下当前还能走吗*/
	if li.iters[li.idx].Valid() {
		li.iters[li.idx].Next()
	} else {
		if li.idx < len(li.iters)-1 {
			li.idx++
			li.iters[li.idx].Rewind()
		}
	}
}

func (li *LevelIterator) Valid() bool {
	/*看看自己是不是最后一个*/
	if li.idx == len(li.iters)-1 {
		return li.iters[li.idx].Valid()
	} else { /*还没有到最后*/
		return true
	}
}

func (li *LevelIterator) Rewind() {
	/*所有迭代器都Rewind*/
	for _, iter := range li.iters {
		iter.Rewind()
	}
	li.idx = 0
}

func (li *LevelIterator) Item() utils.Item {
	e := li.iters[li.idx].Item()
	return e
}

func (li *LevelIterator) Close() {
	for _, iter := range li.iters {
		iter.Close()
	}
}

func (li *LevelIterator) Seek(key []byte) {
	/*TODO:暂时没有头绪*/
}
