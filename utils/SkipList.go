package utils

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
)

const (
	maxLevel       = 4
	levelThreshold = 0.5
)

type SkipList struct {
	height     int32
	headOffset uint32 /*head node position*/
	ref        int32  /*被引用次数*/
	arena      *Arena
	OnClose    func()
}

func (s *SkipList) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *SkipList) DecrRef() {
	atomic.AddInt32(&s.ref, -1)
}

func (s *SkipList) newSkipListIterator() *SkipListIterator {
	s.IncrRef()
	it := &SkipListIterator{
		SkipList: s,
	}

	it.Rewind()

	return it
}

type SkipListIterator struct {
	curNode  *node
	SkipList *SkipList /*迭代器关于的对象*/
}

func (it *SkipListIterator) Key() []byte { /*获取迭代器当前节点的Key*/
	AssertTrue(it.Valid())
	return it.curNode.getKey(it.SkipList.arena)
}

func (it *SkipListIterator) Value() ValueStruct { /*取出值并复制给ValueStruct*/
	/*获取value*/
	valOffset, valSize := decodeValue(it.curNode.value)

	return it.SkipList.arena.getVal(valOffset, valSize)
}

func (it *SkipListIterator) Seek(key []byte) {
	AssertTrue(it.Valid())
	it.curNode, _ = it.SkipList.findNear(key, false, true)
}

func (it *SkipListIterator) Valid() bool {
	if it.curNode != nil { /*判断下一个的节点是否为空*/
		return true
	} else {
		return false
	}
}

/*重新指向第一个节点*/
func (it *SkipListIterator) Rewind() {
	it.SeekToFirst()
}

func (it *SkipListIterator) SeekToFirst() {
	it.curNode = it.SkipList.getNext(it.SkipList.getHead(), 0)
}

func (it *SkipListIterator) Prev() {
	AssertTrue(it.Valid())

	it.curNode, _ = it.SkipList.findNear(it.Key(), true, false)
}

func (it *SkipListIterator) SeekForPrev(key []byte) { /*尽量往前移动*/
	AssertTrue(it.Valid())

	it.curNode, _ = it.SkipList.findNear(key, true, true)
}

func (it *SkipListIterator) Next() {
	AssertTrue(it.Valid())
	it.SkipList.getNext(it.curNode, 0)
}
func (it *SkipListIterator) Item() *Entry {
	return &Entry{
		Key:      it.Key(),
		Value:    it.Value().Value,
		ExpireAt: it.Value().ExpiresAt,
		Version:  it.Value().Version,
	}
}

func NewEntry(Key []byte, Value []byte) *Entry {
	return &Entry{
		Key:   Key,
		Value: Value,
	}
}
func NewSkipList(num int) *SkipList {
	s := &SkipList{}
	s.arena = newArena(uint32(num))
	headOffset := s.arena.putNode(maxLevel)

	s.height = 1
	s.ref = 1
	s.headOffset = headOffset
	return s
}

func (list *SkipList) calcScore(Key []byte) (score float64) {
	/*计算出该键在该list的分数(该分数并不是绝对)*/
	var hash uint64

	l := len(Key)
	if l > 8 { /*len最大就是8*/
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - 8*i)
		hash |= uint64(Key[i]) << shift
	}
	score = float64(hash)

	return
}

func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

func (s *SkipList) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

/*展示出整个skip_list的结构*/
func (s *SkipList) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())

	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		/*遍历整个表*/
		var nodeStr string
		next := s.getNext(s.getHead(), level)
		for next != nil {
			nodeStr = fmt.Sprintf("%s(%s)", next.getKey(s.arena), s.arena.getVal(next.getValueOffset()).Value)

			reverseTree[level] = append(reverseTree[level], nodeStr)

			next = s.getNext(next, level)
		}
	}

	/*根据是否平衡来展示字符串*/

	if align {
		/*需要额外判断是否有着间隔*/
		for level := s.getHeight() - 1; level >= 0; level-- {
			pos := 0
			for _, ele := range reverseTree[0] {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}

				pos++
			}
		}

	}
	/*直接展示即可*/
	for level := s.getHeight() - 1; level >= 0; level-- {
		pos := 0
		for _, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%v|\n", ele)
				break
			} else {

				fmt.Printf("%v->", ele)
			}
			pos++
		}

	}

}

func (s *SkipList) Search(key []byte) ValueStruct {
	node, _ := s.findNear(key, false, true)

	if node == nil {
		return ValueStruct{}
	}

	/*对比二者是否相等*/
	k := node.getKey(s.arena)

	if !SameKey(key, k) {
		return ValueStruct{}
	}
	/*发现相等了*/
	valOffset, valSize := node.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	return vs

}

func RandLevel() int {
	i := 1
	threshold := levelThreshold * 10
	for ; ; i++ {
		if rand.Intn(10) < int(threshold) {
			if i < maxLevel {
				return i
			} else {
				return maxLevel
			}
		}
	}
}

type node struct {
	value uint64 /*   因为value会变化，在并发情况下需要枷锁，编码在一次就可以使用CAS来操作
	value offset: uint32 (bits 0-31)
	value size: uint16 (bits 32-63)
	*/

	keyOffset uint32 /*不可变不需要加锁访问*/
	keySize   uint16 /*不可变不需要加锁访问*/
	height    uint16

	/*node 的next指针数组*/
	tower [maxLevel]uint32
}

/*解析value获取到value本体的偏移量和size*/
func decodeValue(value uint64) (valueOffset uint32, valueSize uint32) {
	valueOffset = uint32(value)
	valueSize = uint32(value >> 32)

	return
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value) /*cas 获取value的值*/

	return decodeValue(value)
}

func (n *node) getKey(arena *Arena) []byte {
	/*通过Arena内存池获取Key*/
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, vo uint64) {
	/*设置相关的值*/
	atomic.StoreUint64(&n.value, vo)
}

/*获取该节点在h层的next指针*/
func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

/*通过cas 操作更新节点的next指针*/
func (n *node) casnextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

/*
找到要插入的位置 返回该位置的前后两个节点的指针偏移量
这个before是插入位置前的任何一个节点(通常来说是头节点)
*/
func (s *SkipList) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		beforeNode := s.arena.getNode(before)

		next := beforeNode.getNextOffset(level)

		nextNode := s.arena.getNode(next)
		if nextNode == nil {
			/*已经找到了最后before有节点而next不存在节点，之后可以在before后面添加节点*/
			return before, next
		}

		/*开始比较节点*/
		nextKey := nextNode.getKey(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 { /*找到了相同的节点，返回特殊情况，供外层特殊处理*/
			return next, next
		}
		if cmp < 0 { /*当前key小于当前层次的下一个,找到了*/
			return before, next
		}

		/*当前情况就是大于的情况了，我们在这一层后面继续找*/
		before = next
	}
}

/*
找到最接近该key的节点
当less false 时 表示我们要最接近key左边的节点
当less true 时 标识我们要最接近key右边的节点
*/
func (s *SkipList) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	pre := s.getHead()

	level := int(s.getHeight()) - 1

	/*自上而下去寻找最近的节点*/
	for {
		next := s.getNext(pre, level)
		if next == nil {
			if level > 0 {
				level--
				break
			}

			/*说明已经走到了最底层的最后*/
			if !less { /*没有比key大的节点*/
				return nil, false
			}
			/*判断前一个节点是否为头节点*/
			if pre == s.getHead() {
				return nil, false
			}

			return pre, false
		}
		/*比较当前节点位置是否合适*/
		nextKey := next.getKey(s.arena)
		cmp := CompareKeys(key, nextKey)

		if cmp > 0 { /*当前key大于下一key继续往后移动*/
			pre = next
			continue
		}
		if cmp == 0 { /*说明找到了相等的key*/
			if allowEqual {
				return next, true
			}
			if !less { /*返回底层中比他大的节点*/
				return s.getNext(next, 0), false
			}
			/*此时我们可以进入下面的层次中找到比next更小的key*/
			if level > 0 {
				/*存在更细粒度的下层选择*/
				level--
				continue
			}

			if pre == s.getHead() {
				/*前一个节点仍然是头节点*/
				return nil, false
			}

			/*我们别无选择了找不到更合适的了*/
			return pre, false
		}
		/*发现了小于的情况  说明了 pre < key < next的情况*/
		if level > 0 {
			level--
			continue
		}

		if !less {
			return next, false
		}

		if pre == s.getHead() {
			return nil, false
		}

		return pre, false

	}
	return nil, false
}

func (s *SkipList) Add(e *Entry) {
	key, v := e.Key, ValueStruct{
		Meta:      e.Meta,
		Version:   e.Version,
		Value:     e.Value,
		ExpiresAt: e.ExpireAt,
	}

	/*遍历每一个层次来找到合适的位置*/
	listHeight := s.getHeight()

	/*分别用于记录前驱节点和后去节点的指针(偏移量)*/
	var prev [maxLevel + 1]uint32
	var next [maxLevel + 1]uint32

	prev[listHeight] = s.headOffset

	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)

		if prev[i] == next[i] { /*直接对节点的值进行替换*/
			vo := s.arena.putVal(v) /*存储并获取value的偏移量*/
			encValue := encodeValue(vo, v.EncodeSize())
			prevNode := s.arena.getNode(prev[i])
			prevNode.setValue(s.arena, encValue)
			return
		}
	}

	/*获取到一个随机的层级*/
	height := RandLevel()

	x := newNode(s.arena, key, v, height) /*新节点*/
	listHeight = s.getHeight()

	for height > int(listHeight) { /*如果height大于listHeight，我们就需要重新赋值*/
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			break
		}

		/*此时说明listHeight已经发生了变化，我们要拿到那个更新的值*/
		listHeight = s.getHeight()
	}

	/*开始给每一层的指针都进行更新*/
	for i := 0; i < height; i++ {
		for { /*不断重复的cas赋值操作*/
			if s.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}

			x.tower[i] = next[i]
			/*获取前一个节点*/
			preNode := s.arena.getNode(prev[i])

			if preNode.casnextOffset(i, next[i], s.arena.getNodeOffset(x)) {
				break /*赋值成功*/
			}

			/*cas失败进行重试,重新获取相应的位置*/
			prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)

			/*判断特殊情况*/
			if prev[i] == next[i] { /*在其他线程有人占有了这个位置，我们将其替换即可，指针的替换由早先占有的线程进行操作*/
				AssertTrue(i == 0)
				vo := s.arena.putVal(v)
				encValue := encodeValue(vo, v.EncodeSize())
				prevNode := s.arena.getNode(prev[i]) /*节点也需要更新，因为cas失败了*/
				prevNode.setValue(s.arena, encValue)

				return
			}
		}
	}
}

func encodeValue(valueOffset uint32, valueSize uint32) uint64 {
	return uint64(valueOffset) | uint64(valueSize)<<32
}

func (s *SkipList) Get(key []byte) ValueStruct {
	n, _ := s.findNear(key, false, true)
	if n == nil { /*没能找到*/
		return ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	/*判断是否相等*/
	if !SameKey(key, nextKey) { /*我们没能找到了节点*/
		return ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.Version = ParseTs(key)
	return vs
}

func (s *SkipList) getNext(curNode *node, level int) *node {
	nextNode := s.arena.getNode(curNode.tower[level])

	return nextNode
}

func (s *SkipList) findLast() *node { /*找到最后一个节点*/
	n := s.getHead()

	/*自上而下不断下沉去寻找*/
	i := int(s.getHeight()) - 1

	for {
		next := s.getNext(n, i)
		if next == nil { /*走到了该层次的尽头*/
			if i == 0 {
				if n == s.getHead() { /*这是一个空跳表*/
					return nil
				}
				return n
			}
			i--
		}
		/*next 不是空的 继续在这个层次寻找*/
		n = next

	}
}
