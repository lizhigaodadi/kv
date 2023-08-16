package utils

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	maxLevel       = 4
	levelThreshold = 0.5
)

type SkipList struct {
	height     int
	headOffset uint32 /*head node position*/
	ref        int32  /*被引用次数*/
	arena      *Arena
	OnClose    func()
}

func (s *SkipList) getNext(n *node, level int) *node {
	/*TODO: 通过内存偏移来找到相应的节点*/
	return &node{}
}

func (list *SkipList) newSkipListIterator() *SkipListIterator {
	return &SkipListIterator{
		indepre:  0,
		curEntry: list.getHeader(),
		SkipList: list,
	}
}

type SkipListIterator struct {
	indepre  int //当前的位置
	curEntry *Entry
	SkipList *SkipList /*迭代器关于的对象*/
}

func (it *SkipListIterator) Valid() bool {
	if it.curEntry == nil {
		return true
	} else {
		return false
	}
}

func (it *SkipListIterator) Rewind() {
	it.curEntry = it.SkipList.getHeader().level[0] /*第二个节点的第一层次才是开始节点*/
	it.indepre = 0
}
func (it *SkipListIterator) next() {
	it.curEntry = it.curEntry.level[0]
	it.indepre++
}
func (it *SkipListIterator) Item() *Entry {
	return it.curEntry
}

func NewEntry(Key []byte, Value []byte) *Entry {
	return &Entry{
		Key:   Key,
		Value: Value,
	}
}
func NewSkipList(test_num int) *SkipList {

	list := &SkipList{}
	list.header = newHeader()
	list.height = maxLevel
	list.rwMutepre = sync.RWMutepre{}

	return list
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

func (s *SkipList) getHeight() int {
	return s.height
}

func (s *SkipList) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

/*展示出整个skip_list的结构*/
func (list *SkipList) Draw(align bool) {
	reverseTree := make([][]string, list.getHeight())

	for level := list.getHeight() - 1; level >= 0; level-- {
		/*遍历整个表*/
		var nodeStr string
		next := list.header.level[level]
		for next != nil {
			nodeStr = fmt.Sprintf("%s(%s)", next.Key, next.Value)

			reverseTree[level] = append(reverseTree[level], nodeStr)

			next = next.level[level]
		}
	}

	/*根据是否平衡来展示字符串*/

	if align {
		/*需要额外判断是否有着间隔*/
		for level := list.getHeight() - 1; level >= 0; level-- {
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
	for level := list.getHeight() - 1; level >= 0; level-- {
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

/*计算两个分数大小*/
func compare(score float64, Key []byte, entry *Entry) int {
	if score == entry.score {
		return bytes.Compare(Key, entry.Key)
	}

	if score < entry.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) Search(Key []byte) ValueStruct {
	/*获取Key的score*/
	score := list.calcScore(Key)

	i := list.height - 1
	var preElement *Entry
	preElement = list.header
	//if preElement == nil {
	//	return ValueStruct{}
	//}
	list.rwMutepre.RLock()

	for i >= 0 {
		for next := preElement.level[i]; next != nil; next = preElement.level[i] {
			if c := compare(score, Key, next); c <= 0 {
				if c == 0 { /*找到了*/
					list.rwMutepre.RUnlock()
					return ValueStruct{
						Value: next.Value,
					}
				}
				/*比这个小进入下一层或结束*/
				break
			}

			/*更新*/
			preElement = next
		}
		i--
	}
	list.rwMutepre.RUnlock()

	return ValueStruct{0, nil}
}

func (list *SkipList) Add(entry *Entry) error {
	score := list.calcScore(entry.Key)
	entry.score = score

	var prevElementHeaders [maxLevel]*Entry
	var prevElement *Entry

	/*找到正确的位置并将其加入进去*/
	i := list.height - 1
	prevElement = list.header
	list.rwMutepre.Lock()
	for i >= 0 {
		prevElementHeaders[i] = prevElement /*记录在该层次访问的前一个元素*/
		/*检测是否大于该数值*/
		for next := prevElement.level[i]; next != nil; next = prevElement.level[i] {
			if c := compare(score, entry.Key, next); c <= 0 {
				if c == 0 {
					/*直接替换值即可*/
					next.Value = entry.Value
					list.rwMutepre.Unlock()

					return nil
				}
				/*进入下一轮轮回或者跳出*/

				break
			}
			/*继续下一个*/
			prevElement = next
			prevElementHeaders[i] = prevElement /*记录在该层次访问的前一个元素*/
		}
		i--

	}

	/*在当前位置进行插入*/
	level := RandLevel()
	entry.level = make([]*Entry, level)

	//开始在所有层次进行插入操作
	for i := 0; i < level; i++ {
		entry.level[i] = prevElementHeaders[i].level[i]
		prevElementHeaders[i].level[i] = entry
	}

	list.rwMutepre.Unlock()

	return nil
}

func newHeader() *Entry {
	return &Entry{
		Key:   nil,
		Value: nil,
		score: -1,
		level: make([]*Entry, maxLevel),
	}
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

func (s *node) getKey(arena *Arena) []byte {
	/*TODO: 通过Arena内存池获取Key*/
	return make([]byte, 5)
}

func (s *node) setValue(arena *Arena, vo uint64) {
	/*TODO: 设置相关的值*/
	atomic.StoreUint64(&s.value, vo)
}

/*获取该节点在h层的next指针*/
func (s *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&s.tower[h])
}

/*通过cas 操作更新节点的next指针*/
func (s *node) casnextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&s.tower[h], old, val)
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

	prev[maxLevel] = s.headOffset

	for i := int(listHeight) - 1; i >= 0; i-- {
		before, next := s.findSpliceForLevel(key, prev[i+1], i)

		if before == next { /*直接对节点的值进行替换*/
			vo := s.arena.putVal(v)

		}
	}
}
