package utils

import (
	"bytes"
	"math/rand"
)

const (
	maxLevel = 4
)

type SkipList struct {
	height int
	header *Entry
}

type ValueStruct struct {
	Value []byte
}

type Entry struct {
	Key   []byte
	Value []byte
	level []*Entry
	score float64
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

	if preElement == nil {
		return ValueStruct{}
	}

	for i >= 0 {
		for next := preElement.level[i]; next != nil; next = preElement.level[i] {
			if c := compare(score, Key, next); c <= 0 {
				if c == 0 { /*找到了*/
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

	return ValueStruct{nil}
}

func (list *SkipList) Add(entry *Entry) error {
	score := list.calcScore(entry.Key)
	entry.score = score

	var prevElementHeaders [maxLevel]*Entry
	var prevElement *Entry

	/*找到正确的位置并将其加入进去*/
	i := list.height - 1
	prevElement = list.header
	for i >= 0 {
		prevElementHeaders[i] = prevElement /*记录在该层次访问的前一个元素*/
		/*检测是否大于该数值*/
		for next := prevElement.level[i]; next != nil; next = prevElement.level[i] {
			if c := compare(score, entry.Key, next); c <= 0 {
				if c == 0 {
					/*直接替换值即可*/
					next.Value = entry.Value
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
	for ; ; i++ {
		if rand.Intn(2) == 0 {
			if i < maxLevel {
				return i
			} else {
				return maxLevel
			}
		}
	}
}
