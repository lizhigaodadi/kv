package cache

import (
	"math/rand"
	"time"
)

type cmsRow []byte

func NewCmsRow(countersNum uint64) cmsRow {
	return make([]byte, countersNum/2)
}

const (
	depth = 4
)

type cmsSketch struct {
	rows  [depth]cmsRow
	seeds [depth]uint64
	mask  uint64
}

func NewCmsSketch(countersNum uint64) *cmsSketch {
	if countersNum == 0 {
		panic("cmSketch: invalid number")
	}
	/*调整为2的整数倍*/
	countersNum = next2Power(countersNum)
	/*创建cms*/
	cs := &cmsSketch{mask: countersNum - 1}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < depth; i++ {
		cs.rows[i] = NewCmsRow(countersNum)
		cs.seeds[i] = source.Uint64()
	}

	return cs
}

/*清空所有的计数槽位*/
func (cs *cmsSketch) Clear() {
	for i := 0; i < depth; i++ {
		cs.rows[i].clear()
	}
}
func (cr cmsRow) clear() {
	len := len(cr)
	for i := 0; i < len; i++ {
		cr[i] = 0
	}
}

func (cs *cmsSketch) Increment(h uint64) {
	for i := 0; i < depth; i++ {
		cs.rows[i].increment((h ^ cs.seeds[i]) & cs.mask)
	}
}

func (cs *cmsSketch) Estimate(h uint64) byte {
	min := byte(255) /*最小值*/
	for i := 0; i < depth; i++ {
		cur := cs.rows[i].get((h ^ cs.seeds[i]) & cs.mask)
		if cur < min {
			min = cur
		}
	}
	return min
}

func (cr cmsRow) get(pos uint64) byte {
	i := pos / 2
	s := (pos & 1) * 4
	return (cr[i] >> s) & 0x0f
}

func (cr cmsRow) increment(pos uint64) {
	i := pos / 2
	s := (pos & 1) * 4
	v := (cr[i] >> s) & 0x0f
	if v < 15 {
		cr[i] += 1 << s
	}
}

func (cs *cmsSketch) Reset() {
	for i, _ := range cs.rows {
		cs.rows[i].reset()
	}
}

func (cr cmsRow) reset() {
	for _, i := range cr {
		cr[i] = (cr[i] >> 1) & 0x77
	}
}

func next2Power(x uint64) uint64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}
