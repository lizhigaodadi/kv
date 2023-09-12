package lsm

import "sync"

/*lsm压缩器*/
type Compacter struct {
	wg sync.WaitGroup
}

/*压缩执行计划*/
type CompactDef struct {
	fromTables []*Table      /*要被压缩的Tables*/
	toTables   []*Table      /*要被合成压缩的Tables*/
	fromLevel  int           /*被压缩的层数*/
	toLevel    int           /*要被压缩去的层数*/
	tb         *TableBuilder /*构建压缩所需的builder*/
}

type Targets struct {
	fileSz    []uint32
	targetSz  []uint32
	baseLevel int
}

type KeyRange struct { /*比较key的范围使用的*/
	left  []byte
	right []byte
}

/*Level优先级的参考依据*/
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            *Targets
}

/*记录了当前数据库的整个压缩状态*/
type compactStatus struct {
	m      sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

type levelCompactStatus struct {
	levelNum int
	delSize  uint32   /*记录了当前层级中即将要被删除的部分所占比重*/
	kr       KeyRange /*记录了当前层级中键值对所占的范围*/
}

func (lm *levelManager) runOnce(id int) bool {
	/*先获取到有哪些层级需要被压缩*/
	prios := lm.pickCompactLevel()
	if id == 0 { /*0层级需要被移动，优先0层级*/
		prios = moveL0ToFront(prios)
	}

	/*判断是否需要压缩*/

	return false
}

func moveL0ToFront(prios []compactionPriority) []compactionPriority {
	/*检测是否有level0在这些存在被压缩可能的层级中*/
	level0 := -1
	for i, prio := range prios {
		if prio.level == 0 {
			level0 = i
			break
		}
	}
	if level0 == -1 {
		return prios /*没有找到level0*/
	}
	/*找到了level0*/
	newPrios := prios[:0]
	newPrios = append(newPrios, prios[level0])
	newPrios = append(newPrios, prios[:level0]...)
	newPrios = append(newPrios, prios[level0+1:]...)

	return newPrios

}
