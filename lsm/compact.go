package lsm

import "sync"

/*lsm压缩器*/
type Compacter struct {
	wg sync.WaitGroup
}

/*这是后台线程工作*/
/*压缩执行计划*/
type CompactDef struct {
	fromTables []*Table      /*要被压缩的Tables*/
	toTables   []*Table      /*要被合成压缩的Tables*/
	fromLevel  int           /*被压缩的层数*/
	toLevel    int           /*要被压缩去的层数*/
	tb         *TableBuilder /*构建压缩所需的builder*/
}

type Targets struct {
	realSz   []uint32
	targetSz []uint32
}

type KeyRange struct { /*比较key的范围使用的*/
	left  []byte
	right []byte
}
