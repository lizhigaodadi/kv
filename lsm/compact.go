package lsm

import (
	"bytes"
	"fmt"
	"kv/utils"
	"log"
	"math"
	"sync"
)

/*lsm压缩器*/
type Compacter struct {
	wg sync.WaitGroup
}

/*压缩执行计划*/
type CompactDef struct {
	id        int
	thisLevel *levelHandler /*要被压缩的Tables*/
	nextLevel *levelHandler /*要被合成压缩的Tables*/
	cp        compactionPriority
	tb        *TableBuilder /*构建压缩所需的builder*/
	top       []*Table
	bot       []*Table
	topKr     *KeyRange
	botKr     *KeyRange
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
	for _, prio := range prios {
		if prio.level == 0 && id == 0 { /*0线程优先压缩level0层*/

		} else if prio.score < 1.0 {
			break /*压缩之后导致情况更加糟糕*/
		}
		if lm.run(id, prio) { /*说明有一个层级压缩成功了*/
			return true
		}
	}
	return false
}

/*根据优先级生成的压缩计划然后执行压缩工作*/
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)

	return false
}

func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level            /*要被压缩到别的地方的层级*/
	if p.t.baseLevel == 0 { /*要压缩去的层级*/
		p.t = lm.getLevelTarget() /*更新一下Target的最新值，说不定有新的希望*/
	}

	/*生成压缩计划*/
	cd := &CompactDef{
		id:        id,
		thisLevel: lm.handlers[l],
		cp:        p,
	}

	if l == 0 { /*level0的特殊压缩,baseLevel作为下一层*/
		cd.nextLevel = lm.handlers[p.t.baseLevel]
		/*执行压缩计划*/
		err := lm.compactL0(cd)
		if err {
			return utils.TableCompactErr
		}
	} else {
		cd.nextLevel = cd.thisLevel
		/*判断一下这是最后一层吗*/
		if !lm.isLastLevel(cd.nextLevel.levelNum) {
			cd.nextLevel = lm.handlers[cd.nextLevel.levelNum+1]
		}

	}

	return nil
}

func (lm *levelManager) compactL0(cd *CompactDef) bool {
	err := lm.compactL0ToBaseLevel(cd)
	if err {
		/*尝试l0到l0的压缩*/
		err = lm.compactL0ToL0(cd)
		return err
	}

	return true
}

func (lm *levelManager) compactL0ToBaseLevel(cd *CompactDef) bool {
	if cd.cp.adjusted >= 0.0 && cd.cp.adjusted <= 1.0 { /*它不配被压缩*/
		return false
	}

	/*获取l0中需要被压缩的表*/
	tables, kr, err := lm.getL0CompactTables()
	cd.bot = tables
	cd.botKr = kr
	if err != nil {
		return false
	}

	/*获取levelBase中要被合并的表*/
	err = lm.fillCompactNextLevel(cd)
	if err != nil {
		log.Fatalf("err: %v", err)
		return false
	}

	/*基础信息已经收集足够了现在开始真正的压缩*/
	err = lm.compact(cd)
	if err != nil {
		return false
	}
	return false
}

func (lm *levelManager) compact(cd *CompactDef) error {
	/*开始真正执行压缩流程，生成迭代器树*/
	ct := append(cd.bot, cd.top...)
	i := buildMergeIterators(ct)
	mi, ok := i.(*MergeIterator)
	if !ok {
		return utils.BuildMergeIterErr
	}

	/*生成相应的TableBuilder*/
	builder := NewTableBuilder(lm.opt)

	for mi.Valid() {
		e := mi.Item().Entry()
		/*判断一下是否可用*/
		if isSkip(e, mi.LastKey()) {
			mi.Next() /*因为Next会修改LastKey的值所以必须在LastKey()之后执行*/
			continue
		}
		mi.Next()
		builder.Add(e)
	}

	/*获取新的tableName*/
	fid := lm.GetNewFid()
	_, err := builder.Flush(lm, utils.FileNameSSTable(lm.opt.workDir, fid)) /*TODO:tables有什么作用，等下回来看看*/
	if err != nil {
		return err
	}
	return nil
}

func isSkip(thisE *utils.Entry, lastKey []byte) bool {
	return bytes.Equal(utils.ParseKey(thisE.Key), utils.ParseKey(lastKey))
}

/*填充这个执行计划的NextLevel所锁定的tables*/
func (lm *levelManager) fillCompactNextLevel(cd *CompactDef) error {
	if cd.bot == nil || cd.botKr == nil { /*没必要执行了*/
		return utils.TopTablesPullErr
	}
	botKr := cd.botKr

	/*找到所有涉及该范围的Tables*/
	top, topKr := lm.handlers[cd.nextLevel.levelNum].getAllTablesInTheRange(botKr)
	cd.top = top
	cd.topKr = topKr
	return nil
}

func getTotalSize(tables ...*Table) int {
	var size int
	for _, t := range tables {
		size += int(t.Size())
	}

	return size
}

func (lm *levelManager) compactL0ToL0(cd *CompactDef) bool {
	return false
}

func (lm *levelManager) getL0CompactTables() ([]*Table, *KeyRange, error) {
	if lm.levelHeight() == 0 {
		return nil, nil, utils.TablesNotInitErr
	}
	return lm.handlers[0].getL0CompactTables()
}

/*返回涉及到的Table们和融合kr之后的范围*/
func (lh *levelHandler) getAllTablesInTheRange(kr *KeyRange) ([]*Table, *KeyRange) {
	if lh.tables == nil || len(lh.tables) == 0 {
		return nil, kr /*范围没有变化，NextLevel为空，直接合并插入即可*/
	}
	var startCompact bool /*用于优化合并速度*/
	compactTables := lh.tables[:0]
	for _, t := range lh.tables {
		dkr := getKeyRange(t)
		/*判断是否在其中*/
		if kr.overlapsWith(dkr) { /*需要合并*/
			startCompact = true
			kr.extends(dkr)
			compactTables = append(compactTables, t)
		} else {
			if startCompact { /*后续合并也不再需要了，因为tables是一个有序的数组*/
				break
			}
		}
	}

	return compactTables, kr
}

/*获取L0中合适的要被压缩的Tables和范围，如果不行返回error*/
func (lh *levelHandler) getL0CompactTables() ([]*Table, *KeyRange, error) {

	/*暴力获取范围*/
	out := lh.tables
	if len(out) == 0 { /*根本不需要了*/
		return nil, nil, utils.TablesNotFoundErr
	}
	/*这里进行暴力获取*/
	out = out[:1]

	/*从fid最小的tables来开始计算，因为fid越小，
	说明其中数据就越小，需要被移动到下一层，新数据才在前几层
	*/
	kr := getKeyRange(lh.tables[0])
	for _, t := range lh.tables[1:] {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extends(dkr)
		} else { /*不再继续添加了*/
			break
		}
	}

	return out, kr, nil
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

func getKeyRange(t ...*Table) *KeyRange {
	if t == nil || len(t) == 0 {
		return nil
	}

	minKey := t[0].MinKey()
	maxKey := t[0].MaxKey()

	for i := 1; i < len(t); i++ {
		if utils.CompareKeys(minKey, t[i].MinKey()) > 0 {
			minKey = t[i].MinKey()
		}

		if utils.CompareKeys(maxKey, t[i].MaxKey()) < 0 {
			maxKey = t[i].MaxKey()
		}
	}

	return &KeyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}

/*检测kr和k之间是否存在重叠区间*/
func (kr *KeyRange) overlapsWith(k *KeyRange) bool {
	if k.isEmpty() {
		return false
	}
	if kr.isEmpty() {
		return true /*如果为空就就加入进去*/
	}
	if utils.CompareKeys(kr.right, k.left) < 0 {
		return false
	}
	if utils.CompareKeys(k.right, kr.left) < 0 {
		return false
	}

	return true
}

/*将kr的KeyRange范围扩展加入dkr*/
func (kr *KeyRange) extends(dkr *KeyRange) {
	if dkr.isEmpty() {
		return
	}
	if kr.isEmpty() {
		return
	}
	/*使用前，请确保二者是存在重叠区间的*/
	var left []byte
	var right []byte
	if utils.CompareKeys(kr.left, dkr.left) < 0 {
		left = kr.left
	} else {
		left = dkr.left
	}

	if utils.CompareKeys(kr.right, dkr.right) < 0 {
		right = dkr.right
	} else {
		right = kr.right
	}
	/*这里没有必要再取该key的极限小或极限大部分了*/
	kr.left = left
	kr.right = right
}

func (kr *KeyRange) isEmpty() bool {
	if kr == nil {
		return true
	}
	if len(kr.left) == 0 && len(kr.right) == 0 {
		return true
	} else {
		return false
	}
}

func (kr *KeyRange) String() string {
	return fmt.Sprintf("[left: %x,right: %x]", kr.left, kr.right)
}

func (kr *KeyRange) isEqual(dkr *KeyRange) bool {
	return bytes.Equal(kr.left, dkr.left) && bytes.Equal(kr.right, dkr.right)
}

/*TODO:通过迭代的方式构建一个二叉树形成的tableMergeIterators集合*/
func buildMergeIterators(tables []*Table) utils.Iterator {
	/*获取我们需要的非叶子节点数量*/
	MergeCount := GetMergeIteratorCount(len(tables))
	nodes := make([]*node, 0)
	node := &node{
		valid: true,
	}
	if MergeCount > 0 {
		node.addIterator(NewMergeIterator())
	} else {
		node.addIterator(NewTableIterator(tables[0])) /*TDOO:添加一个实际迭代数据的迭代器*/
	}
	/*通过迭代的方式二叉树*/
	var j int
	i := 1
	for {
		curNode := nodes[j]
		/*创建节点*/
		if i < MergeCount { /*创建MergeIterator*/
			curNode.addIterator(NewMergeIterator()) /*创建mergeIterator会自动创建两个左右节点*/
			nodes = append(nodes, &curNode.mergeIter.left)
			if i+1 < MergeCount {
				nodes = append(nodes, &curNode.mergeIter.right)
			} else {
				curNode.mergeIter.right.valid = false /*这是无用的*/
				break
			}
		} else { /*创建普通的Iterator*/
			curNode.addIterator(NewTableIterator(tables[i-MergeCount])) /*TDOO:添加一个实际迭代数据的迭代器*/
		}
		i += 2
		j++

	}
	nodes[0].iter.Rewind() /*该方法可以调用fix()来修正*/

	return nodes[0].iter
}

/*返回构建二叉树所需要的非叶子节点的数量*/
func GetMergeIteratorCount(tc int) int {
	//n := tc
	//if tc <= 0 {
	//	return 0
	//}
	//tc = tc | (tc >> 1)
	//tc = tc | (tc >> 2)
	//tc = tc | (tc >> 4)
	//tc = tc | (tc >> 8)
	//tc = tc | (tc >> 16)
	//
	//tc = (tc + 1) >> 2
	var n int
	if tc&1 == 1 {
		n = 1
	}
	return (tc / 2) + n
}
