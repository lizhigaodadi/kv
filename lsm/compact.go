package lsm

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"kv/pb"
	"kv/utils"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"
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
	t         *Targets
	split     []KeyRange
	topKrIdx  int
	botKrIdx  int
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

func (cs *compactStatus) isCompacting(levelNum int, table *Table) bool {
	if _, ok := cs.tables[table.fid]; ok {
		return true
	}
	return false
}

type levelCompactStatus struct {
	levelNum int
	delSize  uint32 /*记录了当前层级中即将要被删除的部分所占比重*/
	krs      []*KeyRange
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
		lm.compactTables(cd)

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
	/*将L0和LBase全部锁住*/
	levelRLock(cd.thisLevel, cd.nextLevel)
	defer levelRUnlock(cd.thisLevel, cd.nextLevel)

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

	lm.cs.updateStatus(cd)
	defer lm.cs.finishUpdateStatus(cd)

	/*基础信息已经收集足够了现在开始真正的压缩*/
	err = lm.compact(cd, false)
	if err != nil {
		return false
	}
	return false
}

func (lm *levelManager) compact(cd *CompactDef, needSplit bool) error {

	/*开始真正执行压缩流程，生成迭代器树*/
	ct := append(cd.bot, cd.top...)
	i := buildMergeIterators(ct)
	mi, ok := i.(*MergeIterator)
	if !ok {
		return utils.BuildMergeIterErr
	}
	if needSplit { /*开始进行分片*/
		lm.addSplits(cd)
	} else {
		lm.noSplits(cd)
	}
	newTables := make([]*Table, 0)
	var builder *TableBuilder
	/*生成相应的TableBuilder*/
	count := 0
	for count < len(cd.split) {
		builder = NewTableBuilder(lm.opt)
		for mi.Valid() {
			e := mi.Item().Entry()
			/*判断一下是否需要跳出循环*/
			if utils.CompareKeys(e.Key, cd.split[count].right) > 0 {
				/*获取新的tableName*/
				fid := lm.GetNewFid()
				newTable, err := builder.Flush(lm, utils.FileNameTemp(lm.opt.workDir, fid))
				if err != nil {
					return err
				}
				newTables = append(newTables, newTable)
				count++
				break
			}
			/*判断一下是否可用*/
			if isSkip(e, mi.LastKey()) {
				mi.Next() /*因为Next会修改LastKey的值所以必须在LastKey()之后执行*/
				continue
			}
			mi.Next()
			builder.Add(e)
		}

	}

	/*后续操作，更新manifest文件，删除原本的几个Table然后添加新的Table*/
	err := lm.updateFileAfterCompact(cd, ct, newTables...)
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

func (lm *levelManager) compactTables(cd *CompactDef) bool {
	cd.levelRLock()
	defer cd.levelRUnlock()
	/*执行对非level0的压缩*/
	/*判断一下是不是lMax压缩到lMax的情况*/
	if cd.thisLevel.levelNum == cd.nextLevel.levelNum { /*这个是压缩自己的操作*/
		return lm.fillMaxLevelTables(cd)
	}

	/*开始levelN到levelN+1的压缩*/
	bTables := make([]*Table, len(cd.thisLevel.tables))
	copy(bTables, cd.thisLevel.tables)

	now := time.Now()
	for _, t := range bTables { /*前提是这个是按照fid来进行排序的*/
		/*判断一下这个是否要被压缩*/
		if lm.cs.isCompacting(cd.thisLevel.levelNum, t) {
			continue
		}
		/*判断这个层级的下一层中压缩的key范围是否包括在t中了*/
		if lm.cs.overlapsWith(cd.nextLevel.levelNum, t) {
			continue
		}
		/*查看创建时间*/
		if now.Sub(now) > 10*time.Second { /*后续生成的文件创建时间只会更小直接退出即可*/
			return false
		}
		/*找到合适的了开始压缩到下一层即可*/
		cd.bot = []*Table{t}
		cd.botKr = getKeyRange(t)
		break
	}
	/*检查是否为空*/
	if len(cd.bot) == 0 { /*没有合适的*/
		return false
	}

	if !lm.normalMatchAppropriateNext(cd) {
		return false
	}

	lm.cs.updateStatus(cd)
	defer lm.cs.finishUpdateStatus(cd)
	/*执行压缩流程*/
	err := lm.compact(cd, true)

	if err != nil {
		return false
	}

	return true
}

func (lm *levelManager) compactL0ToL0(cd *CompactDef) bool {
	/*由于L0CompactToLBase的失败导致执行的方法*/
	/*判断一下该行为只有0号压缩器可以执行*/
	if cd.id != 0 {
		return false
	}
	cd.nextLevel = lm.handlers[0]
	cd.topKr = &KeyRange{}
	cd.top = nil

	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))

	lm.handlers[0].rwMutex.Lock()
	defer lm.handlers[0].rwMutex.Unlock()

	/*开始进行过滤操作*/
	now := time.Now()
	bot := cd.bot
	var out []*Table
	for _, t := range bot {
		/*不能过大*/
		if t.Size() >= 2*cd.t.fileSz[0] {
			continue
		}
		/*检查创建时间*/
		if now.Sub(t.GetCreateAt()) < 10*time.Second {
			/*创建时间超过10s也不要回收*/
			continue
		}
		/*检查当前table是否正在被压缩*/
		if _, beginCompacted := lm.cs.tables[t.fid]; beginCompacted {
			continue
		}

		out = append(out, t)
	}

	/*检查数量是否足够，如果不能压缩很多，那么也没必要压缩*/
	if len(out) < 4 {
		return false
	}
	cd.bot = out

	/*告知其他线程这些Table正在被压缩中*/
	lm.cs.updateStatus(cd)
	defer lm.cs.finishUpdateStatus(cd)

	/*TODO: 这段代码有点不理解*/
	cd.t.fileSz[0] = math.MaxUint32

	/*开始真正的压缩操作*/
	err := lm.compact(cd)
	if err != nil {
		return false
	}
	return true
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

/*在内存中合并文件之后我们仍然需要更新manifest和删除旧的文件，添加新的sst文件*/
func (lm *levelManager) updateFileAfterCompact(cd *CompactDef, oldTables []*Table, newTables ...*Table) error {
	/*先更新manifest文件*/
	mf := lm.mf
	var changes []*pb.ManifestChange
	for _, t := range newTables {
		changes = append(changes, &pb.ManifestChange{
			Id:    t.fid,
			Op:    pb.Operation_CREATE,
			Level: uint32(cd.nextLevel.levelNum),
		})
	}

	for _, t := range oldTables { /*添加要被删除的文件*/
		changes = append(changes, &pb.ManifestChange{
			Id:    t.fid,
			Op:    pb.Operation_DELETE,
			Level: uint32(cd.thisLevel.levelNum),
		})
	}
	err := mf.AddChanges(changes)
	if err != nil {
		return err
	}

	/*删除原本的文件，修改新的文件名称*/
	for _, t := range oldTables {
		err := os.Remove(utils.FileNameSSTable(lm.opt.workDir, t.fid))
		if err != nil {
			return err
		}
	}

	/*改名操作*/
	for _, t := range newTables {
		/*获取fid*/
		fid := t.fid
		newName := utils.FileNameSSTable(lm.opt.workDir, fid)
		/*修改名称*/
		oldName := utils.FileNameTemp(lm.opt.workDir, fid)
		/*开始进行修改*/
		err := os.Rename(oldName, newName)
		if err != nil { /*TODO:异常处理肯定不能如此潦草，之后继续完善*/
			return err
		}

	}

	return nil
}

func levelRLock(this *levelHandler, next *levelHandler) {
	this.rwMutex.RLock()
	next.rwMutex.RLock()
}

func levelRUnlock(this *levelHandler, next *levelHandler) {
	next.rwMutex.RUnlock()
	this.rwMutex.RUnlock()
}

/*更新状态信息，需要加锁,请确保要被修改的层级已经被加锁了*/
func (cs *compactStatus) updateStatus(cd *CompactDef) {
	cs.m.Lock()
	defer cs.m.Unlock()

	delTables := cd.bot
	compactTables := cd.top
	/*计算他们的长度*/
	var delSize uint32

	cd.botKrIdx = len(cs.levels[cd.thisLevel.levelNum].krs)
	cd.topKrIdx = len(cs.levels[cd.thisLevel.levelNum].krs)

	for _, t := range delTables {
		cs.tables[t.fid] = struct{}{}
		delSize += t.Size()
		/*增加被删除的面积*/
		kr := getKeyRange(t)
		/*增加新的范围*/
		cs.levels[cd.thisLevel.levelNum].krs = append(cs.levels[cd.thisLevel.levelNum].krs, kr)
	}
	for _, t := range compactTables {
		cs.tables[t.fid] = struct{}{}
		kr := getKeyRange(t)
		cs.levels[cd.nextLevel.levelNum].krs = append(cs.levels[cd.nextLevel.levelNum].krs, kr)
	}

	/*开始更新*/
	cs.levels[cd.thisLevel.levelNum].delSize += delSize
	/*看看是哪些表要被删除了*/

}

func (cs *compactStatus) finishUpdateStatus(cd *CompactDef) {
	cs.m.Lock()
	defer cs.m.Unlock()

	delTables := cd.bot
	compactTables := cd.top
	var delSize uint32

	for _, t := range delTables {
		/*从map中删除*/
		delete(cs.tables, t.fid)
		delSize += t.Size()
	}

	for _, t := range compactTables {
		delete(cs.tables, t.fid)
	}

	/*修改range范围*/
	topKrIdx := cd.topKrIdx
	botKrIdx := cd.botKrIdx

	topCount := len(cd.top)
	botCount := len(cd.top)
	/*开始修改*/
	cs.levels[cd.nextLevel.levelNum].krs =
		append(cs.levels[cd.nextLevel.levelNum].krs[0:topKrIdx],
			cs.levels[cd.nextLevel.levelNum].krs[topKrIdx+topCount:]...)
	cs.levels[cd.thisLevel.levelNum].krs =
		append(cs.levels[cd.thisLevel.levelNum].krs[0:botKrIdx],
			cs.levels[cd.thisLevel.levelNum].krs[botKrIdx+botCount:]...)

	cs.levels[cd.thisLevel.levelNum].delSize -= delSize

}

/*TODO:待完善*/
func (lm *levelManager) addSplits(cd *CompactDef) {
	/*将两个*/
	cd.split = cd.split[:0]

	width := int(math.Ceil(float64(len(cd.top)) / 5.0))
	if width < 3 {
		width = 3
	}

	skr := cd.botKr
	skr.extends(cd.topKr)

	addRange := func(right []byte) {
		curKr := KeyRange{
			left:  skr.left,
			right: skr.right,
		}
		curKr.right = utils.Copy(right)
		cd.split = append(cd.split, curKr)
		skr.left = curKr.right
	}

	for i, t := range cd.top {
		if i == len(cd.top)-1 {
			addRange([]byte{}) /*终结的标识*/
			return
		}

		if i%width == width-1 {
			/*开始切割*/
			right := utils.KeyWithTs(utils.ParseKey(t.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}

}

func (lm *levelManager) noSplits(cd *CompactDef) {
	/*不进行分片处理*/
	cd.split = make([]KeyRange, 1)
	cd.split[0].left = cd.topKr.left
	cd.split[0].right = []byte{}
}

func (lm *levelManager) fillMaxLevelTables(cd *CompactDef) bool {
	tables := cd.bot
	sortedTables := make([]*Table, len(tables)) /*请确保这些已经被排序了*/
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		return false
	}

	cd.top = []*Table{}
	collectTopTables := func(t *Table, needSz uint32) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].ss.MinKey(), t.ss.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		j++
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize > needSz {
				break
			}
			cd.top = append(cd.bot, newT)
			cd.topKr.extends(getKeyRange(newT))
			j++
		}
	}

	/*第二层过滤*/
	now := time.Now()

	for _, t := range sortedTables {
		/*检查创建时间*/
		if now.Sub(t.GetCreateAt()) < time.Hour {
			continue
		}
		/*检查需要删除的key多不多*/
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		/*检查该表是否正在被压缩中*/
		if lm.cs.isCompacting(cd.thisLevel.levelNum, t) {
			continue
		}
		cd.top = []*Table{t} /*找到合适的了，将这个后面所有合适的一起加入进来*/
		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		if t.Size() > needFileSz { /*没必要继续了*/
			break
		}

		collectTopTables(t, needFileSz)
		/*TODO:更新合并状态信息*/
	}
	if len(cd.top) == 0 {
		return false
	}

	/*TODO:确定好了合并的表就开始更新状态*/
	lm.cs.updateStatus(cd)
	defer lm.cs.finishUpdateStatus(cd)

	/*TODO:开始真正的合并压缩最后一层*/

	return true
}

func (lm *levelManager) sortByStaleDataSize(tables []*Table, cd *CompactDef) {
	/*TODO:统计sst文件中旧数据，并进行排序*/
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

func (cd *CompactDef) levelRLock() {
	/*判断一下两个level是不是同一个*/
	if cd.thisLevel.levelNum == cd.nextLevel.levelNum {
		cd.thisLevel.rwMutex.RLock()
	} else {
		levelRLock(cd.thisLevel, cd.nextLevel)
	}
}

func (cd *CompactDef) levelRUnlock() {
	/*判断一下两个level是不是同一个*/
	if cd.thisLevel.levelNum == cd.nextLevel.levelNum {
		cd.thisLevel.rwMutex.RUnlock()
	} else {
		levelRUnlock(cd.thisLevel, cd.nextLevel)
	}
}

/*该函数仅仅比较范围*/
func (cs *compactStatus) overlapsWith(levelNum int, t *Table) bool {

	krs := cs.levels[levelNum].krs
	curKr := getKeyRange(t)
	for _, kr := range krs {
		if curKr.overlapsWith(kr) {
			return false
		}
	}

	return true
}

/*普通的找到合适的下一层*/
func (lm *levelManager) normalMatchAppropriateNext(cd *CompactDef) bool {
	nextKr := cd.botKr

	/*在下一层寻找合适的*/
	nextTables := make([]*Table, 0)
	nextLevel := cd.nextLevel.levelNum
	now := time.Now()
	for _, t := range cd.nextLevel.tables { /*寻找合适的*/
		if lm.cs.isCompacting(nextLevel, t) || lm.cs.overlapsWith(nextLevel, t) {
			continue
		}
		/*检测创建时间*/
		if now.Sub(t.GetCreateAt()) < 10*time.Second {
			/*直接结束*/
			break
		}

		kr := getKeyRange(t)
		if nextKr.overlapsWith(kr) { /*涉及了*/
			nextKr.extends(kr)
			nextTables = append(nextTables, t)
		}

	}

	return true
}
