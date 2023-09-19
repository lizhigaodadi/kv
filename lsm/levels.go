package lsm

import (
	"errors"
	"kv/file"
	"kv/utils"
	"kv/utils/cache"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type levelManager struct {
	maxFID   uint64
	opt      *Options
	cache    *cache.Cache
	lsm      *LSM
	mf       *file.ManifestFile
	cs       *compactStatus
	handlers []*levelHandler /*必须保持这个是一个有序的*/
	c        *Compacter      /*该lsm所依赖的压缩器*/
}

/*level处理器*/
type levelHandler struct {
	levelNum int           /*表示当前处理器所在的层级*/
	tables   []*Table      /*本层中所有的Table表*/
	lm       *levelManager /*通过它来获取相应的配置信息*/
	rwMutex  *sync.RWMutex
}

/*返回当前层级table*/
func (lh *levelHandler) numTable() int {
	return len(lh.tables)
}

/*返回最后一个level层级*/
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.handlers[len(lm.handlers)-1]
}

func (lm *levelManager) levelHeight() int {
	if lm.handlers == nil {
		return 0
	}
	return len(lm.handlers)
}

func (lm *levelManager) isLastLevel(level int) bool {
	return len(lm.handlers)-1 == level
}

func PrevPullDBMessage() {
	/*TODO:预读取数据库信息*/
}

func NewLevelManager() *levelManager {
	/*TODO:创建一个LevelManager*/
	return nil
}

func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

/*该方法应该在后台线程下执行，生成相应的一个执行计划*/
func (lm *levelManager) pickCompactLevel() (prios []compactionPriority) {
	/*首先获取当前几个level的实际情况*/
	t := lm.getLevelTarget()
	/*判断一下到底决定压缩哪两层*/
	addPriority := func(level int, score float64) { /*因为该方法只有这里使用到了，就设计为一个匿名函数*/
		pro := compactionPriority{
			level:    level,
			adjusted: score,
			score:    score,
			t:        t,
		}
		prios = append(prios, pro)
	}

	/*先计算l0表的优先级*/
	addPriority(0, float64(lm.handlers[0].levelNum)/float64(t.targetSz[0]))

	for i := 1; i < len(lm.handlers); i++ { /*计算每一level的优先级*/
		sz := lm.handlers[i].getRealSize() - lm.cs.levels[i].delSize /*有效大小*/
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(prios) != len(lm.handlers), errors.New("prios init falied"))

	var prevLevel int /*从0跳到baseLevel*/
	for i := t.baseLevel; i < lm.levelHeight()-1; i++ {
		if prios[prevLevel].adjusted >= 1 {
			const minScore = 0.01
			if prios[i].score >= minScore { /*防止过大*/
				prios[prevLevel].adjusted /= prios[i].score
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = i
	}
	/*选择所有分数大于1的优先级*/
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})

	return prios
}

func (lm *levelManager) getLevelTarget() *Targets {
	level := len(lm.handlers)

	adjust := func(sz int64) int64 {
		if sz < int64(lm.opt.BaseLevelSize) {
			return int64(lm.opt.BaseLevelSize)
		}
		return sz
	}

	t := &Targets{
		targetSz: make([]uint32, level),
		fileSz:   make([]uint32, level),
	}

	/*规范每一个层级的期望大小*/
	dbSize := lm.lastLevel().getRealSize()
	for i := len(lm.handlers) - 1; i > 0; i-- {
		levelTargetSize := adjust(int64(dbSize))
		t.targetSz[i] = uint32(levelTargetSize)
		/*如果当前level没有达到合并要求*/
		if t.baseLevel == 0 && int(levelTargetSize) <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= uint32(lm.opt.LevelSizeMultiplier)
	}

	/*规范每一个文件的期望大小*/
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.handlers); i++ {
		if i == 0 {
			t.fileSz[i] = uint32(lm.opt.BaseTableSize)
		} else if i <= t.baseLevel {
			t.fileSz[i] = uint32(tsz)
		} else {
			tsz *= lm.opt.TableSizeMultiplier
			t.fileSz[i] = uint32(tsz)
		}
	}

	/*找到最后一个空level作为目标来实现跨level归并，减少写放大*/
	for i := t.baseLevel + 1; i < len(lm.handlers)-1; i++ {
		if lm.handlers[i].getRealSize() > 0 {
			break
		}
		t.baseLevel = i
	}
	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.handlers
	if b < len(lvl)-1 && lvl[b].getRealSize() == 0 && lvl[b+1].getRealSize() < t.targetSz[b+1] {
		t.baseLevel += 1
	}

	return t
}

func (lh *levelHandler) pullMessageToTarget(t *Targets) error {
	curLevel := lh.levelNum
	opt := lh.lm.lsm.opt
	if curLevel < 0 || curLevel >= len(t.fileSz) {
		return errors.New("Over Level")
	}

	if curLevel == 0 { /*发现这个是最低层层的我们只需要记录Table数量即可*/
		t.targetSz[0] = uint32(opt.BotLevelTableCount)

	} else if curLevel == 0 {
		t.targetSz[1] = uint32(opt.BaseLevelSize)
	} else {
		t.targetSz[curLevel] = t.targetSz[curLevel-1] * uint32(opt.LevelSizeMultiplier)
	}

	t.fileSz[curLevel] = lh.getRealSize()

	return nil /*没有问题*/
}

func (lh *levelHandler) getRealSize() uint32 {
	/*获取实际该层中size大小*/
	if lh.levelNum == 0 {
		return uint32(len(lh.tables))
	} else {
		/*统计每一个Table占用的大小*/
		var levelSize uint32
		for i := 0; i < len(lh.tables); i++ {
			levelSize += lh.tables[i].Size()
		}
		return levelSize
	}
}

func (lm *levelManager) GetNewFid() uint64 {
	return atomic.AddUint64(&lm.maxFID, 1)
}

func (lh *levelHandler) Add(t ...*Table) {
	lh.rwMutex.Lock()
	defer lh.rwMutex.Unlock()
	lh.tables = append(lh.tables, t...) /*总是按照顺序添加进去的*/
}

func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)

	/*开始每个层级的查询*/
	/*level0*/
	entry, err = lm.handlers[0].Get(key)
	if err == nil {
		return entry, nil
	}
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		lh := lm.handlers[level]
		if entry, err = lh.Get(key); err == nil {
			return entry, nil
		}
	}

	return nil, utils.KeyNotFoundErr
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		/*来自l0的特殊查找*/
		return lh.searchL0SST(key)
	} else {
		/*其他层的查找*/
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	/*需要对所有表都进行一次遍历*/
	for _, t := range lh.tables {
		if entry, err := t.Search(key, &version); err != nil {
			return entry, nil
		}
	}
	return nil, utils.KeyNotFoundErr
}

func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	/*对其他层级的表都进行一次搜索*/
	table := lh.getTable(key)
	if table == nil {
		return nil, utils.KeyNotFoundErr
	}
	var version uint64
	entry, err := table.Search(key, &version)
	if err != nil {
		return nil, utils.KeyNotFoundErr
	}

	return entry, nil
}

func (lh *levelHandler) getTable(key []byte) *Table {
	/*先查看该层级的最大和最小的key*/
	minKey := lh.tables[0].ss.MinKey()
	maxKey := lh.tables[lh.levelNum-1].ss.MaxKey()
	if utils.CompareKeys(key, minKey) < 0 || utils.CompareKeys(key, maxKey) > 0 {
		return nil
	}

	/*开始细分到每一个表中去寻找*/
	for _, t := range lh.tables {
		/*判断一下是否在里面*/
		if utils.CompareKeys(key, t.ss.MinKey()) >= 0 && utils.CompareKeys(key, t.ss.MaxKey()) <= 0 { /*找到了*/
			return t
		}
	}
	return nil
}
