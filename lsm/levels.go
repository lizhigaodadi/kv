package lsm

import (
	"kv/file"
	"kv/utils/cache"
)

type levelManager struct {
	maxFID   uint64
	opt      *Options
	cache    *cache.Cache
	lsm      *LSM
	mf       *file.ManifestFile
	handlers []*levelHandler
	c        *Compacter /*该lsm所依赖的压缩器*/
}

/*level处理器*/
type levelHandler struct {
	levelNum int      /*表示当前处理器所在的层级*/
	tables   []*Table /*本层中所有的Table表*/
}

func PrevPullDBMessage() {
	/*TODO:预读取数据库信息*/

}

func NewLevelManager() *levelManager {
	/*创建一个LevelManager*/
	return nil
}

/*该方法应该在后台线程下执行*/
func (lm *levelManager) pickCompactLevel() {
	/*首先获取当前几个level的实际情况*/
}

func (lm *levelManager) getLevelTarget() *Targets {
	level := len(lm.handlers)
	t := Targets{
		targetSz: make([]uint32, level),
		realSz:   make([]uint32, level),
	}

	return nil
}
