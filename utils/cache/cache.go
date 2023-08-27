package cache

import (
	"container/list"
	"github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m         sync.RWMutex
	lru       *WindowsLRU
	slru      *SegmentedLRU
	door      *BloomFilter
	c         *cmsSketch
	t         int32
	threshold int32
	data      map[uint64]*list.Element
}

func NewCache(size int) *Cache {
	const lruPct = 1
	lruSz := (lruPct * size) / 100
	if lruSz < 1 {
		lruSz = 1
	}
	slruSz := int(float64(size) * (100 - lruPct) / 100)
	if slruSz < 1 {
		slruSz = 1
	}
	slruO := int(float64(slruSz) * 0.2)
	if slruO < 1 {
		slruO = 1
	}
	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  NewWindowsLRU(lruSz, data),
		slru: NewSegmentedLRU(data, uint64(slruO), uint64(slruSz)),
		c:    NewCmsSketch(uint64(size)),
		door: newBloomFilter(size, 0.01),
		data: data,
	}
}

func (c *Cache) set(key, value interface{}) bool {
	if key == nil {
		return false
	}

	keyHash, conflictHash := c.keyToHash(key)
	i := storeItem{
		key:      keyHash,
		conflict: conflictHash,
		Value:    value,
		stage:    0,
	}

	evictItem, evicted := c.lru.Add(i)
	if !evicted { /*没有任何对象被废弃，不需要继续执行了*/
		return true
	}

	/*检查slru是否需要丢弃*/
	victimItem := c.slru.victim()
	if victimItem == nil { /*slru 中并不需要丢弃，
		我们可以将WindowsLru中废弃的
		对象加入到slru中继续保留*/
		c.slru.Add(evictItem)
		return false
	}

	if !c.door.Allow(uint32(evictItem.key)) {
		/*说明这个evictItem不是一个高频出现的对象，我们没必要加入到slru中去*/
		return true
	}

	evictFrequency := c.c.Estimate(evictItem.key)
	victimFrequency := c.c.Estimate(victimItem.key)

	if evictFrequency < victimFrequency {
		return true /*不需要更新*/
	}
	c.slru.Add(evictItem)

	return true
}

/*将对象从整个缓存中删除*/
func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	if key == nil { /*参数错误*/
		return 0, false
	}

	keyHash, conflictHash := c.keyToHash(key)
	element, exists := c.data[keyHash]
	if !exists { /*根本没有这个对象*/
		return 0, false
	}
	item := element.Value.(*storeItem)
	if item.conflict != 0 && item.conflict != conflictHash { /*我们找错了*/
		return 0, false
	}
	/*map才是真正来查找数据的地方，删除了map也就get不到了数据*/
	delete(c.data, keyHash)

	return conflictHash, true
}

func (c *Cache) Set(key, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}
func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold { /*已经到了极限了，我们需要重置计数器*/
		c.door.reset()
		c.c.Reset()
		c.t = 0
	}
	keyHash, conflictHash := c.keyToHash(key)

	/*检查该key是否在其中存在着*/
	elementItem, exists := c.data[keyHash]

	if !exists { /*根本不存在这个对象*/
		c.c.Increment(keyHash)
		return nil, false
	}

	c.c.Increment(keyHash) /*更新访问频率*/

	item := elementItem.Value.(*storeItem)
	/*判断对象是否是我们要找的那个*/
	if item.conflict != conflictHash {
		return nil, false
	}

	/*这个对象就是我们要找的,判断一下这个对象是哪个阶段的*/
	if item.stage == 0 {
		c.lru.Get(elementItem)
	} else {
		c.slru.Get(elementItem)
	}

	return item.Value, true
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	/*针对所有对象生成hash数值*/
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("key type not supported")
	}
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))

}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}
func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
