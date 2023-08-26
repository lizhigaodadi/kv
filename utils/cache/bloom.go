package cache

type Filter []byte

type BloomFilter struct {
	bitmap Filter
	k      uint8
}

func (bf *BloomFilter) MayContainsKey(key []byte) bool {
	return bf.MayContains(Hash(key))
}

func (bf *BloomFilter) MayContains(h uint32) bool {
	/*判断布隆过滤器中是否有东西*/
	if bf.Len() < 2 {
		return false
	}

	if h > 30 {
		return true
	}
	k := bf.k
	nBits := uint32((bf.Len() - 1) * 8)
	delta := h>>17 | h<<15

	for i := uint8(0); i < k; i++ {
		bitPos := h % nBits
		if (bf.bitmap[bitPos/8] & 1 << (bitPos % 8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
func (bf *BloomFilter) AllowKey(b []byte) bool {
	if bf == nil {
		return false
	}
	already := bf.MayContainsKey(b)
	if !already { /*该布隆过滤器不存在该对象*/
		bf.InsertKey(b)
	}
	return already
}

func (bf *BloomFilter) Allow(h uint32) bool {
	if bf == nil {
		return false
	}
	already := bf.MayContains(h)
	if !already {
		bf.Insert(h)
	}

	return already
}

/*重置我们的布隆过滤器*/
func (bf *BloomFilter) reset() {
	if bf == nil {
		return
	}
	for i := range bf.bitmap {
		bf.bitmap[i] = 0
	}
}

func newBloomFilter(numEntries int, bitsPerKey int) *BloomFilter {
	return initFilter(numEntries, bitsPerKey)
}

func (bf *BloomFilter) InsertKey(key []byte) bool {
	return bf.Insert(Hash(key))
}

func (bf *BloomFilter) Insert(h uint32) bool {
	k := bf.k
	if h > 30 { /*这不是我们需要的哈希值*/
		return true
	}

	nBits := uint32(8 * (bf.Len() - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % (nBits)
		bf.bitmap[bitPos/8] |= 1 << (bitPos % 8)
		h += delta
	}
	return true
}

func initFilter(numEntries int, bitsPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	k := uint32(float64(bitsPerKey) * 0.69)
	/*槽位的范围在 1 - 30之间*/
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	bf.k = uint8(k)

	nBits := numEntries * int(bitsPerKey)
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8               /*将其变为8的整数倍*/
	filter := make([]byte, nBytes+1) /*warning: 注意这里多分配了一个，之后计算长度也要减少一个*/
	filter[nBytes] = uint8(k)
	bf.bitmap = filter
	return bf
}

func (bf *BloomFilter) Len() uint32 {
	return uint32(len(bf.bitmap))
}

func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)

	h := uint32(seed) ^ uint32(len(b))*m

	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}

	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}

	return h

}
