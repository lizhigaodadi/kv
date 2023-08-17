package utils

import "math"

type Filter []byte

func (filter Filter) appendFilter(buf []byte, keys []uint32, bitsPerKey int) {
	/*TODO: 将新的信息添加到布隆过滤器中*/

}

func BitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(0.69314718056, 2)
	locs := math.Ceil(size / float64(numEntries))

	return int(locs)
}

func CalcHashNum(bitsPerKey int) uint32 {
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		return 1
	}
	if k > 30 {
		return 30
	}
	return k
}
