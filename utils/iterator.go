package utils

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close()
	Seek(key []byte)
}

/*接口都默认为指针实现*/
type Item interface {
	Entry() *Entry
}
