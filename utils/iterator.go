package utils

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close()
	Seek(key []byte)
}

type Item interface {
	Entry() *Entry
}
