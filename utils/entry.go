package utils

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64
	Version   uint64
}

type Entry struct {
	Key      []byte
	Value    []byte
	ExpireAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int
	ValThreshold int
}

func (e *Entry) getValue() []byte {
	return e.Value
}
func (e *Entry) getKey() []byte {
	return e.Key
}
