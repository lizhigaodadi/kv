package utils

type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

func (arena *Arena) getNode(nodeOffset uint32) *node {
	/*TODO:通过偏移量来找到目标节点*/
	return &node{}
}
