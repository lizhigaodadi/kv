package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

func NewCloser() *Closer {
	closer := &Closer{
		waiting:     sync.WaitGroup{},
		CloseSignal: make(chan struct{}),
	}
	return closer
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}

func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}
