package utils

import "sync"

type Throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	errCh     chan error
	finishErr error
}

func NewThrottle(max int) *Throttle {

	return &Throttle{
		ch:    make(chan struct{}, max),
		errCh: make(chan error, max),
	}
}

func (t *Throttle) Do() error {
	for {
		select {
		case t.ch <- struct{}{}:
			t.wg.Add(1)
			return nil
		case err := <-t.errCh:
			if err != nil {
				return err
			}
		}
	}
}

func (t *Throttle) Done(err error) {
	if err != nil {
		t.errCh <- err /*写入*/
	}
	select {
	case <-t.ch:
	default:
		panic("Throttle Do Done mismatch")
	}
	t.wg.Done()
}

/*该方法只会执行一次*/
func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.wg.Wait() /*等待所有任务完成*/
		close(t.errCh)
		close(t.ch)
		for err := range t.errCh {
			if err != nil {
				t.finishErr = err
				return
			}
		}
	})
	return t.finishErr
}
