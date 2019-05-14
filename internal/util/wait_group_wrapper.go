package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {  //注意这个函数的用法。
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
