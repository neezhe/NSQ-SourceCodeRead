package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}
//假如Main线程在接收到系统信号量之后直接退出, 那么其他两个线程也会跟着销毁. 于是这里就会出现一个问题, tcp服务线程 和 http服务线程,
// 是业务逻辑实现的主要线程, 里面可能正在”干活”, 此时需要退出, 是否有数据需要持久化了? 事务是否正好执行到一半?
//所以退出时需要做优雅退出处理. Main所在线程退出之前, 需要 ‘通知’ 并且 ‘等待’ 两个服务线程退出后才能退出
//为了实现这一点, nsq 利用了 sync包中的 waitgroup 组件
func (w *WaitGroupWrapper) Wrap(cb func()) {  //注意这个函数的用法。
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
