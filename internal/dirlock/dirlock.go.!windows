// +build !windows

package dirlock

import (
	"fmt"
	"os"
	"syscall"
)
//定义一个文件锁的结构体
type DirLock struct {
	dir string //目录路径
	f   *os.File  //文件描述符
}

func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir) //// 获取文件描述符
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) //加上排他锁，LOCK_NB表示当遇到文件加排他锁的情况直接返回 Error不用阻塞等待。
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
	}
	return nil
}

func (l *DirLock) Unlock() error {
	defer l.f.Close()  // close 掉文件描述符
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN) // 释放 Flock 文件锁
}
//Flock 是建议性的锁，使用的时候需要指定 how 参数，否则容易出现多个 goroutine 共用文件的问题
//how 参数指定 LOCK_NB 之后，goroutine 遇到已加锁的 Flock，不会阻塞，而是直接返回错误