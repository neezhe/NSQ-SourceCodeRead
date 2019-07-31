package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}
//这个TCPServer函数是公共函数部分，用于nsqlookupd,nsqd的tcp服务，所有的 tcp 服务都可以复用此代码, 不同的服务内容, 只需要实现不同的TCPHandler
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	for {  //tcp已经close时，退出for循环
		clientConn, err := listener.Accept() //会阻塞
		if err != nil {//针对不同的错误级别，采用不同的处理方式
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()  // 是临时的错误, 暂停一下继续
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		go handler.Handle(clientConn)
	}

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
