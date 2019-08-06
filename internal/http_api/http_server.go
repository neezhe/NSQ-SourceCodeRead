package http_api

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/nsqio/nsq/internal/lg"
)

type logWriter struct {
	logf lg.AppLogFunc
}

func (l logWriter) Write(p []byte) (int, error) {
	l.logf(lg.WARN, "%s", string(p))
	return len(p), nil
}
//看过golang的http模块，应该知道http模块最重要的就是http.Handler(处理器)，就是下面的第二个参数，它可以提供路由查找和函数执行功能。
func Serve(listener net.Listener, handler http.Handler, proto string, logf lg.AppLogFunc) error {
	logf(lg.INFO, "%s: listening on %s", proto, listener.Addr())

	server := &http.Server{ //实例化http.Server模块，Server中其实可以设置请求的读取超时时间/响应的写入超时时间/错误日志记录器等。
		Handler:  handler, //此处指定的是多路复用器，是httprouter的多路复用器包了一层的处理器。
		ErrorLog: log.New(logWriter{logf}, "", 0),
	}
	//server.ListenAndServe() //其实此处调用server.ListenAndServe() 就够了，但是下面server.Serve也是一个意思，因为不需要解析ip和端口了，直接操作监听句柄。
	err := server.Serve(listener) //开启http服务，学习一下，如何根据listener监听http服务
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	logf(lg.INFO, "%s: closing %s", proto, listener.Addr())

	return nil
}
