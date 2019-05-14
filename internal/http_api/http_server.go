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
//看过golang的http模块，应该知道http模块最重要的就是http.Handler，就是下面的第二个参数，它提供了路由查找和函数执行功能。
func Serve(listener net.Listener, handler http.Handler, proto string, logf lg.AppLogFunc) error {
	logf(lg.INFO, "%s: listening on %s", proto, listener.Addr())

	server := &http.Server{ //实例化http.Server模块
		Handler:  handler,
		ErrorLog: log.New(logWriter{logf}, "", 0),
	}
	err := server.Serve(listener) //开启http服务
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	logf(lg.INFO, "%s: closing %s", proto, listener.Addr())

	return nil
}
