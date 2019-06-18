package nsqlookupd

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)
//nsqlookup的主要任务是负责注册和管理各个客户端，管理客户端与topic、Channel之间的关系。为了维护这种关系，
// 在nsqlookup内部提供了一张注册表，这张注册表使用RegistrationDB结构来实现。
type NSQLookupd struct {//表示了nsqlookupd服务实例，完成了http和tcp的监听和启动，初始化并维护了registrationMap注册表。所以这个结构体的元素就有如下这些。
	//读写互斥锁应用举例 https://golang.org/pkg/sync/#RWMutex
	//http://blog.csdn.net/aslackers/article/details/62044726
	/**
	 * 基本遵循两大原则：
	 * 1、可以随便读，多个goroutine同时读
	 * 2、写的时候，啥也不能干。不能读也不能写
	 */
	sync.RWMutex
	opts         *Options  //在文件nsqlookupd/options.go中定义，记录NSQLookupd的配置信息
	tcpListener  net.Listener //记录监听的文件描述符
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper //很重要，在文件internal/util/wait_group_wrapper.go中定义，与sync.WaitGroup相关，用于同步
	DB           *RegistrationDB //注册数据库，存放着topic和producer的映射关系注册表
}
//根据配置的nsqlookupd options信息，创建一个NSQLookupd实例
func New(opts *Options) (*NSQLookupd, error) {
	var err error

	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(), //需要注意的是这个结构，见有道笔记有图
	}

	l.logf(LOG_INFO, version.String("nsqlookupd"))

	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress) //监听TCP，监听地址取options中的默认参数，0.0.0.0:4160
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress) //监听并处理HTTP，逻辑和上面TCP差不多 0.0.0.0:4161
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}

	return l, nil
}

// Main starts an instance of nsqlookupd and returns an
// error if there was a problem starting up.
//Main函数，启动nsqlockupd进程
//详情见apps/nsqlookupd/nsqlookupd.go里的Start()方法
func (l *NSQLookupd) Main() error {
	ctx := &Context{l} //Context实例，在文件nsqlookupd/context.go中定义，NSQLookupd类型的指针

	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {  //通过err值来判断
				l.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	tcpServer := &tcpServer{ctx: ctx}  //tcpServer实例
	//使用l.waitGroup.Wrap装饰器，关于sync.WaitGroup介绍，http://blog.csdn.net/aslackers/article/details/62046306
	//在Exit()中有用到，可知当关闭nsqlookupd进程时，主线程(goroutine)会等待所有TCP监听关闭，才关闭自己
	l.waitGroup.Wrap(func() {//Wrap中会开一个routine,并同步计数
		//运行TCP服务，用于处理nsqd上报信息的.
		//第二个参数tcpServer实现了TCPHandler接口，tcpServer接收到TCP数据时，会调用其Handle()方法处理。
		exitFunc(protocol.TCPServer(l.tcpListener, tcpServer, l.logf))
	}) //nsqd连接到nsqlookupd的tcp监听上，通过心跳告诉nsqlookupd自己在线

	//httpServer实例，在文件nsqlookupd/http.go中定义，处理HTTP接收到的数据
	//并定义了一系列HTTP接口(路由)
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		//运行http服务用于向nsqadmin提供查询接口的，本质上，就是一个web服务器，提供http查询接口
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	err := <-exitCh
	return err
}
//获取监听的TCP地址
func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}
//获取HTTP地址
func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}
//退出nsqloopupd进程，关闭两个Listener，等待TCP和HTTP线程都关闭了，才关闭自身
func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait() //waitGroup.Wrap修饰的函数都是几个重要的函数，所以此处需要通过同步来控制。
}
