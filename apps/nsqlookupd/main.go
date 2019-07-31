package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqlookupd"
)

//nsqlookupd实际工作中主要调用的是Init，Start,Stop三个函数。

func nsqlookupdFlagSet(opts *nsqlookupd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqlookupd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address of this lookupd node, (default to the OS hostname)")

	flagSet.Duration("inactive-producer-timeout", opts.InactiveProducerTimeout, "duration of time a producer will remain in the active list since its last ping")
	flagSet.Duration("tombstone-lifetime", opts.TombstoneLifetime, "duration of time a producer will remain tombstoned if registration remains")

	return flagSet
}
//svc采用了模板设计模式在负责初始化、启动、关闭进程。
//这些初始化Init、启动Start和关闭Stop方法通过接口定义，交给具体进程去实现（此处program实现了这几个方法）。而svc则负责管理何时去调用这些方法。
//program结构实现了svc的Service接口。因此可以交给svc来负责管理program的初始化、启动和关闭动作。
type program struct {
	once       sync.Once
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

//Init函数判断了当前的操作系统环境，如果是windwos系统的话，就会将修改工作目录。可以参考https://github.com/judwhite/go-svc首页的例子。
func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

//程序执行步骤：
//
//1、执行nsqlookupd/options.go中的NewOptions()方法，创建Options实例，生成NSQLookupd配置信息
//
//2、执行上面的New()方法，将第1步中创建的Options配置信息作为参数，创建NSQLookupd实例
//
//3、执行NSQLookupd实例的Main()方法，创建nsqloopupd进程，TCP监听0.0.0.0:4160，HTTP监听0.0.0.0:4161
//
//nsqloopupd进程退出时，调用NSQLookupd实例的Exit()方法，关闭TCP和HTTP监听，主线程(goroutine)等待子线程(goroutine)退出，程序退出


func (p *program) Start() error {  //这个代码在svc.Run中会被调用。此处才是nsqlookupd的主体功能。
	opts := nsqlookupd.NewOptions()

	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse(os.Args[1:]) //解析所有的flag

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) { //如果只是查询版本信息，那么查完就返回
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg map[string]interface{}
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}

	options.Resolve(opts, flagSet, cfg)
	nsqlookupd, err := nsqlookupd.New(opts) //此处会开启tcp/http监听,Main中会开始accept
	if err != nil {
		logFatal("failed to instantiate nsqlookupd", err)
	}
	p.nsqlookupd = nsqlookupd //组合

	go func() {
		err := p.nsqlookupd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}
//Stop函数接受外界的signal，如果收到syscall.SIGINT和syscall.SIGTERM信号，就会被调用。svc这个包负责监听这两个信号，在main函数中已经指明了。
//syscall.SIGINT：ctrl+c信号os.Interrupt，中断。
//syscall.SIGTERM：pkill信号syscall.SIGTERM，关闭此服务。
func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqlookupd.Exit() //调用Exit函数，关闭了tcp服务和http服务，然后等两个服务关闭之后，程序结束。“等两个服务关闭”这个动作涉及到goroutine同步，nsq通过WaitGroup(参考Goroutine同步)实现。
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}
