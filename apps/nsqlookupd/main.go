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


func (p *program) Start() error {  //这个代码在svc.Run中会被调用。
	opts := nsqlookupd.NewOptions()

	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse(os.Args[1:]) //解析所有的flag

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
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
	nsqlookupd, err := nsqlookupd.New(opts)
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

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqlookupd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}
