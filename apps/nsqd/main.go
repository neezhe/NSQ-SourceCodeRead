package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD //此处并非组合，组合只能是匿名对象，所以此处就相当于只是记录一下。
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil { //nsqd也是通过go-svc启动
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
//start返回后会进入到svc的代码里面进行等待监听信号量，如果用户杀进程，就调用下面的stop
func (p *program) Start() error {
	opts := nsqd.NewOptions() // 1. 通过程序默认的参数构建 options 实例

	flagSet := nsqdFlagSet(opts)// 2. 将 opts 结合命令行参数集进行进一步初始化
	flagSet.Parse(os.Args[1:]) //因为用到了NewFlagSet,所以此处就需要指定Parse的参数，如果用的是默认Flag,则其参数无需指定

	rand.Seed(time.Now().UTC().UnixNano()) //设置随机数种子，后面所有的随机数的操作都是根据这个种子来的，能确保是随机的。
	// 3. 若 version 参数存在，则打印版本号，然后退出
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {//对于非string类型的flag取值才会用到flag.Getter，这玩在flag包里实现了除string类型外的Get方法，当然绑定自定义变量的时候也需要自己实现了String/Set/Get方法。
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}
	// 4. 若用户指定了自定义配置文件，则加载配置文件，读取配置文件，校验配置文件合法性
	// 读取解析配置文件采用的是第三方库 https://github.com/BurntSushi/toml
	var cfg config
	configFile := flagSet.Lookup("config").Value.String() //因为config是字符串类型的，所以此处就不会用到Getter
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg) //toml文件格式
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	cfg.Validate() //验证配置是否合法，主要关于TLS的验证

	options.Resolve(opts, flagSet, cfg)//要学习反射，把下面这个函数看懂就行了
	// 5. 通过给定参数 opts 构建 nsqd 实例
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd
	// 6. 加载 metadata文件(磁盘文件nsqd.data)，若文件存在，则恢复 topic和channel的信息（如pause状态），并调用 topic.Start方法
	err = p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	//7.持久化当前的topic,channel数据结构，不涉及到数据不封顶持久化. 写入临时文件后改名
	//怎么刚刚启动就要持久化呢？
	err = p.nsqd.PersistMetadata() //持久化数据
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}
	// 8. 在单独的 go routine 中启动 nsqd.Main 方法
	go func() {
		err := p.nsqd.Main()//开始监听服务
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
