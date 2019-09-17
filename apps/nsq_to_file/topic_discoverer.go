package main

import (
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
)

type TopicDiscoverer struct {
	logf     lg.AppLogFunc
	opts     *Options
	ci       *clusterinfo.ClusterInfo
	topics   map[string]*FileLogger
	hupChan  chan os.Signal
	termChan chan os.Signal
	wg       sync.WaitGroup
	cfg      *nsq.Config
}

func newTopicDiscoverer(logf lg.AppLogFunc, opts *Options, cfg *nsq.Config, hupChan chan os.Signal, termChan chan os.Signal) *TopicDiscoverer {
	client := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	return &TopicDiscoverer{
		logf:     logf,
		opts:     opts,
		ci:       clusterinfo.New(nil, client),
		topics:   make(map[string]*FileLogger),
		hupChan:  hupChan,
		termChan: termChan,
		cfg:      cfg,
	}
}

func (t *TopicDiscoverer) updateTopics(topics []string) {
	for _, topic := range topics { //遍历所有topics
		if _, ok := t.topics[topic]; ok {
			continue
		}

		if !t.isTopicAllowed(topic) {
			t.logf(lg.WARN, "skipping topic %s (doesn't match pattern %s)", topic, t.opts.TopicPattern)
			continue
		}
		//核心就在file_logger, FileLogger实现了Handler,Writer接口,在NewFileLogger()的时候,
		// 通过传递进来的配置,topic去计算输出文件名称,初始化FileLogger,然后初始化消费者,连接进行消费消息
		fl, err := NewFileLogger(t.logf, t.opts, topic, t.cfg)
		if err != nil {
			t.logf(lg.ERROR, "couldn't create logger for new topic %s: %s", topic, err)
			continue
		}
		t.topics[topic] = fl

		t.wg.Add(1)
		go func(fl *FileLogger) {
			fl.router() //在初始化FileLogger后 调用router( ) 方法开始处理数据
			t.wg.Done()
		}(fl)
	}
}

func (t *TopicDiscoverer) run() {
	var ticker <-chan time.Time
	if len(t.opts.Topics) == 0 { // 如果没有opts中没有配置topics, 则通过配置的定时器去lookupd查找topic
		ticker = time.Tick(t.opts.TopicRefreshInterval)
	}
	t.updateTopics(t.opts.Topics) // 为新topic进行分配goroutine消费消息写出文件
forloop:
	for {
		select {
		case <-ticker:
			newTopics, err := t.ci.GetLookupdTopics(t.opts.NSQLookupdHTTPAddrs) // 获取所有的topics
			if err != nil {
				t.logf(lg.ERROR, "could not retrieve topic list: %s", err)
				continue
			}
			t.updateTopics(newTopics) //获取的topics交由函数updateTopics()去处理
		case <-t.termChan:  //收中断,退出信号,遍历topics逐个关闭消费,并退出循环
			for _, fl := range t.topics {
				close(fl.termChan)
			}
			break forloop
		case <-t.hupChan: //  收到hup信号, 遍历topics逐个往fl.hupChan发送通知
			for _, fl := range t.topics {
				fl.hupChan <- true
			}
		}
	}
	t.wg.Wait()
}

func (t *TopicDiscoverer) isTopicAllowed(topic string) bool {
	if t.opts.TopicPattern == "" { //检查配置项，看这个topic的消息是否要写出到文件(fileLogger),
		return true
	}
	match, err := regexp.MatchString(t.opts.TopicPattern, topic) //topic名称是否合法
	if err != nil {
		return false
	}
	return match
}
