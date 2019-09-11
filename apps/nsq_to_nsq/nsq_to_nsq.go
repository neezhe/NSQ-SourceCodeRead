// This is an NSQ client that reads the specified topic/channel
// and re-publishes the messages to destination nsqd via TCP

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/timer_metrics"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	channel     = flag.String("channel", "nsq_to_nsq", "nsq channel")
	destTopic   = flag.String("destination-topic", "", "use this destination topic for all consumed topics (default is consumed topic name)")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	statusEvery = flag.Int("status-every", 250, "the # of requests between logging status (per destination), 0 disables")
	mode        = flag.String("mode", "hostpool", "the upstream request mode options: round-robin, hostpool (default), epsilon-greedy")

	nsqdTCPAddrs        = app.StringArray{}
	lookupdHTTPAddrs    = app.StringArray{}
	destNsqdTCPAddrs    = app.StringArray{}
	whitelistJSONFields = app.StringArray{}
	topics              = app.StringArray{}

	requireJSONField = flag.String("require-json-field", "", "for JSON messages: only pass messages that contain this field")
	requireJSONValue = flag.String("require-json-value", "", "for JSON messages: only pass messages in which the required field has this value")
)

func init() {
	//自定义类型，绑定到flag，需要实现 flag.Value接口，也就是string(),set()两个接口。若要获取这个自定义的变量的值的话还需要实现get接口,那就成了flag.Getter类型。
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&destNsqdTCPAddrs, "destination-nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
	flag.Var(&whitelistJSONFields, "whitelist-json-field", "for JSON messages: pass this field (may be given multiple times)")
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	addresses app.StringArray
	producers map[string]*nsq.Producer
	mode      int
	hostPool  hostpool.HostPool
	respChan  chan *nsq.ProducerTransaction

	requireJSONValueParsed   bool
	requireJSONValueIsNumber bool
	requireJSONNumber        float64

	perAddressStatus map[string]*timer_metrics.TimerMetrics
	timermetrics     *timer_metrics.TimerMetrics
}

type TopicHandler struct {
	publishHandler   *PublishHandler
	destinationTopic string
}
//responder() 是通过goroutine异步开启的, 主要是异步消息通知进行最终处理. 然后更新统计状态,
// 可能发生的问题就是producer发送出现故障了,导致消息没正常发送出去,就重新入队列
func (ph *PublishHandler) responder() {
	var msg *nsq.Message
	var startTime time.Time
	var address string
	var hostPoolResponse hostpool.HostPoolResponse

	for t := range ph.respChan {
		switch ph.mode {
		case ModeRoundRobin:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = nil
			address = t.Args[2].(string)
		case ModeHostPool:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = t.Args[2].(hostpool.HostPoolResponse)
			address = hostPoolResponse.Host()
		}

		success := t.Error == nil

		if hostPoolResponse != nil {
			if !success {
				hostPoolResponse.Mark(errors.New("failed"))
			} else {
				hostPoolResponse.Mark(nil)
			}
		}

		if success { // 前面判断HostPoolResponse,就是从主机池里面获取的producer发送有无错误
			msg.Finish()  // 正常消费
		} else {
			msg.Requeue(-1) // 重新入队列
		}
		// 更新统计状态
		ph.perAddressStatus[address].Status(startTime)
		ph.timermetrics.Status(startTime)
	}
}

func (ph *PublishHandler) shouldPassMessage(js map[string]interface{}) (bool, bool) {
	pass := true
	backoff := false

	if *requireJSONField == "" {
		return pass, backoff
	}

	if *requireJSONValue != "" && !ph.requireJSONValueParsed {
		// cache conversion in case needed while filtering json
		var err error
		ph.requireJSONNumber, err = strconv.ParseFloat(*requireJSONValue, 64)
		ph.requireJSONValueIsNumber = (err == nil)
		ph.requireJSONValueParsed = true
	}

	v, ok := js[*requireJSONField]
	if !ok {
		pass = false
		if *requireJSONValue != "" {
			log.Printf("ERROR: missing field to check required value")
			backoff = true
		}
	} else if *requireJSONValue != "" {
		// if command-line argument can't convert to float, then it can't match a number
		// if it can, also integers (up to 2^53 or so) can be compared as float64
		if s, ok := v.(string); ok {
			if s != *requireJSONValue {
				pass = false
			}
		} else if ph.requireJSONValueIsNumber {
			f, ok := v.(float64)
			if !ok || f != ph.requireJSONNumber {
				pass = false
			}
		} else {
			// json value wasn't a plain string, and argument wasn't a number
			// give up on comparisons of other types
			pass = false
		}
	}

	return pass, backoff
}

func filterMessage(js map[string]interface{}, rawMsg []byte) ([]byte, error) {
	if len(whitelistJSONFields) == 0 {
		// no change
		return rawMsg, nil
	}

	newMsg := make(map[string]interface{}, len(whitelistJSONFields))

	for _, key := range whitelistJSONFields {
		value, ok := js[key]
		if ok {
			// avoid printing int as float (go 1.0)
			switch tvalue := value.(type) {
			case float64:
				ivalue := int64(tvalue)
				if float64(ivalue) == tvalue {
					newMsg[key] = ivalue
				} else {
					newMsg[key] = tvalue
				}
			default:
				newMsg[key] = value
			}
		}
	}

	newRawMsg, err := json.Marshal(newMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal filtered message %v", newMsg)
	}
	return newRawMsg, nil
}

func (t *TopicHandler) HandleMessage(m *nsq.Message) error {
	return t.publishHandler.HandleMessage(m, t.destinationTopic)
}
//consumer消息消费，主要就是过滤消息, 然后根据均衡策略算法去获取生产者,然后去发送异步消息, 禁用自动响应,使得异步消息结果在responder()方法中异步处理。
func (ph *PublishHandler) HandleMessage(m *nsq.Message, destinationTopic string) error {
	var err error
	msgBody := m.Body

	if *requireJSONField != "" || len(whitelistJSONFields) > 0 {
		var js map[string]interface{}
		err = json.Unmarshal(msgBody, &js)
		if err != nil {
			log.Printf("ERROR: Unable to decode json: %s", msgBody)
			return nil
		}
		// 根据配置进行消息过滤
		if pass, backoff := ph.shouldPassMessage(js); !pass {
			if backoff {
				return errors.New("backoff")
			}
			return nil
		}

		msgBody, err = filterMessage(js, msgBody)

		if err != nil {
			log.Printf("ERROR: filterMessage() failed: %s", err)
			return err
		}
	}
	// 计时开始, 通过时间差Sub 进行耗时计算
	startTime := time.Now()

	switch ph.mode { // 根据均衡策略,生产者去发送异步消息
	case ModeRoundRobin:
		counter := atomic.AddUint64(&ph.counter, 1)
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		p := ph.producers[addr]
		// 使用atomic原子操作, 自增 然后进行取模运算 轮询选取producer, 发布异步消息在ph.respChan进行通知
		err = p.PublishAsync(destinationTopic, msgBody, ph.respChan, m, startTime, addr)
	case ModeHostPool: // 在主机池里面根据算法获取生产者,然后发送异步消息
		hostPoolResponse := ph.hostPool.Get()
		p := ph.producers[hostPoolResponse.Host()]
		err = p.PublishAsync(destinationTopic, msgBody, ph.respChan, m, startTime, hostPoolResponse)
		if err != nil {
			hostPoolResponse.Mark(err)
		}
	}

	if err != nil {
		return err
	}
	m.DisableAutoResponse()  //禁用自动响应反馈 (就是auto finish)
	return nil
}

func hasArg(s string) bool {
	argExist := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == s {
			argExist = true
		}
	})
	return argExist
}
//主要做了解析命令行参数,根据配置创建producer,consumer,然后producer开启N个goroutine(responder())去异步处理消息与更新统计信息,
// consumer通过直连或者服务发现形式去进行消费, 最后就是信号源监听程序收尾工作
func main() {
	var selectedMode int

	cCfg := nsq.NewConfig()
	pCfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cCfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Var(&nsq.ConfigFlag{pCfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_nsq v%s\n", version.Binary)
		return
	}
	// 一系列 输入参数校验
	if len(topics) == 0 || *channel == "" {
		log.Fatal("--topic and --channel are required")
	}

	for _, topic := range topics {
		if !protocol.IsValidTopicName(topic) {
			log.Fatal("--topic is invalid")
		}
	}

	if *destTopic != "" && !protocol.IsValidTopicName(*destTopic) {
		log.Fatal("--destination-topic is invalid")
	}

	if !protocol.IsValidChannelName(*channel) {
		log.Fatal("--channel is invalid")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(destNsqdTCPAddrs) == 0 {
		log.Fatal("--destination-nsqd-tcp-address required")
	}
	// 生产者均衡使用了RoundRobin,HostPool 两种可选策略
	switch *mode {
	case "round-robin": // 轮询的方式
		selectedMode = ModeRoundRobin
	case "hostpool", "epsilon-greedy": // 主机池轮询 --- 贪婪算法
		selectedMode = ModeHostPool
	}

	termChan := make(chan os.Signal, 1) // 信号源的监听对程序进行收尾工作
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	defaultUA := fmt.Sprintf("nsq_to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	cCfg.UserAgent = defaultUA
	cCfg.MaxInFlight = *maxInFlight  //多个消费者 maxInFlight 需要配置,否则接收会有问题
	pCfg.UserAgent = defaultUA

	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs { //这个地址表示指定的生产者的地址
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatalf("failed creating producer %s", err)
		}
		producers[addr] = producer // 生产者 添加进集合, key为地址
	}

	perAddressStatus := make(map[string]*timer_metrics.TimerMetrics) // 给生产者添加 耗时统计
	if len(destNsqdTCPAddrs) == 1 {
		// disable since there is only one address
		perAddressStatus[destNsqdTCPAddrs[0]] = timer_metrics.NewTimerMetrics(0, "")
	} else {
		for _, a := range destNsqdTCPAddrs {
			perAddressStatus[a] = timer_metrics.NewTimerMetrics(*statusEvery,
				fmt.Sprintf("[%s]:", a))
		}
	}

	hostPool := hostpool.New(destNsqdTCPAddrs) // 根据模式选择池子策略 默认的 或者  贪婪算法
	if *mode == "epsilon-greedy" {
		hostPool = hostpool.NewEpsilonGreedy(destNsqdTCPAddrs, 0, &hostpool.LinearEpsilonValueCalculator{})
	}

	var consumerList []*nsq.Consumer

	publisher := &PublishHandler{   // 生产者 处理封装
		addresses:        destNsqdTCPAddrs,
		producers:        producers,
		mode:             selectedMode,
		hostPool:         hostPool,
		respChan:         make(chan *nsq.ProducerTransaction, len(destNsqdTCPAddrs)),
		perAddressStatus: perAddressStatus,
		timermetrics:     timer_metrics.NewTimerMetrics(*statusEvery, "[aggregate]:"),
	}

	for _, topic := range topics {
		consumer, err := nsq.NewConsumer(topic, *channel, cCfg)
		consumerList = append(consumerList, consumer)  // 多消费者循环添加到 consumerList ,并添加handlermessage添加
		if err != nil {
			log.Fatal(err)
		}

		publishTopic := topic
		if *destTopic != "" {
			publishTopic = *destTopic
		}
		topicHandler := &TopicHandler{
			publishHandler:   publisher,
			destinationTopic: publishTopic,
		}
		consumer.AddConcurrentHandlers(topicHandler, len(destNsqdTCPAddrs))
	}
	for i := 0; i < len(destNsqdTCPAddrs); i++ { //  根据生产者个数去开启len个goroutine去异步处理发送结果以及统计
		go publisher.responder()
	}

	for _, consumer := range consumerList { // 消费者直连配置的nsqds去连接消费
		err := consumer.ConnectToNSQDs(nsqdTCPAddrs)
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, consumer := range consumerList { // 通过服务发现去建立连接进行消费
		err := consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
		if err != nil {
			log.Fatal(err)
		}
	}

	<-termChan // wait for signal  // 收到中断信号, consumer 停止消费,释放并退出

	for _, consumer := range consumerList {
		consumer.Stop()
	}
	for _, consumer := range consumerList {
		<-consumer.StopChan
	}
}
