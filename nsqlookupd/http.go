package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

type httpServer struct {
	ctx    *Context   //用于传递nsqlookupd地址
	router http.Handler //httprouter实例，用于定义路由以及提供路由查找入口
}

func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqlookupd.logf)
	//实例化一个httprouter，和gin框架中的httprouter是同一个东西。
	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	//设置panic时的处理函数
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqlookupd.logf)
	//设置not found处理函数
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqlookupd.logf)
	//当请求方法不支持时的处理函数
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqlookupd.logf)
	s := &httpServer{
		ctx:    ctx,
		router: router,
	}
	//Decorate是个装饰器，第一个参数为需要被装饰的视图函数，从第二参数开始，都是装饰函数，最后返回装饰好的视图函数
	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText)) //心跳检测，返回OK
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1)) //获取版本信息

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.V1))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.V1)) //返回某个topic下所有的producers
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.V1)) //获取所有的topic
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.V1)) //获取所有的channels
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.V1)) //获取所有的节点

	// only v1
	//在创建channel的时候和创建topic的时候不一样，
//channel注册的时候注册了两部分内容:
//	1.根据channel构建一个key，注册channel和Producers的映射
//	2. 再根据channel对应的topic构建一个key, 注册topic和Producers的映射
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))//创建topic， 这里只是创建topic而不会添加该topic的producer
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1)) //删除topic
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1)) //创建channel, 也不会添加producer
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))//删除channel
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}
//获取所有topic
func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	//获取所有topic的名称
	topics := s.ctx.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}
//获取指定topic下的所有channel
func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}
//当完成了create topic/channel，完成了消费者register后，就开始下面lookup了
//获取指定topic下的所有有效producer
//当消费者需要查询某个topic在哪些nsqd上时，可以发送/lookup 的http命令查询，nsqlookupd的查询topic信息的接口是这样的：curl 'http://127.0.0.1:4161/lookup?topic=testtopic'
//找到Channel对应的生产者Producers, 然后与这些Producers建立连接，订阅相关信息
//lookup操作是根据Topic查询该topic底下的所有channel，和对应的producers。所以包含三个步骤:
//1.根据Topic，看看是否已经注册了。如果没有注册抛出异常
//2.否者将topic下的所有channel获取出来
//3.获取该topic下的所有producers
func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic") //从url中拿到tpoic对应的值
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	// 1. 根据Topic，看看是否已经注册了。如果没有注册抛出异常
	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {//topic不存在
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}
	// 2. 将topic下的所有channel获取出来。*表示取出所有
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	// 3. 获取该topic下的所有producers
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	//去掉不活跃的producer机器
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}
//在lookupd上增加了一条topic的记录，并且他的producers为空
func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	//创建一个topic， 只是简单的记录了一下topic信息，并且加到DB里面，对应的nsqd数组为空
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {//验证topic名称是否合法
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)
	key := Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}
//topic底下有多个channel，所有topic的删除首先需要删除channel，然后再删除topic
func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	// 先查找所有的channel注册并删除
	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf(LOG_INFO, "DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}
	//查询并删除topic
	registrations = s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf(LOG_INFO, "DB: removing topic(%s)", topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}
//逻辑删除指定topic的一个节点(producer)
func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	//node格式 <broadcast_address>:<http_port>
	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")//根据topic查producer
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {//逻辑删除
			p.Tombstone()
		}
	}

	return nil, nil
}
//创建topic和channel
func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	//获取topic和channel，并验证是否是有效参数
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	//新增channel
	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.ctx.nsqlookupd.DB.AddRegistration(key)
	//新增topic
	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}
//删除已有topic下的一个channel
func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	//获取topic和channel，并验证是否是有效参数
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	//查询出所有的已经注册的registrations
	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations { //根据这些key从registryMap中删除
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration) // 删除
	}

	return nil, nil
}
//定义节点信息(producer (nsqd))
type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}
//获取所有已知节点(nsqd)
func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	topicProducersMap := make(map[string]Producers)
	for i, p := range producers {
		topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			if _, exists := topicProducersMap[t]; !exists {
				topicProducersMap[t] = s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
			}

			topicProducers := topicProducersMap[t]
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
					break
				}
			}
		}

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.ctx.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	return data, nil
}
