package nsqd

import (
	"bytes"
	"net"
	"os"
	"strconv"
	"time"
	"encoding/json"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

//当连接建立成功后（不要忘记在这之前发送了一个MagicV1的消息），会执行下面这个回调函数。
//此回调函数的主要逻辑为nsqd向nsqlookupd发送一个IDENTIFY命令请求以表明自己身份信息，然后遍历自己所维护的topicMap集合，
// 构建所有即将执行的REGISTER命令请求，最后依次执行每一个请求。
func connectCallback(n *NSQD, hostname string) func(*lookupPeer) { //返回是函数，闭包的妙用
	return func(lp *lookupPeer) {
		// 1. 打包 nsqd 自己的信息，主要是与网络连接相关
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.RealTCPAddr().Port
		ci["http_port"] = n.RealHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress
		// 2. 发送一个 IDENTIFY 命令请求，以提供自己的身份信息
		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		resp, err := lp.Command(cmd) // 执行IDENTIFY操作
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return

		} else { //解析并校验 IDENTIFY 请求的响应内容

			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
				lp.Close()
				return
			} else {
				n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
				if lp.Info.BroadcastAddress == "" {
					n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
				}
			}
		}

		// build all the commands first so we exit the lock(s) as fast as possible
		// 4. 构建所有即将发送的 REGISTER 请求，用于向 nsqlookupd注册信息 topic 和channel信息
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap { //n.topicMap数据来自于哪里？刚上电的时候，会从文件中读取。
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()
		// 5. 最后，遍历 REGISTER命令集合，依次执行它们，并忽略返回结果（当然肯定要检测请求是否执行成功）
		//REGISTER命令可以用来给topic添加producer
		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

//nsqd和lookup交互的代码
//1.nsqd连接nsqlookupd服务，执行IDENTIFY操作；
//2.将nsqd的Metadata中的topic、channel注册到nsqlookupd服务;
//3.15秒心跳一次，对nsqlookupd执行一次PING操作
//4.新增或删除topic、channel时，REGISTER或UNREGISTER到nsqlookupd服务。
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer // 已连接的lookupd服务实例
	var lookupAddrs []string      // 已链接的lookupd服务地址
	connect := true               //connect默认为true，会执行这段代码，连接到nsqlookupd服务，执行IDENTIFY操作。然后将connect设为false，避免再次执行。

	//添加或减少nsqlookupd服务时，会对已连接的nsqlookupd进行过滤（主要针对减少nsqlookupd服务），然后将connect设置为true，再次执行上面的连接操作。
	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}
	////0.0.0.0：这个IP地址在IP数据报中只能用作源IP地址，这发生在当设备启动时但又不知道自己的IP地址情况下。
	//如果一个主机有两个IP地址，192.168.1.1 和 10.1.2.1，并且该主机上的一个服务监听的地址是0.0.0.0,那么通过两个ip地址都能够访问该服务
	//n.getOpts().NSQLookupdTCPAddresses = []string{"127.0.0.1:4160"}
	//fmt.Println("========begin lookupLoop=================", n.getOpts().NSQLookupdTCPAddresses)
	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		// 1. 在 nsqd 刚创建时，先构造 nsqd 同各 nsqlookupd（从配置文件中读取）的 lookupPeer 连接，并执行一个回调函数
		if connect { // 在 nsqd 启动时会进入到这里，即创建nsqd与各 nsqlookupd 的连接
			for _, host := range n.getOpts().NSQLookupdTCPAddresses { // 配置文件中或命令中指定的nsqlookupd服务的地址nsqlookupd_tcp_addresses
				if in(host, lookupAddrs) { // 如果已经连接过了，跳过
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname)) //建立对应的网络连接的抽象实体(lookupPeer实例)
				lookupPeer.Command(nil)           // 开始建立与lookupd的交互，nil代表连接初始建立，没用实际命令请求

				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host) // 更新 nsqlookupd 的连接实体和地址
			}
			// 赋值给n.lookupPeers
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}
		// 这里是 nsqd 实例处理与 nsqlookupd 实例交互的主循环
		select {
		case <-ticker: //15秒发送一次心跳包，执行一次PING操作,告诉nsqlookup自己在线，此目的是为了及时检测到已关闭的连接
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				// 发送一个 PING 命令请求，利用 lookupPeer 的 Command 方法发送此命令请求，并读取响应，忽略响应（正常情况下 nsqlookupd 端的响应为 ok）
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
			//收到 nsqd 的通知，即 nsqd.Notify 方法被调用，
			// 从 notifyChan 中取出对应的对象 channel 或 topic，（在 channel 或 topic 创建及退出/exit(Delete)会调用 nsqd.Notify 方法）
		case val := <-n.notifyChan: // topic和channel有变化时(新增或删除)，发送REGISTER、UNREGISTER命令，执行REGISTER、UNREGISTER操作
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel: //发送REGISTER/UNREGISTER通知所有的nsqlookupd有channel 添加或除移。
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true { // 若 channel 已退出，即 channel被 Delete，则构造 UNREGISTER 命令请求
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else { // 否则表明 channel 是新创建的，则构造 REGISTER 命令请求
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}
			// 遍历所有nsqd保存的nsqlookupd实例的地址信息，向每个 nsqlookupd 发送对应的 Command
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd) // 这里忽略了返回的结果，nsqlookupd 返回的是 ok
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan: //当从nsqd通过optsNotificationChan通道收到nsqlookupd地址变更消息，则重新从配置文件中加载nsqlookupd的配置信息
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan: //若nsqd退出了，此处理循环也需要退出
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}
// nsqlookupd服务的HTTP地址
func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
