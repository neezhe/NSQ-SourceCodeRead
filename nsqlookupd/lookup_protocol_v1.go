package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

type LookupProtocolV1 struct {
	ctx *Context
}

//在这个循环中不断执行nsqd服务的请求
func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn)       //继承了conn
	reader := bufio.NewReader(client) //将client封装成一个拥有size大小缓存的 bufio.Reader对象
	for {                             //读取请求命令
		line, err = reader.ReadString('\n') //持续的从tcp连接中读取数据包，直到遇到'\n'，也包括'\n'，功能同 ReadBytes，只不过返回的是字符串。内部有copy,ReadLine更快，没有copy.
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		//按空格切割用户请求，params存储的就是用户请求命令以及参数,这个参数是啥？请看nsqd的代码。
		params := strings.Split(line, " ")

		var response []byte
		//执行请求并返回结果
		response, err = p.Exec(client, reader, params) //进去看看
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			_, sendErr := protocol.SendResponse(client, []byte(err.Error())) //将错误信息发送给客户端
			if sendErr != nil {
				p.ctx.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}
	//如果上面的循环终止了(程序退出或致命错误)，关闭客户端连接，并删除客户端的所有注册信息
	conn.Close()
	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): closing", client)
	if client.peerInfo != nil {
		registrations := p.ctx.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

//由下面的代码可见lookupd的TCP支持4个命令。
//当客户端初始化完后，首先发送IDENTIFY操作，辨认身份，再发送REGISTER 注册一个Registration category:client key: subkey:
//如果客户端新增一个topic和channel，发送REGISTER，注册这个topic
//如果客户端删除一个topic或chennel，发送UNREGISTER，注销即删除这个注册信息
//TODO 客户端新增或删除channel时，是否调用REGISTER UNREGISTER操作，待确认
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING": //nsqd每隔15s都会向nsqlookupd发送心跳，表明自己还活着；
		return p.PING(client, params)
	case "IDENTIFY": //当nsqd第一次连接nsqlookupd时，发送IDENTITY，验证自己身份；
		return p.IDENTIFY(client, reader, params[1:])
	//nsqd是怎么告诉nsqlookupd,他有个新的topic创建的呢？ 这就得看 REGISTER操作了。
	case "REGISTER": //当nsqd创建一个topic或者channel时，向nsqlookupd发送REGISTER请求，在nsqlookupd上更新当前nsqd的topic或者channel信息；
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER": //当nsqd删除一个topic或者channel时，向nsqlookupd发送UNREGISTER请求，在nsqlookupd上更新当前nsqd的topic或者channel信息；
		//具体各个命令怎么执行，这里就不去分析了；需要提一点是，nsqd的信息是保存在registration_db这样的实例里面的；
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) { //看这个字符串的书写是否合规范
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

//通过http完成了AddRegistration后即完成了create tpoic或Create channel后，现在通过tcp执行REGISTER操作，注册信息
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil { //必须先执行IDENTIFY操作，执行IDENTIFY操作时会填充client.peerInfo
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	//nsqd新建了topic/channel后会发过来,此处读取nsqd发送过来的topic和channel,以备用来查询，
	// 然后根据是否有channel来进行对应的处理：如果有channel，就需要记录(topic,channel)对，否则只需要记录topic就行了，这里数据结构是共用的.
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}
	//注册信息到RegistrationDB
	//如果有channel，需要单独记录一下channel，因为topic和channel可以1:n的
	if channel != "" {
		key := Registration{"channel", topic, channel}
		//把channel和producer关联起来。注意nsqd中的producer指的是nsqd，这里的client.peerInfo是什么时候被赋值的？在IDENTIFY命令中被赋值的
		if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	//注册topic到RegistrationDB
	key := Registration{"topic", topic, ""}
	if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

//从注册表指定的topic或channel中移除producer, 为什么使用tcp方式的UNREGISTER命令完成？(因为tcp方式保持了客户端的信息)。
//1.首先构建key
//2.根据key去注册表中查询该key下的所有producers
//3.for循序遍历producers，如果producers的id与当前客户端的id一致，则移除
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" { //如果传入的参数指定了channel
		key := Registration{"channel", topic, channel}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else { ////如果传入的参数指定了topic,没有指定channel
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		registrations := p.ctx.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			if removed {
				p.ctx.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	}

	return []byte("OK"), nil
}

//客户端初始化后，先进行IDENTIFY操作，验证身份，初始化peerInfo
//IDENTIFY操作在四个操作中最先执行，且一个客户端只执行一次
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil { //已经验证过身份了
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	peerInfo := PeerInfo{id: client.RemoteAddr().String()} //将客户端的地址作为peerInfo的唯一标识符
	err = json.Unmarshal(body, &peerInfo)                  //这个body在哪里被传入的?
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String()

	// require all fields//验证数据完整性
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano()) //修改peerInfo的lastUpdate为当前时间，不理解为什么这样做

	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)
	//将当前client注册到RegistrationDB里，Registration的Category是"client"， Key和SubKey都为空
	client.peerInfo = &peerInfo
	if p.ctx.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// build a response//构建响应数据给client
	data := make(map[string]interface{})
	data["tcp_port"] = p.ctx.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.ctx.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.ctx.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		p.ctx.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate)) //通过 Unix 时间戳生成 time.Time 实例
		now := time.Now()
		p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano()) //赋予变量新值，而不管它原来是什么值。
	}
	return []byte("OK"), nil
}
