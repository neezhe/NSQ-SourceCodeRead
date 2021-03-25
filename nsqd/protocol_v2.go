package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"nsq/internal/protocol"
	"nsq/internal/version"
)

const maxTimeout = time.Hour

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	ctx *context
}

//IOLoop 里主要就是启动了两个线程, 一个处理订阅的消息的发送, 一个处理client 发过来的命令请求
// 针对连接到 nsqd 的每一个 client，都会单独在一个 goroutine 中执行这样一个 IOLoop请求处理循环
func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time
	//主要就是在全局结构体nsqd中加1，然后创建client结构，然后把这个client结构体加入到nsqd的map中
	clientID := atomic.AddInt64(&p.ctx.nsqd.clientIDSequence, 1)
	client := newClientV2(clientID, conn, p.ctx)
	p.ctx.nsqd.AddClient(client.ID, client)

	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan) //从该client订阅的channel中读取消息并发送给consumer（通过SendMessage），这个操作的上游就是tpoic的messagePump把消息放入channel
	// 1. 同步 messagePump 的启动过程，因为 messagePump会从client获取的一些属性
	// 而避免与执行 IDENTIFY 命令的 client 产生数据竞争，即当前client在后面可能修改相关的数据
	<-messagePumpStartedChan //阻塞在此处等上句messagePump先关闭messagePumpStartedChan。原因在于，需要确保messagePump里面的初始化完成，当前客户端在后面的操作可能修改相关的数据。消息的订阅发布工作在辅助的messagePump协程处理。

	//下面开始处理client请求，开始循环读取客户端请求然后解析参数，进行处理, 这个工作在客户端的主协程处理
	for {
		//V2版本的协议让客户端拥有心跳功能。每隔 30 秒（默认设置），nsqd 将会发送一个 _heartbeat_ 响应，并期待返回。（在messagePump就一直有一个心跳在发送）
		//如果客户端空闲其回复心跳是发了一个NOP命令。如果 2 个 _heartbeat_ 响应没有被应答， nsqd将会超时（下面设置的），并且强制关闭客户端连接。IDENTIFY 命令可以用来改变/禁用这个行为（因为可以设置超时时间等属性）。
		// read不会因为client本来就没消息而超时，只会阻塞,
		//如果 2个 heartbeat响应没有被应答（因为最长也也不会超过这个时间），nsqd将会强制关闭客户端连接
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2)) //nolint设的也是绝对时间，表示刚accept到读完请求body的时长（已经包括3次握手）。
		} else {
			//若客户端未设置 HeartbeatInterval，则读取等待不会超时。 不设置做心跳则不设置读超时
			client.SetReadDeadline(zeroTime) //nolint
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		// 2.1 读取命令请求，并对它进行解析，解析命令的类型及参数
		line, err = client.Reader.ReadSlice('\n') //读取数据直到遇到指定的界定符为止，第二次读会覆盖第一次读的数据。
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}
		// 这个V2 版本的协议, 一行是一个命令，//V2协议大致为“  V2 command params\n”或“  V2 command params\r\n”
		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'， 处理一下\r, 有可能win版本的回车是 \r\n
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// 每个命令, 用空格来划分参数
		params := bytes.Split(line, separatorBytes)

		p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params)

		var response []byte
		response, err = p.Exec(client, params) //执行这条命令，是FIN还是SUB等
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}
		// 假如命令是有 '响应' 的, 发送响应.此处的放回是针对此刻这个命令的，messagePump中的send返回的是消息。
		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting ioloop", client)
	conn.Close()           //发现只有出错后才会跳出for循环
	close(client.ExitChan) // 通知 messagePump 退出
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	p.ctx.nsqd.RemoveClient(client.ID)
	return err
}

func (p *protocolV2) SendMessage(client *clientV2, msg *Message) error {
	p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): writing msg(%s) to client(%s) - %s", msg.ID, client, msg.Body)
	var buf = &bytes.Buffer{}

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock() // 因为client的处理还有一个 messagePump线程, 所以发送要锁
	//此处设的是发送心跳的超时，在ioloop中设置的是读心跳包的超时。
	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		//SetWriteDeadline和SetReadDeadline是服务端才有的设置。就是说我服务端对数据处理并返回结果的这个时间不能无限制的长，即ServerHTTP函数有时间限制，不然占着资源部释放很危险。
		//SetWriteDeadline时间计算正常是从request header的读取结束开始，到 response write结束为止 (也就是 ServeHTTP 方法的声明周期)。
		//这玩意在每次发送的时候都会设置一遍，因为设的是个绝对值而非相对值。client调用这个方法实际是conn在调用这个方法。
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval)) //nolint
	} else {
		client.SetWriteDeadline(zeroTime) //nolint 0表示无超时，可无限等待。
	}
	//V2协议版本发送给client, 是使用[(4byte)消息长度 , (4byte)消息类型, (载体)] 的 帧格式
	//但是为什么这个格式的封装不写在 protocal_v2.go 而是在protocaol定义上?
	//个人觉得, 具体封包格式的 '具体实现' 应该在 '协议的具体实现'里, 也就是 protocal_v2,而不是 '协议的定义' 里
	_, err := protocol.SendFramedResponse(client.Writer, frameType, data) //为何client.Writer成了io.writer?io包中只是定义了读取器写入器等接口，但真正实现这些接口并不在io包中，
	//比如bufio中就实现了带缓冲的读取器和写入器，此外net.Conn, os.Stdin, os.File/strings.Reader/bytes.Reader，bytes.Buffer/bufio.Reader/Writer等对象都实现了相应功能的读取器和写入器。
	//注意一个语法细节，client.Writer是*bufio.Writer类型而非bufio.Writer，是因为实现io.Writer接口的是*bufio.Writer
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	client.writeLock.Unlock()

	return err
}

//当连接到一个 nsqd 实例时，客户端库必须发送以下数据，顺序是：
//魔术标识符(v2)
//一个 IDENTIFY 命令 (和负载) 和读/验证响应
//一个 SUB 命令 (指定需要的话题（topic)) 和读/验证响应
//一个初始化 RDY 值 1
func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) { //这个命令不需要做认证。按照客户端的调用逻辑，是建立连接成功后，马上发送一个MagicV1的消息不等返回就马上发送"IDENTIFY"命令（表明客户端的身份信息），并接受其返回值。
		return p.IDENTIFY(client, params) //更新服务器上的客户端元数据和协商功能。服务器会根据客户端请求的内容返回“ok”或一个JSON数据，注意：客户端发送了 feature_negotiation (并且服务端支持)，响应体将会是 JSON
	}
	err := enforceTLSPolicy(client, p, params[0]) // client是否做了认证
	if err != nil {
		return nil, err
	}
	switch {
	//当一个客户端成功接收到msg 并处理完成, 按照协议会向nsqd 发送一个 "FIN" 命令通知nsqd, 这时候nsqd 会将这条msg 从InFlight队列中删除
	case bytes.Equal(params[0], []byte("FIN")): //消费者收到msgid后，发送FIN+msgid通知服务器成功投递消息，可以清空消息了
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")):
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")): //若客户端空闲，则对服务端心跳的回复为此命令
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")): //TCP独有,如果 IDENTIFY 响应中有 auth_required=true，客户端必须在 SUB, PUB 或 MPUB 命令前前发送 AUTH 。否则，客户端不需要AUTH认证。
		return p.AUTH(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

//nsqd 针对每一个消费者的消息的处理循环
//2. 客户端发送SUB命令后，SUB()函数会通知客户端的messagePump协程去订阅这个channel的消息；
//3. messagePump协程收到订阅更新的管道消息后，会等待在Channel.memoryMsgChan和Channel.backend.ReadChan()上；
//4. 只要有生产者发送消息后，channel.memoryMsgChan便会有新的消息到来，其中一个客户端就能获得管道的消息；
//5. 拿到消息后调用StartInFlightTimeout将消息放到队列，用来做超时重传，然后调用SendMessage发送给客户端 ；
//此代码的具体执行：
//1.首先刚开始肯定是进行第一个if执行，因为subChannel == nil且客户端也未准备好接收消息。注意此时会将各个channel，并刷新缓冲，而且将flushed设置为true；
//2.应该是identifyEventChan分支被触发，即客户端发送了IDENTIFY命令，此时，设置了部分channel。比如heartbeatChan，因此nsqd可以定期向客户端发送hearbeat消息了。并且heartbeatChan可能在下述的任何时刻触发，但都不影响程序核心执行逻辑；
//3.此时，就算触发了heartbeatChan，上面的仍然执行第一个if分支，没有太多改变；
//4.假如此时客户端发送了一个SUB请求，则此时subEventChan分支被触发，此时subChannel被设置，且subEventChan之后再也不能被触发。此时客户端的状态为stateSubscribed；
//5.接下来，上面的代码执行的仍然是第一个if分支，因为此时subChannel != nil，但是客户端仍未准备好接收消息，即客户端的ReadyCount属性还未初始化；
//6.按正常情况，此时客户端应该会发送RDY命令请求，设置自己的ReadyCount，即表示客户端能够处理消息的数量。
//7.接下来，上面的代码总算可以执行第二个if分支，终于初始化了memoryMsgChan和backendMsgChan两个用于发送消息的消息队列了，同时将flusherChan设置为nil，显然，此时不需要刷新缓冲区；
//8.此时，ReadyStateChan分支会被触发，因为客户端的消息处理能力确实发生了改化；
//9.但ReadyStateChan分支的执行不影响上面代码中被触发的if分支，执行第二个分支。换言之，此时程序中涉及到的各属性没有发生变化；
//10.现在，按正常情况终于要触发了memoryMsgChan分支，即有生产者向此channel所关联的topic投递了消息，因此nsqd将channel内存队列的消息发送给订阅了此channel的消费者。此时flushed为false；
//11.接下来，按正常情况（假设客户端还可以继续消息消息，且消息消费未超时），上面代码应该执行第三个if分支，即设置两个消息队列，并设置flusherChan，因为此时确实可能需要刷新缓冲区了。
//12.一旦触发了flusherChan分支，则flushed又被设置成true。表明暂时不需要刷新缓冲区，直到nsqd发送了消息给客户端，即触发了memoryMsgChan或backendMsgChan分支；
//13.然后可能又进入第二个if分支，然后发送消息，刷新缓冲区，反复循环…
//14.假如某个时刻，消费者的消息处理能力已经变为0了，则此时执行第一个if分支，两个消息队列被重置，执行强刷。显然，此时考虑到消费者已经不能再处理消息了，因此需要“关闭”消息发送的管道。
func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) { //pump就是水泵的意思
	var err error
	var memoryMsgChan chan *Message
	var backendMsgChan chan []byte
	var subChannel *Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time //定义一个单向读channel（从管道出），//表示需要进行显式地刷新（是一个ticker），用于有多个客户端的低容量topic中的channel上的不可控的消息延时。
	var sampleRate int32
	// 1. 获取客户端的属性，client结构体中初始化了这几个channel
	subEventChan := client.SubEventChan                              //subEventChan是客户端发起订阅行为的那个channel结构体，订阅一次后会重置为null。
	identifyEventChan := client.IdentifyEventChan                    //这玩意只在处理客户端的IDENTIFY命令的时候会进行操作。
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout) //NewTicker是反复的，NewTimer是一次的。
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)      //设置和客户端的心跳定时器
	heartbeatChan := heartbeatTicker.C                               //heartbeatTicker.C是一个channel,每隔HeartbeatInterval间隔，<-heartbeatTicker.C就可以拿到一个值。
	msgTimeout := client.MsgTimeout                                  //配置文件里面配置的“同一条消息隔多久后再发送一次”
	// V2 版本的协议采用了选择性地将返回给 client 的数据进行缓冲，即通过减少系统调用频率来提高效率
	// 只有在两种情况下才采取显式地刷新缓冲数据
	// 		1. 当 client 还未准备好接收数据。
	// 			a. 若 client 所订阅的 channel 被 paused
	// 			b. client 的readyCount被设置为0，
	// 			c. readyCount小于当前正在发送的消息的数量 inFlightCount
	//		2. 当 channel 没有更多的消息给我发送了，在这种情况下，当前程序会阻塞在两个通道上
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan) //这一句就是用来解除阻塞的，被关闭的channel不能被写但能被读，被读到的是默认值，当然同一个channel也不能被关闭多次。

	for { //注意这几个if语句都是针对消费者的，生产者虽然也会开这个messagePump协程，但是他并不会用到这几个if语句的逻辑
		//subChannel == nil即此客户端未订阅任何channel或者客户端还未准备好接收消息
		if subChannel == nil || !client.IsReadyForMessages() { //在消费者未发送RDY命令给服务端之前，服务端不会推送消息给客户端
			// the client is not ready to receive messages...
			memoryMsgChan = nil //当客户端订阅的channel还未创建完毕时，或者没有准备好时，与该channel相关联的用于接收消息的内存消息队列和磁盘消息队列都会被置位空，进而不会接收到任何消息。
			backendMsgChan = nil
			flusherChan = nil
			// force flush
			client.writeLock.Lock() //client.writeLock是个读写锁
			err = client.Flush()    // 强制下刷缓冲区。强烈注意bufio.Writer仅在缓存充满或者显式调用Flush方法时处理(发送)数据，即其write函数只是在填写缓冲区而已。
			client.writeLock.Unlock()
			if err != nil {
				goto exit //当for 和 select结合使用时，break语言是无法跳出for之外的，因此若要break出来，这里需要加一个标签，使用goto， 或者break 到具体的位置。
			}
			flushed = true //准备好开始接收数据后，flushed只会在定时到后或有消息到后才被置上。
		} else if flushed { //如果表明上一个循环中，已经flush过了
			memoryMsgChan = subChannel.memoryMsgChan // 这两个 channel 即为 消费者 所订阅的 channel的两个消息队列
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = nil //刷新过一次后，此处不需要再刷新，进入下一轮从chnanel中取数据，有数据的话，下一个分支中的flusherChan又会被置上
		} else { //只有从内存或者磁盘中收到消息时，才会进入到此分支（因为flushed被置为false）
			// 在执行到此之前，subChannel肯定已经被设置过了，且已经从 memoryMsgChan 或 backendMsgChan 取出过消息，因此，可以准备刷新消息发送缓冲区了，即设置flusherChan
			memoryMsgChan = subChannel.memoryMsgChan //messagePump协程收到订阅更新的管道消息后，会等待在Channel.memoryMsgChan和Channel.backend.ReadChan()上；
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = outputBufferTicker.C //开始接受消息时才打开这个flush的定时开关
		}
		select {
		case <-flusherChan: //定时触发此channel，隔一段时间flush一次
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan: //这个case下面没有什么要处理的，目的就是解除阻塞，进行下一轮for循环重新执行一下上面的if语句。
		case subChannel = <-subEventChan: //消费者独有。subChannel在下面被用到，subChannel就是订阅Chnanel,Client需要发送一个SUB请求来订阅Channel
			// 将 subEventChan 重置为nil，原因表示一个消费者订阅了一个channel后就，以后就一致监视这个channel,所以这个case只在订阅的时候被使用一次。
			// 而置为nil的原因是，在SUB命令请求方法中第一行即为检查此客户端是否处于 stateInit 状态，
			// 而调用 SUB 了之后，状态变为 stateSubscribed
			subEventChan = nil //表示再也不能被触发，除非有新的订阅，但是subChannel不为nil
			// 当 nsqd 收到 client 发送的 IDENTIFY 请求时，会设置此 client的属性信息，然后将信息 push 到	identifyEventChan。
			// 因此此处就会收到一条消息，同样将 identifyEventChan 重置为nil，这表明只能从 identifyEventChan 通道中接收一次消息，因为在一次连接过程中，只允许客户端初始化一次。
			// 在IDENTIFY命令处理请求中可看到在第一行时进行了检查，若此时客户端的状态不是 stateInit，则会报错。
			// 最后，根据客户端设置的信息，更新部分属性，如心跳间隔 heartbeatTicker
		case identifyData := <-identifyEventChan: //消费者生产者共有。这玩意只在处理客户端的IDENTIFY命令的时候会进行操作。传递一些由客户端设置的一些参数，参数包括OutputBufferTimeout、HeartbeatInterval、SampleRate以及MsgTimeout，它们是在客户端发出IDENTIFY命令请求时，被压入到identifyEventChan管道的。
			//其中OutputBufferTimeout用于构建flusherChan定时刷新发送给客户端的消息数据，
			// 而HeartbeatInterval用于定时向客户端发送心跳消息，
			// SampleRate（采样率）则用于确定此次从channel中取出的消息，是否应该发送给此客户端	，
			//最后的MsgTimeout则用于设置消息投递并被处理的超时时间，最后会被设置成message.pri作为消息先后发送顺序的依据
			identifyEventChan = nil //一个客户端在建立连接后只允许发送一次IDENTIFY命令，此处表示不再从identifyEventChan获取消息
			//根据客户端设置的信息，更新部分属性
			outputBufferTicker.Stop()                 //ticker中的stop函数会停止ticker数据产生，但是不会关闭ticker的channel
			if identifyData.OutputBufferTimeout > 0 { //上面停止默认的outputBufferTicker下面开启新的outputBufferTicker
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 { //上面停止默认的heartbeatTicker下面开启新的heartbeatTicker
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 { //同样，更新采样率
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		case <-heartbeatChan: //消费者生产者共有。由于有定时器在，所以此for循环不会出现all goroutine are asleep的错误。
			//定时向客户端发送心跳消息，由HeartbeatInterval确定
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
			//开始订阅管道subChannel.memoryMsgChan/backend， 每个客户端都可以订阅到channel的内存或者磁盘队列里面;
		case b := <-backendMsgChan: //消费者独有。关键。当从此通道中收到消息时，表明有channel实例有消息需要发送给此客户端。
			// 根据 client 在与 nsqd 建立连接后，第一次 client 会向 nsqd 发送 IDENFITY 请求,用来为 nsqd 提供 client 自身的信息，即为 identifyData，而 sampleRate 就包含在其中。
			// 换言之，客户端会发送一个 0-100 的数字给 nsqd，在 nsqd 服务端，它通过从 0-100 之间随机生成一个数字，
			// 若其大于 客户端发送过来的数字 sampleRate，则 client 虽然订阅了此 channel，且此 channel中也有消息了，但是不会发送给此 client。
			// 这里就体现了 官方文档 中所说的，当一个 channel 被 client 订阅时，它会将收到的消息随机发送给这一组 client 中的一个。
			// 而且，就算只有一个 client，从程序中来看，也不一定能够获取到此消息，具体情况也与 client 编写的程序规则相关
			if sampleRate > 0 && rand.Int31n(100) > sampleRate { //通过生成一个0到100范围内的随机数，若此随机数小于SampleRate则此消息会发送给此客户端
				continue
			}

			msg, err := decodeMessage(b) // 将消息解码
			if err != nil {
				p.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
			msg.Attempts++ //消息尝试发送的次数msg.Attempts，注意： 当消息发送的次数超过一定限制时，可由 client 自己在应用程序中做处理
			//subChannel就是要发送消息的channel
			//inflight功能用来保证消息的一次到达，所有发送给客户端，但是没收到FIN确认的消息都放到这里面。
			//然后，调用channel实例的StartInFlightTimeout将消息压入到in-flight queue中（代表正在发送的消息队列），等待被queueScanWorker处理。
			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout) //nolint监听这个channel的请求，如果有消息到来，被触发后发送给客户端
			client.SendingMessage()                                     // 更新client 的关于正在发送消息的属性
			err = p.SendMessage(client, msg)                            //正式发送消息到指定的 client，这个很简单，go语言对于网络请求包装的非常像同步读写，比C简单太多了，不需要处理任何内存结构，buffer组织等，方便到不行：
			if err != nil {
				goto exit
			}
			flushed = false
		case msg := <-memoryMsgChan: //消费者独有 9. 从 memoryMsgChan 队列中收到了消息
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			msg.Attempts++
			// 填充消息的消费者ID、投送时间、优先级，然后调用pushInFlightMessage函数将消息放入inFlightMessages字典中。最后调用addToInFlightPQ将消息放入inFlightPQ队列中。
			// 至此，消息投递流程完成，接下来需要等待消费者对投送结果的反馈。消费者通过发送FIN、REQ、TOUCH来回复对消息的处理结果。
			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout) //nolint监听这个channel的请求，如果有消息到来，被触发后发送给客户端
			client.SendingMessage()
			err = p.SendMessage(client, msg) //很奇怪，既然此处已经发送到客户端，那为什么上面StartInFlightTimeout还需要将此消息加入到inflight队列中？因为inFlight队列是NSQ用来实现消息至少投递一次的
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan: //消费者生产者共有。当客户端退出时，则对应的处理循环也需要退出。
			goto exit
		}
	}

exit:
	p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

//命令格式：
//IDENTIFY\n
//[ 4-byte size in bytes ][ N-byte JSON data ]
//长度，json数据
func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice) //lenSlice之只是一个临时用来放数据的切片，没什么重要性
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %+v", client, identifyData)

	err = client.Identify(identifyData) //根据输入改变client结构体中身份信息
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}
	//如果上面这行FeatureNegotiation为true,那么才会进入到下面的的json返回处理
	tlsv1 := p.ctx.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.ctx.nsqd.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 6
	if deflate && identifyData.DeflateLevel > 0 {
		deflateLevel = identifyData.DeflateLevel
	}
	if max := p.ctx.nsqd.getOpts().MaxDeflateLevel; max < deflateLevel {
		deflateLevel = max
	}
	snappy := p.ctx.nsqd.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.ctx.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.ctx.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

//要将nsqd配置为需要授权，需要使用符合auth-http协议的auth服务器指定-auth-http-address=host:port。
//AUTH\n
//[ 4-byte size in bytes ][ N-byte Auth Secret ]
//命令长度，命令内容（是发送的secrete）
func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH already set")
	}

	if !client.ctx.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH disabled")
	}

	if err := client.Auth(string(body)); err != nil { //额外有一个认证服务器，需要自己实现整个认证过程。
		// we don't want to leak errors contacting the auth server to untrusted clients
		p.ctx.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH no authorizations found")
	}

	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if client.ctx.nsqd.IsAuthEnabled() {
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			p.ctx.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

//客户端在指定的 topic 上订阅消息
//消费者使用TCP协议，发送SUB topic channel命令订阅某个channel，NSQD会获取需要订阅的topic和channel，然后将client添加到相应的channel中，通知c.SubEventChan
func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	// 1.做一些校验工作，只有当 client 处于 stateInit 状态才能订阅某个 topic 的 channel，
	// 换言之，当一个client订阅了某个channel之后，它的状态会被更新为stateSubscribed，因此不能再订阅 channel 了。
	// 总而言之，一个 client 只能订阅一个 channel，但一个channel可以有多个client
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}
	// 2. 获取订阅的 topic 名称、channel 名称，并对它们进行校验
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}
	// 3. 同时检查此客户端是否有订阅的权限
	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	// This retry-loop is a work-around for a race condition, where the
	// last client can leave the channel between GetChannel() and AddClient().
	// Avoid adding a client to an ephemeral channel / topic which has started exiting.
	// 此循环是为了避免 client订阅到正在退出的 ephemeral属性的 channel 或 topic
	var channel *Channel
	for {
		//获取topic和channel实例
		topic := p.ctx.nsqd.GetTopic(topicName)
		channel = topic.GetChannel(channelName)
		// 5. 调用 channel的 AddClient 方法添加指定客户端，NSQD结构体中存储的是发布者的client,在channel中存的client是消费者的。
		if err := channel.AddClient(client.ID, client); err != nil { //将client加入到相应的channel中
			return nil, protocol.NewFatalClientErr(nil, "E_TOO_MANY_CHANNEL_CONSUMERS",
				fmt.Sprintf("channel consumers for %s:%s exceeds limit of %d",
					topicName, channelName, p.ctx.nsqd.getOpts().MaxChannelConsumers))
		}
		// 6. 若此 channel 或 topic 为ephemeral，并且channel或topic正在退出，则移除此client
		if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
			channel.RemoveClient(client.ID)
			time.Sleep(1 * time.Millisecond)
			continue
		}
		break
	}
	atomic.StoreInt32(&client.State, stateSubscribed) // 6. 修改客户端的状态为 stateSubscribed
	//下面这行很重要，设置本client所订阅的channel，这样接下来的SubEventChan就会由当前clienid开启的后台协程来订阅这个channel的消息。
	// 7.这一步比较关键，将订阅的 channel实例传递给了client，同时将channel发送到了client.SubEventChan 通道中。
	// 后面的SubEventChan就会使得当前的 client在一个 goroutine中订阅这个channel的消息
	client.Channel = channel //记录下这个消费者对应的channel
	// update message pump
	client.SubEventChan <- channel //通知client的messagePump开始工作了

	return okBytes, nil // 9. 返回 ok
}

//在消费者未发送RDY命令给服务端之前，服务端不会推送消息给客户端，因为此时服务端认为消费者还未准备好接收消息（由方法client.IsReadyForMessages实现）。
//另外，此RDY命令的含义，简而言之，当RDY 100即表示客户端具备一次性接收并处理100个消息的能力，因此服务端此时更可推送100条消息给消费者（如果有的话），
//每推送一条消息，就要修改client.ReadyCount的值。
// 而RDY命令请求的处理非常简单，即通过client.SetReadyCount方法直接设置client.ReadyCount的值。注意在这之前的两个状态检查动作。
func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		p.ctx.nsqd.logf(LOG_INFO,
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed { //由此可见，RDY命令只用在消费者和nsqd之间。
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.ctx.nsqd.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ctx.nsqd.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

//当消费者将channel发送的消息消费完毕后，会显式向nsq发送FIN命令（类似于ACK）。当服务端收到此命令后，就可将消息从消息队列中删除。
// FIN方法首先调用client.Channel.FinishMessage方法将消息从channel的两个集合in-flight queue队列及inFlightMessages 字典中移除。
// 然后调用client.FinishedMessage更新client的维护的消息消费的统计信息。
// 消费者 client 收到消息后，会向 nsqd　响应　FIN+msgID　通知服务器成功投递消息，可以清空消息了
func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing { //只有消费者才会发这个命令？
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}
	// 2. 获取 msgID
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}
	err = client.Channel.FinishMessage(client.ID, *id) //将消息从 channel 的 in-flight queue 及 inFlightMessages 字典中移除
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}

	client.FinishedMessage() // 4. 更新 client 维护的消息消费的统计信息

	return nil, nil
}

//消费者可以通过向服务端发送REQ命令以将消息重新入队，即让服务端一定时间后（也可能是立刻）将消息重新发送给channel关联的客户端。
// 此方法的核心是client.Channel.RequeueMessage，它会先将消息从in-flight queue优先级队列中移除，然后根据客户端是否需要延时timeout发送，
// 分别将消息压入channel的消息队列(memoryMsgChan或backend)，或者构建一个延时消息，并将其压入到deferred queue。
func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) { //重新入队，并指定是否需要延时发送。
	state := atomic.LoadInt32(&client.State) // 1. 先检验 client 的状态以及参数信息
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}
	// 2. 从请求中取出重入队的消息被延迟的时间，并转化单位，同时限制其不能超过最大的延迟时间 maxReqTimeout
	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	maxReqTimeout := p.ctx.nsqd.getOpts().MaxReqTimeout
	clampedTimeout := timeoutDuration

	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}
	// 3. 调用 channel.RequeueMessage 将消息重新入队。首先会将其从 in-flight queue 中删除，
	// 然后依据其 timeout 而定，若其为0,则直接将其添加到消息队列中，否则，若其 timeout 不为0,则构建一个 deferred message，
	// 并设置好延迟时间为 timeout，并将其添加到 deferred queue中
	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}

	client.RequeuedMessage() // 4. 更新 client 保存的关于消息的统计计数

	return nil, nil
}

func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

//客户端发送消息的形式：
//PUB <topic_name>\n
//[ 4-byte size in bytes ][ N-byte binary data ]
//这是用tcp的方式来PUB,但是和用http的方式大同小异，详情见http的doPUB
func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	topicName := string(params[1]) // 1. 读取 topic 名称
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}
	//前面被读走了命令的第一行，下面开始读命令的第二行:数据长度 具体数据
	bodyLen, err := readLen(client.Reader, client.lenSlice) // 2. 先读取消息体长度bodyLen，并在长度上进行校验, 在client 请求的下一行开始, 头4个字节是消息体长度
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)             //建立缓冲区，并将此长度的消息读入
	_, err = io.ReadFull(client.Reader, messageBody) // 3. 读取指定字节长度的消息内容到 messageBody
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}
	//client 是否有权限对这个 topic做PUB 操作
	//客户端必须在 SUB, PUB 或 MPUB 命令前前发送 AUTH
	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}
	//get一下topic，如果没有会自动创建，并且开启topic的消息循环，开始从lookupd同步消息
	topic := p.ctx.nsqd.GetTopic(topicName)
	// 6. 构造一条 message，并将此 message 投递到此 topic 的消息队列中
	msg := NewMessage(topic.GenerateID(), messageBody) //GenerateID产生一个消息的唯一标识符
	err = topic.PutMessage(msg)                        //实际上就是topic.put,将消息放入t.memoryMsgChan或者磁盘后就返回了。客户端流程结束.
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1) // 7. 修改此client发送消息的计数。

	return okBytes, nil // 回复 Ok
}

//MPUB一次性发布多条消息，DPUB用于发布延时投递的消息等等
func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, topic,
		p.ctx.nsqd.getOpts().MaxMsgSize, p.ctx.nsqd.getOpts().MaxBodySize)
	if err != nil {
		return nil, err
	}

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, uint64(len(messages)))

	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.ctx.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1)

	return okBytes, nil
}

//TOUCH命令请求，即重置消息的超时时间
func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	return nil, nil
}

func readMPUB(r io.Reader, tmp []byte, topic *Topic, maxMessageSize int64, maxBodySize int64) ([]*Message, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 4 == total num, 5 == length + min 1
	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || int64(numMessages) > maxMessages {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, NewMessage(topic.GenerateID(), msgBody))
	}

	return messages, nil
}

// validate and cast the bytes on the wire to a message ID
func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("Invalid Message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	if p.ctx.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
