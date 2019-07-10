package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/go-diskqueue"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
//channel最重要的几个结构应该是跟延迟投递消息，以及可靠性保证的消息的相关数据结构。两者基本类似，都是用最小堆优先级队列实现的。
type Channel struct { //nsqd的channel是最接近消费者端的，有他特有的东西，包括投递，确认等；
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64 // 需要重新排队的消息数
	messageCount uint64// 接收到的消息的总数
	timeoutCount uint64 // 正在发送的消息的数量

	sync.RWMutex

	topicName string// 其所属的 topic 名称
	name      string // channel 名称
	ctx       *context// 存放nsqd 实例

	backend BackendQueue //磁盘持久化存储队列
	//注意此处的Message结构，和Topic中的一样。代表生产者或者消费者的一条消息。是nsq消息队列系统中最基本的元素。
	memoryMsgChan chan *Message//channel的消息管道，topic发送消息会放入这里面，所有SUB的客户端会用后台协程订阅到这个管道后面, 1:n

	exitFlag      int32 // 退出标识（同 topic 的 exitFlag 作用类似）
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer //与此 channel关联的client集合，即订阅的Consumer集合所有订阅的topic都会记录到这里。也可以用来在关闭的时候清理client，以及根据clientid找client。
	paused         int32 // 若paused属性被设置，则那些订阅了此channel的客户端不会被推送消息
	ephemeral      bool // 标记此 channel 是否是临时的
	deleteCallback func(*Channel)//实际上就是DeleteExistingChannel，删除回调函数（同 topic 的 deleteCallback 作用类似）
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	//延迟投递消息，消息体会放入deferredPQ，并且由后台的queueScanLoop协程来扫描消息，将过期的消息照常使用c.put(msg)发送出去
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue //被延迟发送的消息集合，它是一个最小堆优先级队列,其中优先级比较字段为消息发送时间(Item.Priority)
	deferredMutex    sync.Mutex
	//正在发送中的消息记录，直到收到客户端的FIN才会删除，否则timeout到来会重传消息的.
	//这里应用层有个坑，如果程序处理延迟了，那么可能重复投递，那怎么办,应用层得注意这个，设置了timeout就得接受有重传的存在，（因此client需要对消息做去重处理 de-duplicate）
	inFlightMessages map[MessageID]*Message //已经发送给client但是没有得到client确认的消息,叫inFlightMessages。
	inFlightPQ       inFlightPqueue//代表正在发送的消息集合，同样是最小堆优先级队列，优先级比较字段也为消息发送时间(Message.pri)。
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
//同topic涉及的逻辑非常相似，只不过channel还初始化了两个用于存储发送消息的优先级队列inFlightPQ和deferredPQ
//3条调用此方法的链：
//其一，nsqd.Start->nsqd.LoadMetadata->topic.GetChannel->topic.getOrCreateChannel->NewChannel；//刚启动的时候
// 其二，httpServer.doCreateChannel->topic.GetChannel；//消费者订阅channel的时候
// 其三，protocolV2.SUB->topic.GetChannel。//客户端在订阅channel的时候
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ()// 初始化 channel 维护的两个消息队列

	if strings.HasSuffix(channelName, "#ephemeral") {
		// 3. 同　topic　类似，那些 ephemeral 类型的 channel 不会关联到一个 BackendQueue，
		// 而只是被赋予了一个 dummy BackendQueue
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		// 4. 实例化一个后端持久化存储，同样是通过 go-diskqueue  来创建的，
		// 其初始化参数同 topic 中实例化 backendQueue 参数类似
		c.backend = diskqueue.New( //创建磁盘队列diskqueue
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	c.ctx.nsqd.Notify(c)// 5. 通知 lookupd 添加注册信息

	return c
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}
//删除和关闭channel的方法与topic中的类似，但有两个不同点：
// 其一，无论是关闭还是删除channel都会显式地将订阅了此channel的客户端强制关闭（当然是关闭客户端在服务端的实体）；
// 其二，关闭和删除channel都会显式刷新channel，即将channel所维护的三个消息队列：
// 内存消息队列memoryMsgChan、正在发送的优先级消息队列inFlightPQ以及被推迟发送的优先级消息队列deferredPQ，将它们的消息显式写入到持久化存储消息队列。
// Delete empties the channel and closes
func (c *Channel) Delete() error {// 删除此 channel，清空所有消息，然后关闭
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {// 只是将三个消息队列中的消息刷盘，然后关闭
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {// 1. 保证还未被设置 exitFlag，即还在运行中，同时设置 exitFlag
		return errors.New("exiting")
	}

	if deleted {// 2. 若需要删除数据，则通知 nsqlookupd，有 channel 被删除
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	// 3. 强制关闭所有订阅了此 channel 的客户端
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {// 4. 清空此 channel 所维护的内存消息队列和持久化存储消息队列中的消息
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete() // 5. 删除持久化存储消息队列中的消息
	}

	// write anything leftover to disk
	c.flush()// 6. 强制将内存消息队列、以及两个发送消息优先级队列中的消息写到持久化存储中
	return c.backend.Close() // 7. 关闭持久化存储消息队列
}
// 清空 channel 的消息
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()// 1. 重新初始化（清空） in-flight queue 及 deferred queue
	// 2. 清空由 channel 为客户端维护的一些信息，比如 当前正在发送的消息的数量 InFlightCount
	// 同时更新了 ReadyStateChan
	for _, client := range c.clients {
		client.Empty()
	}
	// 3. 将 memoryMsgChan 中的消息清空
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}
	// 4. 最后将后端持久化存储中的消息清空
finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
// 将未消费的消息都写到持久化存储中，
// 主要包括三个消息集合：memoryMsgChan、inFlightMessages和deferredMessages
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}
	// 1. 将内存消息队列中的积压的消息刷盘
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}
	// 2. 将还未发送出去的消息 inFlightMessages 也写到持久化存储
finish:
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()
	// 3. 将被推迟发送的消息集合中的 deferredMessages 消息也到持久化存储
	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
//发送消息给后面的消费者
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m) //将消息放入channel.memoryMsgChan里面，或者放到后台持久化里面，如果客户端来不及接受的话, 那就存入文件。
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1) //更新消息计数
	return nil
}
//channel发送消息也很简单
//channel的putmessage，还是老办法，将消息放入channel.memoryMsgChan里面，
//或者放到后台持久化里面，如果客户端来不及接受的话, 那就存入文件
//这里客户端是如何接收到消息的呢？可以看SUB命令了.
//sub命令最后会调用到client.SubEventChan &lt;- channel, 也就是说，会在这个客户端对应的消息循环里面记录这个channel.memoryMsgChan
//并且监听他，任何客户端SUB到某个channel后，其消息循环便会订阅到对应这个channel的memoryMsgChan上面，
//所以同一个channel，客户端随机有一个能收到消息. 这里我们知道，channel的消息发送也是通过管道，
//而管道的另一端，则是所有订阅到这上面的client的消息处理协程

func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m: //此memoryMsgChan中的消息只有flush函数才能拿去。
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}
//延迟投递消息
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	//延迟投递消息，也算一个messageCount。 参数timeout是延迟秒数
	atomic.AddUint64(&c.messageCount, 1)//更新消息计数
	c.StartDeferredTimeout(msg, timeout) //将 message 添加到 deferred queue 中
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	//收到FIN命令结束倒计时，完美投递
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
// 将消息重新入队。这与 timeout 参数密切相关。
// 当 timeout == 0 时，直接将此消息重入队。
// 否则，异步等待此消息超时，然后 再将此消息重入队，即是相当于消息被延迟了
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id) // 1. 先将消息从 inFlightMessages 移除
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg) // 2. 同时将消息从 in-flight queue 中移除，并更新 chanel 维护的消息重入队数量 requeueCount
	atomic.AddUint64(&c.requeueCount, 1)
	// 3. 若 timeout 为0,则将消息重新入队。即调用 channel.put 方法，将消息添加到 memoryMsgChan 或 backend
	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout) // 否则，创建一个延迟消息，并设置延迟时间
}

// AddClient adds a client to the Channel's client list
//SUB订阅消息仅限于TCP协议，客户端通过SUB命令订阅上来，最后调用cannnel.AddClient函数，
// 函数很简单，就在channel上记录了一下当前clientid，以备后面进行清理等使用
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	//sub命令触发到这里，在channel上面增加一个client。因为在channel中维护了一个client的map
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return nil
	}

	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}

	c.clients[clientID] = client
	return nil
}

// RemoveClient removes a client from the Channel's client list
//另外clients还有一个作用，就是如果topic属于临时队列，不需要保存历史痕迹的，
// 如果所有消费者都已经退出后，这会删除channel，进而如果所有channel都关闭了，就会删除上层的topic
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		//所有client都退出了，如果是临时的ephemeral topic，就会删除这个channel，
		// 实际上就是DeleteExistingChannel
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}
//nsqd对于消息的确认到达，是通过消费者发送FIN+msgid来实现的，可想而知，他需要一个队列记录：当前正在发送，待确认的消息列表，类似窗口协议，性能差点可能，这就是：InFlightQueue 。
//发送客户端消息的时候，会调用StartInFlightTimeout来记录当前的infight消息，以备进行重传。如果客户端收到消息，会发送FIN命令来结束消息倒计时，不用重传了。简单看看StartInFlightTimeout代码。
//这个函数会在客户端的protocolV2) messagePump 循环体里面，当某个客户端连接得到内存或者磁盘消息后，在发送前会设置这个inflight队列，
// 用来记录当前正在发送的消息，如果超时时间到来还没有确认收到，那么会有queueScanLoop循环去扫描，并且重发这条消息,
// 最后会调用到processInFlightQueue
//此函数将消息加入到inflight队列中，这里只是将消息存入队列，那么在哪里消费呢？前面Nsqd在完成监听部分的初始化后，有四个自启动的goroutine，启动的n.queueScanLoop()就是用来执行消费的。
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()//初始化消息的过期时间为timeout+now,pri是优先级的缩写，此处就是按照过期的优先级进行堆排序存放的。
	err := c.pushInFlightMessage(msg)//此处入队，processInFlightQueue处出队。
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)//将msg加入到InFlight队列中，InFlight其实是一个堆排序队列，优先级是按照超时时间来排序的，越靠近过期时间，将会越靠前
	return nil
}
//可以看到，对于延迟投递的消息最后就是用消息到达的时间戳来当优先级值，放入优先级队列。
//放入到优先级队列里面. 那么这条消息怎么触发呢？ 在main函数里面，会启动queueScanLoop协程，后者会定时启动扫描任务扫描所有channels,
// 然后去处理这个优先级队列，调用processDeferredQueue 函数，如果有到期的消息就触发他；
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	//延迟投递消息，计算绝对时间absTs, 然后新建一个pqueue.Item放到优先级队列里面，优先级就是absTs，按照时间顺序排列
	absTs := time.Now().Add(timeout).UnixNano() // 1. 计算超时超时戳，作为 Priority
	item := &pqueue.Item{Value: msg, Priority: absTs} // 2. 构造 item
	//下面就是记录一下deferredMessages[id] = item，防止重复延迟投递
	err := c.pushDeferredMessage(item) // 3. item 添加到 deferred 字典
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item) // 4. 将 item 放入到 deferred message 优先级队列
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}
//主要逻辑就是从优先级队列里面循环读取已到期的消息，然后将其重新调用put函数发送出去，跟客户端PUB是一个效果：
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		//循环处理到期消息，这里如果有大量消息没有处理，就会死循环一直到处理完毕为止
		//目测不是太好，如果业务设置某个时间到期的消息，基本上nsqd应该会卡的不行了
		//会不断在这里处理这些消息，当然对于服务是还能响应的
		c.deferredMutex.Lock()
		//如果有取一条的到期消息
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		//从c.deferredMessages 删除
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		//调用常规发送消息的put函数去照常投递消息
		c.put(msg)
	}

exit:
	return dirty
}
///把 InFlightQueue里,优先级小于参数t的,全部重新发送
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock() // 先检查是否已经退出
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t) // 从队列中获取已经过期的消息
		c.inFlightMutex.Unlock()

		if msg == nil {//如果没有取到消息，那么就返回，仍然保持dirty := false
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID) // 如果获取到了符合条件的msg，按msg.ID将msg在infight队列中删除
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		c.put(msg)// 消息在channel中发起重新投递，重新塞入原始发送队列: channel.memoryMsgChan <- m
	}

exit:
	return dirty
}
