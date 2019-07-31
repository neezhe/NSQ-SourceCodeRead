package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)
//所有topic列表会存储在NSQD.topicMap[]里面
//总结一下就是nsq的topic主要记录有哪些channel，以及内存管道memoryMsgChan和持久化存储BackendQueue，
//每一个topic后面会有一个消息协程负责处理这个topic的事务。
type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64 // 此 topic 所包含的消息的总数（内存+磁盘）
	messageBytes uint64// 此 topic 所包含的消息的总大小（内存+磁盘）

	sync.RWMutex //读写channel的时候要用到的锁

	name              string
	channelMap        map[string]*Channel //最主要的变量在于channelMap，这是这个topic拥有的所有channel集合。
	backend           BackendQueue //backend是对应的持久化磁盘存储的队列。用interface表示一个结构体和方法的集合。
	memoryMsgChan     chan *Message //memoryMsgChan 是这个topic对应的内存队列，即消息在内存中的通道
	startChan         chan int // 消息处理循环开关
	exitChan          chan int // topic 消息处理循环退出开关
	channelUpdateChan chan int // 消息更新的开关
	waitGroup         util.WaitGroupWrapper // waitGroup 的一个 wrapper，这个是用来等待这个topic的所有消息处理循环messagePump循环退出
	exitFlag          int32  //这个标志是在删除一个topic时，先设置这个，然后再进行真实删除的。PutMessage的时候也会检查这状态，如果正在进行topic删除，那直接返回不入队。
	idFactory         *guidFactory // 用于生成客户端实例的ID

	ephemeral      bool //临时topic以#开头，这样的topic不会写到磁盘上（不会持久化），当此 topic 所包含的所有的 channel 都被删除后，被标记为ephemeral的topic也会被删除
	deleteCallback func(*Topic) // topic 被删除前的回调函数，且对 ephemeral 类型的 topic有效，并且它只在 DeleteExistingChannel 方法中被调用
	deleter        sync.Once

	paused    int32 //标记此topic是否有被paused，若被paused，则其不会将消息写入到其关联的channel的消息队列
	pauseChan chan int

	ctx *context
}

//程序中存在以下几条链来调用NewTopic创建Topic：
// 其一，nsqd.Start->nsqd.LoadMetadata->nsqd.GetTopic->NewTopic；
// 其二，httpServer.getTopicFromQuery->nsqd.GetTopic->NewTopic；
// 其三，protocolV2.PUB/SUB->nsqd.GetTopic这三条调用路径。
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	//初始化一个topic结构，并且设置其backend持久化结构，然后开启消息监听协程messagePump,处理消息。
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback, //topic删除函数，其实是DeleteExistingTopic
		idFactory:         NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}
	//HasPrefix检查字符串前缀开头，HasSuffix检查字符串后缀结尾。
	if strings.HasSuffix(topicName, "#ephemeral") { //临时topic以#ephemeral开头，没有持久化机制，只放入内存中，所以其backend其实是个黑洞，直接丢掉
		t.ephemeral = true
		t.backend = newDummyBackendQueue()//new一个不是真正意义上的BackendQueue,其实只是生成了一个go chan。真正意义的BackendQueue是下面的diskQueue。DummyBackendQueue表示不执行任何有效动作，显然这是考虑到临时的topic不用被持久化。
	} else {
		//正常的topic，需要设置其log函数，以及最重要的，backend持久化机制
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		//下面初始化一下持久化的diskqueue数据结构, 传入路径和文件大小相关的参数，以及sync刷磁盘的配置
		//diskqueue这个包主要功能是消息持久化存储组件。
		//diskQueue是从nsq项目中抽取而来，将它单独作为一个项目go-diskqueue。它本身比较简单，只有一个源文件diskqueue.go。
		t.backend = diskqueue.New( //注意这个diskQueue是一个私有的，必须通过其自带方法才能访问。小写都是针对包而言的，所有的小写都不能被其他包访问，但是能被本包访问。这里的New是大写，也就是说通过暴露出来的方法来操作包的私有化变量。
			topicName,
			ctx.nsqd.getOpts().DataPath, // 数据存储路径，当前目录或指定的目录
			ctx.nsqd.getOpts().MaxBytesPerFile, // 存储文件的最大字节数
			int32(minValidMsgLength), // 最小的有效消息的长度
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength, // 最大的有效消息的长度
			ctx.nsqd.getOpts().SyncEvery, // 单次同步刷新消息的数量，即当消息数量达到 SyncEvery 的数量时，需要执行刷新动作（否则会留在操作系统缓冲区）
			ctx.nsqd.getOpts().SyncTimeout, // 两次同步刷新的时间间隔，即两次同步操作的最大间隔
			dqLogf, // 日志
		)
	}

	t.waitGroup.Wrap(t.messagePump) //异步开启消息监听循环messagePump协程，这是最重要的一步。阻塞等待被唤醒。
	//下面的通知中，已经有一个持久化的操作
	t.ctx.nsqd.Notify(t) //通知lookupd有新的topic产生了，在nsqd的main函数中运行了一个nsqlookupd的协程检测notifyChan中有没有数据，有的话就向nsqlookupd发送，tcp的方式，然后发送Register命令。给所有的nsqlookupd实例。
	return t
}

func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// 根据 channel 名称返回 channel 实例，且有可能是新建的。线程安全方法。
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName) //拿到channel
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		// 若此 channel为新创建的，则 push 消息到 channelUpdateChan中，
		// 使(t *Topic) messagePump中的memoryMsgChan 及 backend 刷新状态
		case t.channelUpdateChan <- 1: //如果时新创建的channel，另一端是(t *Topic) messagePump
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
// 根据 channel 名称获取指定的 channel，若不存在，则创建一个新的 channel 实例。非线程安全
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName] //获取一个channel，如果没有就新建它
	//调用方已经对topic加锁了t.Lock()， 所以不需要加锁
	if !ok {
		// 注册 channel 被删除时的回调函数
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		//不存在，初始化一个channel，设置持久化结构等
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback) //NewChannel新建流程比较简单，也没有topic那种创建后端异步队列的流程
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}
// 此方法由 httpServer.PUB 或 protocolV2.PUB 方法中调用，即生产者通过 http/tcp 投递消息到 topic
//消息的发送操作是二进制的TCP PUB或者http的“/pub?topic=testtopic” 接口，后面其实都是调用的(t *Topic) PutMessage函数去真正发送一条消息到一个topic。
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	//简单看一下是不是我们正在退出状态，如果是就直接返回。这里使用了一个atomic Int32类型的exitFlag退出标志。
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	//真正的发送消息函数是put, 我们知道topic存储目标有2个，一个原生内存管道memoryMsgChan，另外一个是持久化存储backend。怎么判别呢？
	// 答案就是先看memoryMsgChan是否已经满了，如果满了就不能继续塞了，那就存到后端持久化存储里面去。
	//memoryMsgChan的容量由 getOpts().MemQueueSize设置，在上面的 NewTopic 函数里面进行初始化，之后不能修改了。
	err := t.put(m)
	if err != nil {
		return err
	}
	// 3. 更新当前 topic 所对应的消息数量以及消息总大小
	atomic.AddUint64(&t.messageCount, 1) //消息计数
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body))) //记录消息长度
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

//这里memoryMsgChan的大小我们可以通过--mem-queue-size参数来设置，上面这段代码的流程是如果memoryMsgChan还没有满的话
//就把消息放到memoryMsgChan中，否则就放到backend(disk)中。topic的mesasgePump检测到有新的消息写入的时候就开始工作了，
func (t *Topic) put(m *Message) error {
	// 这里巧妙利用了 chan 的特性
	// 先写入memoryMsgChan这个队列,假如 memoryMsgChan已满, 不可写入
	// golang 就会执行 default 语句,
	select {
	case t.memoryMsgChan <- m: //将这条消息直接塞入内存管道，mesasgePump开始处理。
	default: //如果内存消息管道满了(memoryMsgChan的容量由 getOpts().MemQueueSize设置)，那么就放入到后面的持久化存储里面
		b := bufferPoolGet() //从缓冲池中获取缓冲，可复用buffer，减少对象生成，阅读一下sync.Pool包
		err := writeMessageToBackend(b, m, t.backend) //将消息写入持久化消息队列，backend是创建topic的时候建立的diskqueue
		bufferPoolPut(b)// 放回缓存池
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
//当通过http或TCP来PUB消息的时候，会把消息写到topic的memoryMsgChan或Backend，然后通知Topic的messagePump来处理。而Topic的messagePump只在NewTopic的时候会被调用，而调用NewTopic有3条路。
//其一，nsqd.Start->nsqd.LoadMetadata->nsqd.GetTopic->NewTopic；//在程序刚上电时，所有的Topic都必须new出来。
//其二，httpServer.getTopicFromQuery->nsqd.GetTopic->NewTopic；
//其三，protocolV2.PUB/SUB->nsqd.GetTopic这三条调用路径。//当向一个topic上发布或者订阅的时候，会检查这个topic是否存在，不存在就创建。
func (t *Topic) messagePump() { //此函数只在NewTopic的时候被调用。
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	// 1. 等待开启 topic 消息处理循环，即等待调用 topic.Start，
	for {
		select { //没有default语句，select语句将被阻塞,
		case <-t.channelUpdateChan: //当拿到“被新建的”channel的时候，通知此处，但此处continue不做任何操作。
			continue
		case <-t.pauseChan: //
			continue
		case <-t.exitChan:
			goto exit
		//上面的几个chan无法进入下面的数据处理，发现只有这个chan中有数据的话才能跳出此循环，也就是只有当调用了GetTopic中最后的Start函数才行。
		// 但是这个Topic的messagePump函数也是只能由GetTopic的中间创建，也就是说此messagePump先被创建然后阻塞在此处，等GetTopic结束时跳出此for循环。
		//但注意此GetTopic有3条路经会被调用。
		case <-t.startChan: //比如当pub一个消息后（比如curl -d 'hello world 1' 'http://127.0.0.1:4151/pub?topic=test'），在gettopic中初始化完topic后，会通知此处的startChan
			//下面就开始从Memory chan或者disk读取消息
		}
		break
	}
	t.RLock()//避免锁竞争, 所以缓存已存在的 channel
	for _, c := range t.channelMap {  //拿到这个topic的所有channel,多线程中遍历类中一个成员变量的操作, 需要给类加锁
		chans = append(chans, c)
	}
	t.RUnlock()
	//若topic.channelMap 存在 channel，且 topic 未被 paused，则初始化两个通道 memoryMsgChan，backendChan
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan //用户pub消息后，是topic.PutMessage在源源不断的向memoryMsgChan中写数据，memoryMsgChan是在NewTopic的时候设置的大小
		backendChan = t.backend.ReadChan() //下面要从DiskQueue.ReadChan中取消息，在new一个diskqueue的时候会把从文件中读取信息到readChan
	}

	// main message loop
	// 开始从Memory chan或者disk读取消息
	// 如果topic对应的channel发生了变化，则更新channel信息
	for {
		select { //阻塞在这5个chan
		case msg = <-memoryMsgChan: //内存队列,注意每个case互不干扰，msg的值不会进入到下面backendChan的case中
		case buf = <-backendChan: //磁盘队列（文件里）
			msg, err = decodeMessage(buf) //磁盘读出的消息要转换成和内存中一致的消息格式，即message的结构体形式。
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan: //只在channel更新的时候才加锁获取channel,这样就避免了一次循环就加锁获取的低效操作。
			//上面避免锁竞争, 缓存了这个topic已存在的所有channel。假如更新channel，会通知过来,需要重新初始化 memoryMsgChan及 backendChan
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() { //重新初始化
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
			// 当收到 pause 消息时，则将 memoryMsgChan及backendChan置为 nil，注意不能 close，
			// 二者的区别是 nil的chan不能接收消息了，但不会报错。而若从一个已经 close 的 chan 中尝试取消息，则会 panic。
		case <-t.pauseChan:
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan: // 3.4 当调用 topic.exit 时会收到信号，以终止 topic 的消息处理循环
			goto exit
		}
		// 3. 往该tpoic对应的每个channel写入message，因为每个 channel 需要一个独立 msg，因此需要在拷贝时需要创建 msg 的副本，针对 msg 是否需要被延时投递来放到不同的队列(如果是deffermessage
		// 的话放到对应的deffer queue中，否则放到该channel对应的memoryMsgChan中)。
		for i, channel := range chans { //遍历每个channel,然后将消息一个个发送到channel的流程里面.看到没，此处就是将一条topic的消息多播到多有的channel,然后消费者通过订阅的channel读取，如果一个channel上面有多个consumer，则随机。
			//到这里只有一种可能，有新消息来了, 那么遍历channel，调用PutMessage发送消息
			chanMsg := msg //为啥要拷贝？因为每个channel需要一个独立的消息实例
			if i > 0 {// 若此 topic 只有一个 channel，则不需要显式地拷贝了，下面就是在显式拷贝。
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			// 将 msg push 到 channel 所维护的延时消息队列 deferred queue，等待消息的延时时间走完后，会把消息进一步放入到 in-flight queue 中
			if chanMsg.deferred != 0 { //如果是defered延迟投递的消息，那么放入特殊的队列
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg) // 将 msg push 到普通消息队列 in-flight queue
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}
//注意topic删除Delete()函数和topic关闭Close（）函数很相似，区别为：
//一是前者还显式调用了nsqd.Notify以通知nsqlookupd删除topic实例的注册信息，同时重新持久化元数据。
// 二是前者还需要递归删除topic关联的channel集合，且显式调用了channel.Delete方法（此方法同topic.Delete方法相似）。
// 三是前者还显式清空了memoryMsgChan和backend两个消息队列中的消息。
// 因此，若只是关闭或退出topic，则纯粹退出messagePump消息处理循环，并将memoryMsgChan中的消息刷盘，需要持久化所有未被处理/消费的消息，然后再关闭所有的 channel，退出。
// （方法调用链分别为：topic.Delete->topic.exit->nsqd.Notify->nsqd.PersistMetadata->chanel.Delete->topic.Empty->topic.backend.Empty->topic.backend.Delete，以及topic.Close->topic.exit->topic.flush->topic.backend.Close）
// Delete empties the topic and all its channels and closes

//哪些地方会调用Delete方法。其一，httpServer.doDeleteTopic->nsqd.DeleteExistingTopic->topic.Delete；
// 其二，nsqd.GetTopic->nsqd.DeleteExistingTopic->topic.Delete。而对于topic.Close方法，则比较直接：nsqd.Exit->topic.Close。
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	//消息的删除函数，大概做的事情为：1.通知lookupd； 2.关闭topic.exitChan管道让topic.messagePump退出；
	// 3.循环删除其channelMap列表； 4.将内存未消费的消息持久化；

	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) { // 1. 保证目前还处于运行的状态
		return errors.New("exiting")
	}

	if deleted { // 2. 当被　Delete　调用时，则需要先通知 lookupd 删除其对应的注册信息
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t) //通知lookupLoop协程进行处理，有topic更新了，并重新持久化元数据。
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}
	// 3. 关闭 exitChan，保证所有的循环全部会退出，比如消息处理循环 messagePump 会退出
	close(t.exitChan)
	// 4. 同步等待消息处理循环 messagePump 方法的退出，才继续执行下面的操作（只有消息处理循环退出后，才能删除对应的 channel集合）。
	//下面其实是在等待启动消息循环的代码退出： t.waitGroup.Wrap(func() { t.messagePump() }), 这样不会再有在这个topic上的操作了
	// synchronize the close of messagePump()
	t.waitGroup.Wait()
//Wait等待协程退出后，就开始清理channel等信息了。接下来准备清空对应channel的数据 ,
// 看是否是要删除topic来决定调用delete还是close, 这个处理类似topic的处理
	if deleted { // 4. 若是被 Delete 方法调用，则需要清空 topic 所包含的 channel（同 topic 的操作类似）
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		//然后在通知后面的disqqueue进行清理删除
		return t.backend.Delete()
	}
	// 5. 否则若是被 Close 方法调用，则只需要关闭所有的 channel，
	// 不会将所有的 channel 从 topic 的 channelMap 中删除
	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	//如果还有内存消息没处理完需要写入后端的持久化设备，// 6. 将内存中的消息，即 t.memoryMsgChan 中的消息刷新到持久化存储
	t.flush()
	return t.backend.Close()
}
// 清空内存消息队列和持久化存储消息队列中的消息
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}
// 刷新内存消息队列即 t.memoryMsgChan 中的消息到持久化存储 backend
func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}
