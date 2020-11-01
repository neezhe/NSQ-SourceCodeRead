package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

//registration_db.go文件用MAP以一对多的形式保存Producer，并提供一系列增、删、改、查的操作封装。同时使用RWMutex做并发控制。
//一个topic对应多个Producer，为啥？因为一个topic和channel可能由多个producer发布。

//定义类型RegistrationDB，字面意思：注册数据库，保存nsqd的注册信息
//按照官网的quick start操作，查看终端输出，可发现nsqd每15秒通过4160端口向nsqlookupd发送心跳包
//简单跟踪nsqlookupd/nsqlookupd.go中的protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)，可发现RegistrationDB保存nsqd的注册信息
// RegistrationDB 以 map 结构包含了所有节点信息; 名为db, 实则最多算个cache罢了,但是实现了一系列的增删改查封装，同时使用RWMutex做并发控制。
type RegistrationDB struct { //见有道笔记图解
	sync.RWMutex                                 //读写锁，因为对Registration的增删改查可能多个线程在同时进行，所以需要加锁保证安全。
	registrationMap map[Registration]ProducerMap //以 Registration 为 key 储存 Producers, 即生产者nsqd。把topic/channel和producer关联起来。
	//这个key是怎么产生的？例如创建一个“aa”的topic,那么产生的key就为 Registration{"topic", “aa”, ""}
}
type Registration struct {
	Category string // 这个用来指定key的类型，比如是topic或者channel或者client(第一次连接nsqlookup的IDENTITY验证信息 )
	Key      string // 存放定义的topic值
	SubKey   string // 如果是channel类型，存放channel值
}
type Registrations []Registration

//通过curl 'http://127.0.0.1:4161/lookup?topic=topic_name'命令查看对应topic的信息
type PeerInfo struct {
	lastUpdate       int64  //上次更新时间.
	id               string // 唯一id表示这个Producer,就是nsqd的ip:port
	RemoteAddress    string `json:"remote_address"`    // 远程地址，和id竟然都是一个意思
	Hostname         string `json:"hostname"`          // 主机名字
	BroadcastAddress string `json:"broadcast_address"` // 广播地址， 和远程地址区别？？
	TCPPort          int    `json:"tcp_port"`          // tcp端口
	HTTPPort         int    `json:"http_port"`         // http 端口
	Version          string `json:"version"`           // 版本，大概用来做版本兼容使用
}

//对于nsqlookupd来说，它的producer就是nsqd，每个Producer代表一个生产者，存放该Producer的一些信息:
type Producer struct {
	peerInfo *PeerInfo //// 节点信息
	//是否删除，关于tombstones官网有介绍http://nsq.io/components/nsqlookupd.html#deletion_tombstones
	//英语不行，看不懂，网上查了一下，有翻译成逻辑删除的意思，目前还没搞懂这块
	//大体知道是和删除nsqd有关
	tombstoned bool
	//删除时间
	tombstonedAt time.Time
}

//定义类型Producers为Producer的slice
type Producers []*Producer
type ProducerMap map[string]*Producer //string是PeerInfo中的唯一标识符id(就是nsqd的ip:port字符串)

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

//将Producer标记为tombstone，并记录标记的时间
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

//判断Producer是否为tombstone状态
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	//func (t Time) Sub(u Time) Duration 计算两个时间的差(time.Now() - p.tombstonedAt)
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

//新建RegistrationDB类型变量
func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap), //make一个
	}
}

//管理RegistrationDB，本质上使用的就是map的增、删、查操作, 无非是先构建Registration类型的key, 根据key去操作。
// 因为可能多个操作同时在并行执行，为了保住线程安全，每个涉及到RegistrationDB.registrationMap的增、删、查操作都利用了RegistrationDB定义的读写锁进行加锁，
func (r *RegistrationDB) AddRegistration(k Registration) { //创建Topic和Chnnel是通过Http请求接口完成。通过http创建一个topic或channel.
	//写锁
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k] //Registration是否已经在registrationMap
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer) //只设置了registrationMap的key，value是一个空的Producers列表，后续添加Producer的工作则交给了AddProducer方法
	}
}

//通过http调用AddRegistration完成注册topic或channel后，下面tcp通过AddProducer来添加producer,负责将Producer添加到注册表中key对应的producers列表中
// add a producer to a registration
//就是对registrationMap 的查询，如果当前这个客户端以及在映射表里面就不需要处理，否则就加进去。
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok { //就算没有这个Registration在registrationMap没有value，我此处要要给这个key创建一个空的value
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k] //返回值是一个ProducerMap类型
	//看这个producer是否已存在
	_, found := producers[p.peerInfo.id] //可见同一个topic下可以有不同ip的nsqd,但这些nsqd的生产者可以相同
	if !found {
		//producer不存在时，则添加
		producers[p.peerInfo.id] = p
	}
	//返回添加成功或失败(true或false)
	return !found
}

// remove a producer from a registration
//从registration里删除一个指定的producer
//入参：k registration中的key id producer.peerInfo.id
//返回操作成功或失败和剩下的producer个数
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	//判断registration中是否存在这个key
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false

	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	//删除一个producer的方法，开始删除。
	delete(producers, id) //标准库函数，删除map中指定的key表示的项
	//返回操作结果和cleaned slice长度
	return removed, len(producers)
}

// remove a Registration and all it's producers
//删除Registration和对应的producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock() //为了保证数据的一致性，此处开始加锁
	defer r.Unlock()
	delete(r.registrationMap, k)
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

//根据category key subkey查找registrations slice
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) { //此处如果没有*通配符，则唯一查找
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok { //因为所有的Registration都唯一的存在registrationMap中，所以此处这样判断其是否存在
			return Registrations{k} //数组，所以要用大括号
		}
		return Registrations{}
	}
	//若查找条件里面有通配符*,比如，若key为*,则找出key所有的值。
	results := Registrations{}
	for k := range r.registrationMap {
		//不符合，则跳过当前循环，继续下一个循环
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	//返回满足通配符条件的Registrations
	return results
}

//根据category key subkey查找Producers slice，逻辑和上面的FindRegistrations差不多
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])
	}

	results := make(map[string]struct{})
	var retProducers Producers
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, found := results[producer.peerInfo.id]
			if found == false {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

//根据producer.peerInfo.id查找registration
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		if _, exists := producers[id]; exists {
			results = append(results, k)
		}
	}
	return results
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

//获取指定的registrations
func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

//获取registrations中的所有key
func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

//获取registrations中的所有SubKey
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

//获取所有可用的producer，即nsqd
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		//根据nsqlookupd终端输出，并简单跟踪nsqlookupd/nsqlookupd.go中的protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)，
		//在nsqlookupd/lookup_protocol_v1.go的LookupProtocolV1.PING()方法中发现atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
		//可知producer.peerInfo.lastUpdate为producer与nsqlookupd最新交互时间
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		//满足下面两个条件的producer将被忽略
		//1、超过了活跃时间，在inactivityTimeout时间内，没有与nsqlookupd交互
		//2、被标记为tombstone状态，在tombstoneLifetime时间内标记的producer被忽略
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

//获取Producers中的所有peerInfo
func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	return producers
}
