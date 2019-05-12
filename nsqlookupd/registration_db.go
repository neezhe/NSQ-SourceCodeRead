package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

//registration_db.go文件用MAP以一对多的形式保存Producer，并提供一系列增、删、改、查的操作封装。同时使用RWMutex做并发控制。
//一个topic对应多个Producer，即nsqd

//定义类型RegistrationDB，字面意思：注册数据库，保存nsqd的注册信息
//按照官网的quick start操作，查看终端输出，可发现nsqd每15秒通过4160端口向nsqlookupd发送心跳包
//简单跟踪nsqlookupd/nsqlookupd.go中的protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)，
//可发现RegistrationDB保存nsqd的注册信息

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}
//nsqd首次注册时，Category:client Key: SubKey:
//nsqd有topic时，Category:topic Key:test SubKey: Key为topic名称
type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []Registration
//通过curl 'http://127.0.0.1:4161/lookup?topic=topic_name'命令查看对应topic的信息
type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}
//对于nsqlookupd来说，它的producer就是nsqd
type Producer struct {
	peerInfo     *PeerInfo
	//是否删除，关于tombstones官网有介绍http://nsq.io/components/nsqlookupd.html#deletion_tombstones
	//英语不行，看不懂，网上查了一下，有翻译成逻辑删除的意思，目前还没搞懂这块
	//大体知道是和删除nsqd有关
	tombstoned   bool
	//删除时间
	tombstonedAt time.Time
}
//定义类型Producers为Producer的slice
type Producers []*Producer
type ProducerMap map[string]*Producer

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
		//make一个map
		registrationMap: make(map[Registration]ProducerMap),
	}
}
// add a registration key
//添加一个registration的key，只设置了map的key，value是一个空的Producers slice
func (r *RegistrationDB) AddRegistration(k Registration) {
	//写锁
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

// add a producer to a registration
//添加producer到指定的registration里
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k]
	//看这个producer是否已存在
	_, found := producers[p.peerInfo.id]
	if found == false {
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
	delete(producers, id)
	//返回操作结果和cleaned slice长度
	return removed, len(producers)
}

// remove a Registration and all it's producers
//删除Registration和对应的producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
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
	//返回指定的category key subkey对应的registration
	//如果key和subkey使用通配符*查找，则返回该条件下的所有数据
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			//返回指定的Registrations
			return Registrations{k}
		}
		return Registrations{}
	}
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
