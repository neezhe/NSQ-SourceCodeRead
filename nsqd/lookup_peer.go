package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/lg"
)

// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
// lookupPeer 代表 nsqd 同 nsqdlookupd 进行连接、读取以及写入操作的一种抽象类型结构
// lookupPeer 实例被设计成延迟连接到 nsqlookupd，并且会自动重连
type lookupPeer struct {
	logf            lg.AppLogFunc
	addr            string // 需要连接到对端的地址信息，即为 nsqlookupd 的地址
	conn            net.Conn// 网络连接
	state           int32 // 当前 lookupPeer 连接的状态 5 种状态之一
	connectCallback func(*lookupPeer) // 成功连接到指定的地址后的回调函数
	maxBodySize     int64 // 在读取命令请求的处理返回结果时，消息体的最大字节数
	Info            peerInfo
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
// peerInfo 代表 lookupPeer 实例的与网络连接相关的信息实体
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// newLookupPeer creates a new lookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		logf:            l,
		addr:            addr,
		state:           stateDisconnected,
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// Connect will Dial the specified address, with timeouts
func (lp *lookupPeer) Connect() error {
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// String returns the specified address
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
// lookupPeer 实例从指定的连接lookupPeer.conn中读取数据，并指定超时时间
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
// lookupPeer 实例将数据写入到指定连接中，并指定超时时间
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
func (lp *lookupPeer) Close() error {
	lp.state = stateDisconnected
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
// 为 lookupPeer执行一个指定（Identify...）的命令，并且获取返回的结果。
// 如果在这之前没有连接到对端，则会先进行连接动作
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	initialState := lp.state
	if lp.state != stateConnected { // 1. 当连接尚未建立时，走这里
		err := lp.Connect() // 2. 发起连接建立过程
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected // 3. 更新对应的连接状态
		_, err = lp.Write(nsq.MagicV1) // 4. 在发送正式的命令请求前，需要要先发送一个 4byte 的序列号，用于指定后面用于通信的协议版本。
		if err != nil {
			lp.Close()
			return nil, err
		}
		if initialState == stateDisconnected {// 5. 在连接成功后，需要执行一个成功连接的回调函数（在正式发送命令请求之前）
			lp.connectCallback(lp)//此回调函数的主要逻辑为nsqd向nsqlookupd发送一个IDENTIFY命令请求以表明自己身份信息，然后遍历自己所维护的topicMap集合，构建所有即将执行的REGISTER命令请求，最后依次执行每一个请求
		}
		if lp.state != stateConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}
	// 6. 在创建 lookupPeer 时会发送一个空的命令请求，其目的为创建正式的网络连接，同时，执行连接成功的回调函数。
	if cmd == nil {
		return nil, nil
	}
	//7. 发送指定的命令请求到对端（包括命令的 name、params，一个空行以及body（写body之前要先写入其长度））
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	resp, err := readResponseBounded(lp, lp.maxBodySize) // 8. 读取并返回响应内容
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
