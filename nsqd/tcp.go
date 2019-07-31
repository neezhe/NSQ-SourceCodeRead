package nsqd

import (
	"io"
	"net"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *context //注意此处并不是继承，
}
//由此可见p.ctx.nsqd是一个全局的结构体，运行的所有的服务（topic/channel的一切行为）都是在p.ctx.nsqd创建之后，所以他能被所有服务使用。
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)//从流中读取4个字节的数据到buf，作为协议版本号，被读取的数据，会从流中截取掉，用LimitReader()似乎更好？
	if err != nil { //若客户端关闭（EOF err）或者读到的数据不对,那服务端也需要关闭
		p.ctx.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{ctx: p.ctx}
		//假如有另外一个版本的协议
		//case "  V3":
		//	prot = &protocolV3{ctx: p.ctx}
	default://如果不是"  V2"协议，报错，该goroutine停止，nsqlookupd中的是v1
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn) //进入到此处处理每个连接，
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
