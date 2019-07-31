package nsqlookupd

import (
	"io"
	"net"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *Context //尼玛，存的是指针，算不算组合？
}
//这个tcpServer只有一个成员ctx,这个context也只有一个成员，即这个nsqlookupd实例的地址,这个context其实主要作用就是在各模块间传递nsqlookupd这个实例，便于访问nsqlookupd地址
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4) //tcp协议格式: 4字节的size，4字节的协议版本号(V1)，之后的都是数据。
	_, err := io.ReadFull(clientConn, buf) //ReadFull将buf读满（就是读指定长度），精确地从clientConn中将len(buf)个字节读取到 buf中。如果字节数不是指定长度，则返回错误信息和正确的字节数。
	if err != nil {//当没有字节能被读时，返回EOF错误。读了一些，但是没读完产生EOF错误时，返回ErrUnexpectedEOF错误。这是读取指定长度的时候用法方法。
		p.ctx.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)
//nsq协议有默认(其实就是v0)和v1，因此代码有根据协议的版本执行不同的代码；我们以协议v1为例；
//这个Handle方法最后调用了LookupProtocolV1.IOLoop方法；由名字可以看出这个IOLoop函数是一个循环
	var prot protocol.Protocol
	switch protocolMagic { //消息的前4个字节是存放客户端的版本，也叫做协议的魔数protocolMagic,根据这个protocolMagic选择对应的该版本处理方式。目前只支持“ V1”.这个设计是为了版本兼容，由不同的版本，在下面开个case就行了。
	case "  V1":  //连接之后，客户端会发送一个4字节的头部来表示版本，这个方法事NSQ约定的，而非TCP协议本身的。见https://nsq.io/clients/tcp_protocol_spec.html
		prot = &LookupProtocolV1{ctx: p.ctx}
	default://如果不是"  V1"协议，则断开链接，打印错误信息，返回E_BAD_PROTOCOL
		protocol.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	//对TCP数据处理的核心逻辑
	err = prot.IOLoop(clientConn)//如果是"  V1"协议，则调用LookupProtocolV1的IOLoop方法
	if err != nil {
		p.ctx.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
