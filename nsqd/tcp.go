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

	//nsq已经和客户端约定了必须要发送4字节的protocolMagic来表明使用的协议版本。
	buf := make([]byte, 4)
	//因为一般的read就算没有读到指定长度也会返回，一般需要循环读取，所以此处用ReadFull表示必须读到指定长度的数据后再返回。
	//在golang中没有阻塞和非阻塞的设置，所以当read的底层缓冲为空或者write的底层缓冲满了的时候，就会导致阻塞，这时候如果不希望永久阻塞，就会在服务端的read和write之前设置SetReadDeadline/SetWriteDeadline
	//收到FIN报文时，read()才会读到EOF.如果此处没有读满4个字节，而且也没有收到EOF,那么将一直阻塞，为了防止阻塞才会设置超时。close和shutdown会发送FIN报文。
	//如果读到的数据大于4字节，只读出4字节，那么不会阻塞。
	_, err := io.ReadFull(clientConn, buf) //从流中读取4个字节的数据到buf，作为协议版本号，被读取的数据，会从流中被清掉，后面不会再读到重复的数据，用LimitReader()似乎更好？
	if err != nil {                        //若客户端关闭（EOF err）或者读到的数据不对,那服务端也需要关闭
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
	default: //如果不是"  V2"协议，报错，该goroutine停止，nsqlookupd中的是v1
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn) //Handle函数里面主要就是判断协议版本，然后进入到此处处理每个连接，
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
