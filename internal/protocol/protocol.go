package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface { //将协议封装, 方便以后不同协议的扩展
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4
	//大端：高位字节放在内存的低地址端，低位字节放在内存的高地址端。
	//小端：低位字节放在内存的低地址段，高位字节放在内存的高地址端。
	//为了程序的兼容，我们在开发跨服务器的TCP服务时，每次发送和接受数据都要进行转换，这样做的目的是保证代码在任何计算机上执行时都能达到预期的效果。
	//大端序或者小端序，取决于软件开始时通讯双方的协议规定(双方都用大端或者小端)。TCP/IP协议RFC1700规定使用“大端”字节序为网络字节序，开发的时候需要遵守这一规则。默认golang是使用大端序。
	binary.BigEndian.PutUint32(beBuf, size) //PutUint32表示把第二个参数通过大端的形式存到第一个参数表示的byte数组中,此处把消息的长度信息要用大端的方式返回
	n, err := w.Write(beBuf)//此处和下面几行都是发送，和一次发送是一样的效果
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}
