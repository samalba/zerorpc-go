
package zerorpc

import (
        "fmt"
        "bytes"
        zmq "github.com/alecthomas/gozmq"
        msgpack "github.com/msgpack/msgpack-go"
       )


var (
        PAIR   = zmq.PAIR
        PUB    = zmq.PUB
        SUB    = zmq.SUB
        REQ    = zmq.REQ
        REP    = zmq.REP
        DEALER = zmq.DEALER
        ROUTER = zmq.ROUTER
        PULL   = zmq.PULL
        PUSH   = zmq.PUSH
        XPUB   = zmq.XPUB
        XSUB   = zmq.XSUB
    )

type Socket interface {
    Connect(endpoint string)
    Invoke(method string, args ...interface{}) interface{}
}

type zerorpcSocket struct {
    context zmq.Context
    socket zmq.Socket
}


func NewSocket(t zmq.SocketType) Socket {
    context, _ := zmq.NewContext()
    socket, _ := context.NewSocket(t)
    return &zerorpcSocket{context, socket}
}

func (c *zerorpcSocket) Connect(endpoint string) {
    fmt.Printf("Connecting to \"%v\"\n", endpoint)
    c.socket.Connect(endpoint)
}

func buildMessage(method string, args []interface{}) []byte {
    buf := &bytes.Buffer{}
    headers := make(map[string]string)
    //TODO: implement message_id with uuid
    headers["message_id"] = "deadbeef1234"
    data := make([]interface{}, 3)
    data[0] = headers
    data[1] = method
    data[2] = args
    msgpack.Pack(buf, data)
    return buf.Bytes()
}

func (c *zerorpcSocket) Invoke(method string, args ...interface{}) interface{} {
    message := buildMessage(method, args)
    fmt.Println(message)
    c.socket.Send(message, 0)
    raw, _ := c.socket.Recv(0)
    buf := bytes.NewBuffer(raw)
    value, _, _ := msgpack.Unpack(buf)
    data := value.Interface().([]interface{})
    for k, v := range data[0].(map[interface{}]interface{}) {
        fmt.Printf("%s = %s\n", k, v)
    }
    fmt.Printf("%s\n", data[1])
    return data[2].([]interface{})[0]
}


func NewClient(endpoint string) Socket {
    socket := NewSocket(REQ)
    socket.Connect(endpoint)
    return socket
}
