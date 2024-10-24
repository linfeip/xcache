package xcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
	"xcache/protocol"
)

type ConnectionSide int32

const (
	SideServer ConnectionSide = iota
	SideClient
)

type RequestHandler interface {
	HandleRequest(conn *connection, request *protocol.ProtoFrame)
	HandleDisconnected(conn *connection, err error)
}

type connection struct {
	net.Conn
	handler RequestHandler
	side    ConnectionSide
}

func (c *connection) Close() error {
	return c.Conn.Close()
}

func (c *connection) Side() ConnectionSide {
	return c.side
}

func (c *connection) serve(ctx context.Context) {
	defer func() {
		re := recover()
		var err error
		if re != nil {
			switch v := re.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("%v", v)
			}
		}
		if c.handler != nil {
			c.handler.HandleDisconnected(c, err)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 开始读取网络数据
			request := c.decode()
			if c.handler != nil {
				c.handler.HandleRequest(c, request)
			}
		}
	}
}

func (c *connection) encode(f *Frame) []byte {
	data, err := proto.Marshal(f.Message)
	assert(err)
	var frame = &protocol.ProtoFrame{
		Cmd:   f.Cmd,
		Data:  data,
		ReqId: f.ReqId,
		Error: f.Error,
	}
	rData, err := proto.Marshal(frame)
	assert(err)
	total := 4 + len(rData)
	totalBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalBytes, uint32(total))

	buffer := bytes.NewBuffer(make([]byte, 0, total))
	buffer.Write(totalBytes)
	buffer.Write(rData)
	return buffer.Bytes()
}

func (c *connection) decode() *protocol.ProtoFrame {
	var totalBytes [4]byte
	err := binary.Read(c, binary.LittleEndian, &totalBytes)
	assert(err)

	total := binary.LittleEndian.Uint32(totalBytes[:])
	var buffer = make([]byte, total-4)
	_, err = io.ReadFull(c, buffer)
	assert(err)

	var request = &protocol.ProtoFrame{}
	err = proto.Unmarshal(buffer, request)
	assert(err)

	return request
}
