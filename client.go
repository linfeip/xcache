package xcache

import (
	"context"
	"errors"
	"hash/crc32"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"xcache/protocol"
)

func NewClient(ctx context.Context, addrs []string) (Cache, error) {
	c := &client{
		ctx:       ctx,
		addrs:     addrs,
		conn2node: make(map[*connection]*protocol.Node),
		node2conn: make(map[uint64]*connection),
		pendings:  make(map[uint64]*clientPending),
	}
	err := c.init()
	return c, err
}

type clientPending struct {
	reqId    uint64
	expire   time.Time
	callback func(frame *Frame)
}

type client struct {
	ctx       context.Context
	addrs     []string
	nodes     []*protocol.Node
	conn2node map[*connection]*protocol.Node
	node2conn map[uint64]*connection
	pendings  map[uint64]*clientPending
	rw        sync.Mutex
	timeout   time.Duration
}

func (c *client) init() error {
	c.timeout = 5 * time.Second
	var err error
	var cc net.Conn
	for _, addr := range c.addrs {
		cc, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
	}

	go c.workLoop()

	if cc != nil {
		// 找出一个可用节点, 去集群中获取真正的集群信息
		conn := &connection{handler: c, side: SideClient, Conn: cc}
		go conn.serve(c.ctx)
		reqId := genId()
		data := conn.encode(&Frame{
			ReqId:   reqId,
			Cmd:     protocol.CMDType_TClusterNodes,
			Message: &protocol.ClusterNodesArgs{},
		})
		_, err = conn.Write(data)
		if err != nil {
			return err
		}

		signal := make(chan struct{})
		c.pendings[reqId] = &clientPending{
			callback: func(frame *Frame) {
				defer func() {
					close(signal)
					_ = conn.Close()
				}()
				if frame.Error != nil {
					err = errors.New(frame.Error.GetError())
					return
				}
				reply := frame.Message.(*protocol.ClusterNodeReply)
				c.nodes = reply.Nodes
			},
			expire: time.Now().Add(c.timeout),
		}
		<-signal
	}

	return err
}

func (c *client) HandleRequest(conn *connection, f *protocol.ProtoFrame) {
	pending, ok := c.pendings[f.GetReqId()]
	if !ok {
		return
	}
	delete(c.pendings, f.GetReqId())
	if f.Error != nil {
		frame := &Frame{
			ReqId: f.GetReqId(),
			Cmd:   f.Cmd,
			Error: f.Error,
		}
		pending.callback(frame)
		return
	}

	newMsg := cmdInfos[f.GetCmd()]
	if newMsg == nil {
		return
	}
	msg := newMsg()
	err := proto.Unmarshal(f.Data, msg)
	assert(err)

	frame := &Frame{
		ReqId:   f.GetReqId(),
		Cmd:     protocol.CMDType_TClusterNodesReply,
		Message: msg,
	}
	pending.callback(frame)
}

func (c *client) HandleDisconnected(conn *connection, err error) {
	if err != nil {
		log.Errorf("client handle disconnected local-addr: %s remote-addr: %s err: %v", conn.LocalAddr(), conn.RemoteAddr(), err)
	}
	c.rw.Lock()
	defer c.rw.Unlock()
	node := c.conn2node[conn]
	if node != nil {
		delete(c.conn2node, conn)
		delete(c.node2conn, node.Id)
	}
}

func (c *client) Get(k string) (string, error) {
	conn, err := c.getConn([]byte(k))
	if err != nil {
		return "", err
	}
	reqId := genId()
	data := conn.encode(&Frame{
		ReqId: reqId,
		Cmd:   protocol.CMDType_TGet,
		Message: &protocol.GetArgs{
			Key: k,
		},
	})
	c.rw.Lock()
	defer c.rw.Unlock()

	signal := make(chan struct{})
	var v string
	c.pendings[reqId] = &clientPending{
		callback: func(frame *Frame) {
			defer close(signal)
			if frame.Error != nil {
				err = errors.New(frame.Error.GetError())
				return
			}

			reply := frame.Message.(*protocol.GetReply)
			v = reply.GetValue()
		},
		reqId:  reqId,
		expire: time.Now().Add(c.timeout),
	}
	_, err = conn.Write(data)
	if err != nil {
		return "", err
	}
	<-signal
	return v, err
}

func (c *client) Set(k, v string) error {
	conn, err := c.getConn([]byte(k))
	if err != nil {
		return err
	}

	reqId := genId()
	data := conn.encode(&Frame{
		ReqId: reqId,
		Cmd:   protocol.CMDType_TSet,
		Message: &protocol.SetArgs{
			Key:   k,
			Value: v,
		},
	})
	c.rw.Lock()
	defer c.rw.Unlock()

	signal := make(chan struct{})
	c.pendings[reqId] = &clientPending{
		callback: func(frame *Frame) {
			defer close(signal)
			if frame.Error != nil {
				err = errors.New(frame.Error.GetError())
				return
			}
		},
		reqId:  reqId,
		expire: time.Now().Add(c.timeout),
	}
	_, err = conn.Write(data)
	if err != nil {
		return err
	}
	<-signal
	return err
}

func (c *client) ClusterNodes() []*protocol.Node {
	return c.nodes
}

func (c *client) hash(k []byte) uint32 {
	return crc32.ChecksumIEEE(k)
}

func (c *client) getConn(k []byte) (*connection, error) {
	c.rw.Lock()
	defer c.rw.Unlock()
	if len(c.nodes) == 0 {
		return nil, errors.New("no available nodes")
	}
	hash := int(c.hash(k))
	node := c.nodes[hash%len(c.nodes)]
	conn := c.node2conn[node.Id]
	if conn == nil {
		// 发起连接
		cc, err := net.Dial("tcp", node.Addr)
		if err != nil {
			return nil, err
		}
		conn = &connection{handler: c, side: SideClient, Conn: cc}
		c.node2conn[node.Id] = conn
		c.conn2node[conn] = node
		go conn.serve(c.ctx)
	}
	return conn, nil
}

func (c *client) workLoop() {
	after := time.After(time.Second)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-after:
			c.processTimeout()
			after = time.After(time.Second)
		}
	}
}

func (c *client) processTimeout() {
	c.rw.Lock()
	defer c.rw.Unlock()
	now := time.Now()
	for _, pending := range c.pendings {
		if now.After(pending.expire) {
			// 超时
			pending.callback(&Frame{ReqId: pending.reqId, Error: NewError(504, errors.New("request timeout"))})
			delete(c.pendings, pending.reqId)
		}
	}
}
