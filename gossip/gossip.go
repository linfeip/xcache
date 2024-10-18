package gossip

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"xcache/gossip/protocol"
)

//go:generate protoc --go_out=. gossip.proto

var log = &logger{}

type ClusterEvent interface {
	HandleJoin(node *NodeLink)
	HandleMaybeFail(node *NodeLink)
	HandleFail(node *NodeLink)
	HandleUpdate(node *NodeLink)
}

func NewCluster(addr string) *Cluster {
	c := &Cluster{
		addr:      addr,
		myself:    &protocol.Node{Addr: addr, Id: uint64(rand.Int63())},
		nodeLinks: make(map[uint64]*NodeLink),
		conns:     make(map[net.Conn]*NodeLink),
	}
	c.nodes = append(c.nodes, c.myself)
	return c
}

type Cluster struct {
	myself    *protocol.Node
	nodes     []*protocol.Node
	nodeLinks map[uint64]*NodeLink
	conns     map[net.Conn]*NodeLink
	rw        sync.RWMutex
	ev        ClusterEvent
	addr      string
}

func (c *Cluster) Serve() error {
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		return err
	}

	log.Debugf("start serve addr: %s", c.addr)
	go c.tick(time.Second)
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Debugf("accept error cluster addr %s", c.addr)
			return err
		}
		go c.handleConn(conn)
	}
}

func (c *Cluster) tick(interval time.Duration) {
	after := time.After(interval)
	for {
		select {
		case <-after:
			c.SendPing()
			after = time.After(interval)
		}
	}
}

func (c *Cluster) Nodes() []*NodeLink {
	c.rw.RLock()
	defer c.rw.RUnlock()
	return c.nodeList()
}

func (c *Cluster) Join(addr string) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	for conn := range c.conns {
		if conn.RemoteAddr().String() == addr {
			return errors.New("node already exists")
		}
	}
	// 跟远程建立连接
	ln := NewNodeLink(&protocol.Node{Addr: addr}, nil)
	err := ln.connect()
	if err != nil {
		return err
	}
	// 发送Join包
	return ln.sendJoin(c.myself)
}

func (c *Cluster) nodeList() []*NodeLink {
	nodes := make([]*NodeLink, 0, len(c.nodeLinks))
	for _, n := range c.nodeLinks {
		nodes = append(nodes, n)
	}
	return nodes
}

func (c *Cluster) handleConn(conn net.Conn) {
	defer func() {
		rr := recover()
		var err error
		if rr != nil {
			switch rr.(type) {
			case error:
				err = rr.(error)
			default:
				err = fmt.Errorf("%v", rr)
			}
		}
		c.handleDisconnect(conn, err)
	}()
	// read from connection
	for {
		var totalBytes [4]byte
		err := binary.Read(conn, binary.LittleEndian, &totalBytes)
		assert(err)

		total := binary.LittleEndian.Uint32(totalBytes[:])
		var buffer = make([]byte, total-4)
		_, err = io.ReadFull(conn, buffer)
		assert(err)

		var request = &protocol.ProtoRequest{}
		err = proto.Unmarshal(buffer, request)
		assert(err)

		switch request.Cmd {
		case protocol.CMDType_TJoin:
			// 处理Join
			join := &protocol.Join{}
			err = proto.Unmarshal(request.GetData(), join)
			assert(err)
			c.handleJoin(conn, join)
			if ln := c.getLink(conn); ln != nil {
				if err = c.sendJoinAck(ln); err != nil {
					log.Debugf("send pong error: %v", err)
				}
			}
		case protocol.CMDType_TJoinAck:
			joinAck := &protocol.JoinAck{}
			err = proto.Unmarshal(request.GetData(), joinAck)
			assert(err)
			c.handleJoinAck(joinAck)
		case protocol.CMDType_TPing:
			ping := &protocol.Ping{}
			err = proto.Unmarshal(request.GetData(), ping)
			assert(err)
			c.handlePing(conn, ping)
			if nl := c.getLink(conn); nl != nil {
				err = c.sendPong(nl)
				if err != nil {
					log.Debugf("send pong error: %v", err)
				}
			}
		case protocol.CMDType_TPong:
			pong := &protocol.Pong{}
			err = proto.Unmarshal(request.GetData(), pong)
			assert(err)
			c.handlePong(pong)
		default:
			panic("not support cmd: " + request.Cmd.String())
		}
	}
}

func (c *Cluster) handleDisconnect(conn net.Conn, err error) {
	c.rw.Lock()
	defer c.rw.Unlock()
	if c.conns[conn] == nil {
		return
	}

	log.Debugf("connection disconnect %s", conn.RemoteAddr().String())
}

func (c *Cluster) getLink(conn net.Conn) *NodeLink {
	c.rw.RLock()
	defer c.rw.RUnlock()
	return c.conns[conn]
}

func (c *Cluster) SendPing() {
	c.rw.RLock()
	defer c.rw.RUnlock()
	// TODO 选出个别节点发送ping
	for _, nl := range c.conns {
		nodes := make([]*protocol.Node, 0, len(c.nodeLinks))
		for _, n := range c.nodeLinks {
			if n == nl {
				continue
			}
			nodes = append(nodes, n.Node())
		}
		err := nl.sendPing(nodes)
		if err != nil {
			log.Debugf("send ping has error: %v", err)
		}
	}
}

func (c *Cluster) sendPong(ln *NodeLink) error {
	// 向目标节点发送Pong协议, 带上当前的该节点中维护的集群信息
	return ln.sendPong(c.randomNodes(3))
}

func (c *Cluster) sendJoinAck(ln *NodeLink) error {
	nodes := c.randomNodes(3)
	return ln.sendJoinAck(nodes)
}

func (c *Cluster) randomNodes(limit int) []*protocol.Node {
	// 随机发送3个节点
	nodes := c.nodes
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	num := min(len(nodes), limit)
	return nodes[:num]
}

func (c *Cluster) handleJoin(conn net.Conn, join *protocol.Join) {
	if join.GetNode() == nil {
		fmt.Printf("conn: %s join node data is nil", conn.RemoteAddr().String())
		return
	}
	c.rw.Lock()
	defer c.rw.Unlock()
	// 已经存在的节点Join忽略
	if _, ok := c.nodeLinks[join.GetNode().GetId()]; ok {
		return
	}
	// 不存在加入集群节点信息
	c.addNewNodes([]*protocol.Node{join.GetNode()})
}

func (c *Cluster) handlePing(conn net.Conn, ping *protocol.Ping) {
	if len(ping.GetNodes()) == 0 { //|| ping.GetTime() == 0 {
		log.Debugf("handlePing err nodeLinks len = %d time = %d", len(ping.GetNodes()), ping.GetTime())
		return
	}
	// 有新的节点
	c.rw.Lock()
	defer c.rw.Unlock()
	c.addNewNodes(ping.GetNodes())
}

func (c *Cluster) addNewNodes(nodes []*protocol.Node) {
	// TODO 新的节点异步握手, 处理节点数据变更
	for _, node := range nodes {
		if _, ok := c.nodeLinks[node.GetId()]; !ok {
			ln := NewNodeLink(node, nil)
			err := ln.connect()
			if err != nil {
				log.Debugf("connect to addr: %s error: %v", node.GetAddr(), err)
				continue
			}
			// 向这个还不知道的节点发送一个加入集群请求
			err = ln.sendJoin(c.myself)
			if err != nil {
				log.Debugf("send join err: %v", err)
				continue
			}
			c.nodeLinks[node.GetId()] = ln
			c.conns[ln.conn] = ln
			c.nodes = append(c.nodes, node)
		}
	}
}

func (c *Cluster) handlePong(pong *protocol.Pong) {
	if len(pong.GetNodes()) == 0 {
		return
	}
	c.rw.Lock()
	defer c.rw.Unlock()
	c.addNewNodes(pong.GetNodes())
}

func (c *Cluster) handleJoinAck(joinAck *protocol.JoinAck) {
	if len(joinAck.GetNodes()) == 0 {
		return
	}
	c.rw.Lock()
	defer c.rw.Unlock()
	c.addNewNodes(joinAck.GetNodes())
}

func assert(err error) {
	if err != nil {
		panic(err)
	}
}

func NewNodeLink(node *protocol.Node, conn net.Conn) *NodeLink {
	return &NodeLink{
		node: node,
		conn: conn,
	}
}

type NodeLink struct {
	node      *protocol.Node
	conn      net.Conn
	lastPing  time.Time
	lastPong  time.Time
	nextReqId uint64
	id        uint64
	state     protocol.NodeState
}

func (ln *NodeLink) Node() *protocol.Node {
	return ln.node
}

func (ln *NodeLink) Addr() string {
	if ln.conn == nil {
		return ""
	}
	return ln.conn.RemoteAddr().String()
}

func (ln *NodeLink) Id() uint64 {
	return ln.id
}

func (ln *NodeLink) State() protocol.NodeState {
	return ln.state
}

func (ln *NodeLink) connect() error {
	conn, err := net.Dial("tcp", ln.node.GetAddr())
	if err != nil {
		return err
	}
	ln.conn = conn
	return nil
}

func (ln *NodeLink) sendJoin(node *protocol.Node) error {
	msg := &protocol.Join{
		Node: node,
	}
	data, err := ln.encode(protocol.CMDType_TJoin, msg)
	if err != nil {
		return err
	}
	_, err = ln.conn.Write(data)
	return err
}

func (ln *NodeLink) sendPong(nodes []*protocol.Node) error {
	msg := &protocol.Pong{
		Nodes: nodes,
	}
	data, err := ln.encode(protocol.CMDType_TPong, msg)
	if err != nil {
		return err
	}
	_, err = ln.conn.Write(data)
	return err
}

func (ln *NodeLink) sendPing(nodes []*protocol.Node) error {
	if len(nodes) == 0 {
		return nil
	}
	msg := &protocol.Ping{
		Nodes: nodes,
	}
	data, err := ln.encode(protocol.CMDType_TPing, msg)
	if err != nil {
		return err
	}
	_, err = ln.conn.Write(data)
	return err
}

func (ln *NodeLink) encode(cmd protocol.CMDType, msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	reqId := atomic.AddUint64(&ln.nextReqId, 1)
	var request = &protocol.ProtoRequest{
		Cmd:   cmd,
		Data:  data,
		ReqId: reqId,
	}
	rData, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	total := 4 + len(rData)
	totalBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalBytes, uint32(total))

	buffer := bytes.NewBuffer(make([]byte, 0, total))
	buffer.Write(totalBytes)
	buffer.Write(rData)
	return buffer.Bytes(), nil
}

func (ln *NodeLink) sendJoinAck(nodes []*protocol.Node) error {
	joinAck := &protocol.JoinAck{
		Nodes: nodes,
	}
	data, err := ln.encode(protocol.CMDType_TJoinAck, joinAck)
	if err != nil {
		return err
	}
	_, err = ln.conn.Write(data)
	return err
}

type logger struct {
}

func (l *logger) Debugf(format string, vv ...any) {
	fmt.Printf(format, vv...)
	fmt.Println()
}
