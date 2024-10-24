package xcache

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	"xcache/protocol"
)

type ClusterEvent interface {
	OnFail(node *protocol.Node)
	OnPFail(node *protocol.Node)
	OnActive(node *protocol.Node)
}

var (
	pingTimeout = time.Second * 15
	dialTimeout = time.Second * 3
)

type pendingPing struct {
	conn   *connection
	expire time.Time
	ctime  time.Time
}

func newCluster(ctx context.Context, id uint64, addr string, handler RequestHandler) *cluster {
	cl := &cluster{
		ctx: ctx,
		myself: &protocol.Node{
			Addr: addr,
			Id:   id,
		},
		nodes:        make(map[uint64]*protocol.Node),
		node2conn:    make(map[uint64]*connection),
		conn2node:    make(map[*connection]*protocol.Node),
		pendingPings: make(map[uint64]*pendingPing),
		pfails:       make(map[uint64]map[uint64]struct{}),
		handler:      handler,
	}
	// 把自己节点加入到nodes中
	cl.nodes[cl.myself.Id] = cl.myself
	return cl
}

type cluster struct {
	rw           sync.RWMutex
	ctx          context.Context
	myself       *protocol.Node
	nodes        map[uint64]*protocol.Node      // nodes
	node2conn    map[uint64]*connection         // node-client
	conn2node    map[*connection]*protocol.Node // client-node
	pendingPings map[uint64]*pendingPing
	pfails       map[uint64]map[uint64]struct{} // 可能下线的节点投票记录
	handler      RequestHandler
}

func (cl *cluster) Myself() *protocol.Node {
	return cl.myself
}

func (cl *cluster) Join(target *protocol.Node) error {
	cl.rw.RLock()
	if _, ok := cl.nodes[target.Id]; ok {
		cl.rw.RUnlock()
		return fmt.Errorf("join target id: %d already exists", target.Id)
	}
	cl.rw.RUnlock()

	node := &protocol.Node{
		Addr: target.Addr,
		Id:   target.Id,
	}
	cc, err := net.DialTimeout("tcp", node.Addr, dialTimeout)
	if err != nil {
		return err
	}
	cl.rw.Lock()
	defer cl.rw.Unlock()

	client := &connection{
		Conn:    cc,
		handler: cl.handler,
		side:    SideClient,
	}
	cl.nodes[node.Id] = node
	cl.node2conn[node.Id] = client
	cl.conn2node[client] = node
	go client.serve(cl.ctx)

	reqId := genId()
	data := client.encode(&Frame{
		ReqId: reqId,
		Cmd:   protocol.CMDType_TPing,
		Message: &protocol.Ping{
			Nodes: []*protocol.Node{cl.myself},
			Self:  cl.myself,
		},
	})
	_, err = client.Write(data)
	if err != nil {
		return err
	}

	now := time.Now()
	expire := now.Add(pingTimeout)
	cl.pendingPings[reqId] = &pendingPing{
		conn:   client,
		expire: expire,
		ctime:  now,
	}

	return nil
}

func (cl *cluster) Len() int {
	cl.rw.RLock()
	defer cl.rw.RUnlock()
	l := 0
	for _, n := range cl.nodes {
		if n.State < protocol.NodeState_NSPFail {
			l++
		}
	}
	return l
}

func (cl *cluster) Nodes() []*protocol.Node {
	cl.rw.RLock()
	defer cl.rw.RUnlock()
	nodes := make([]*protocol.Node, 0, len(cl.nodes))
	for _, n := range cl.nodes {
		if n.State < protocol.NodeState_NSFail {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (cl *cluster) HandleDisconnected(conn *connection, err error) {
	cl.rw.Lock()
	defer cl.rw.Unlock()

	if log.Level().Enabled(zap.DebugLevel) {
		log.Debugf("cluster handle error myself id: %d addr: %s local-addr: %s remote-addr: %s, side: %d, err: %v",
			cl.myself.Id,
			cl.myself.Addr,
			conn.LocalAddr(),
			conn.RemoteAddr(),
			conn.Side(),
			err)
	}

	// 删除连接
	node, ok := cl.conn2node[conn]
	if ok {
		delete(cl.conn2node, conn)
		delete(cl.node2conn, node.Id)
		// 更新节点信息成PFail
		if node.State < protocol.NodeState_NSPFail {
			node.State = protocol.NodeState_NSPFail
			cl.pfails[node.Id] = make(map[uint64]struct{})
			cl.pfails[node.Id][cl.myself.Id] = struct{}{}
			cl.OnPFail(node)
		}
	}
}

func (cl *cluster) HandlePing(conn *connection, msg *protocol.Ping) *protocol.Pong {
	// server端收到ping包, 然后响应pong包
	cl.rw.Lock()
	defer cl.rw.Unlock()

	sender := cl.conn2node[conn]
	cl.processNodes(sender, msg.Nodes)
	// 向ping的目标节点写回pong消息
	// 在当前节点集群中随机出3个节点响应到pong中
	nodes := cl.randNodes(3, msg.Self.Id)
	return &protocol.Pong{Nodes: nodes}
}

func (cl *cluster) HandlePong(conn *connection, reqId uint64, msg *protocol.Pong) {
	// client端接收到pong包, 删除等待pong包的request id
	// 对当前节点不存在的集群节点发起连接
	cl.rw.Lock()
	defer cl.rw.Unlock()
	delete(cl.pendingPings, reqId)
	sender := cl.conn2node[conn]
	cl.processNodes(sender, msg.Nodes)
}

func (cl *cluster) OnActive(node *protocol.Node) {
	if log.Level().Enabled(zap.DebugLevel) {
		log.Debugf("OnActive myself id: %d addr: %s node: %d addr: %s",
			cl.myself.Id,
			cl.myself.Addr,
			node.Id,
			node.Addr)
	}
}

func (cl *cluster) OnFail(node *protocol.Node) {
	if log.Level().Enabled(zap.DebugLevel) {
		log.Debugf("OnFail myself id: %d addr: %s node: %d addr: %s",
			cl.myself.Id,
			cl.myself.Addr,
			node.Id,
			node.Addr)
	}
}

func (cl *cluster) OnPFail(node *protocol.Node) {
	if log.Level().Enabled(zap.DebugLevel) {
		log.Debugf("OnPFail myself id: %d addr: %s node: %d addr: %s",
			cl.myself.Id,
			cl.myself.Addr,
			node.Id,
			node.Addr)
	}
}

// processNodes 处理响应的nodes, 如果当前集群中不存在就发起连接
func (cl *cluster) processNodes(sender *protocol.Node, nodes []*protocol.Node) {
	for _, node := range nodes {
		if nd, ok := cl.nodes[node.Id]; ok {
			// 更新节点信息
			if sender != nil && node.State >= protocol.NodeState_NSPFail {
				if pfail, ok := cl.pfails[node.Id]; ok {
					// 如果sender节点跟当前节点都认为node已经PFail, 就给他投票
					pfail[sender.Id] = struct{}{}
					if len(cl.pfails[node.Id]) >= (len(cl.nodes)/2 + 1) {
						// 如果超过半数, 都认为这个节点下线了, 那么就把节点状态从PFail -> Fail
						nd.State = protocol.NodeState_NSFail
						// 移除PFail的队列
						delete(cl.pfails, node.Id)
						if log.Level().Enabled(zap.DebugLevel) {
							log.Debugf("node state change to Fail id: %d addr: %s", nd.Id, nd.Addr)
						}
						// 通知变成Fail
						// TODO: 并且进行主从切换
						cl.OnFail(nd)
					}
				}
			}
			continue
		}
		// 新的集群节点
		cl.nodes[node.Id] = node
		if node.State < protocol.NodeState_NSPFail {
			// 新的节点, 并且不是PFail, Fail节点, 建立client去连接对端节点
			cc, err := net.DialTimeout("tcp", node.Addr, dialTimeout)
			if err != nil {
				log.Errorf("node: %d addr: %s dial err: %v", node.Id, node.Addr, err)
				continue
			}
			// 保存连接节点之间的对应关系
			client := &connection{Conn: cc, handler: cl.handler, side: SideClient}
			cl.node2conn[node.Id] = client
			cl.conn2node[client] = node
			node.State = protocol.NodeState_NSActive
			cl.OnActive(node)
			go client.serve(cl.ctx)
		}
	}
}

func (cl *cluster) randNodes(limit int, filters ...uint64) []*protocol.Node {
	nodes := make([]*protocol.Node, 0, limit)
	for _, node := range cl.nodes {
		if cl.contains(node.Id, filters) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func (cl *cluster) contains(id uint64, ids []uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func (cl *cluster) workLoop() {
	after := time.After(time.Second)
	for {
		select {
		case <-cl.ctx.Done():
			return
		case <-after:
			cl.processPending()
			cl.processPing()
			after = time.After(time.Second)
		}
	}
}

func (cl *cluster) processPending() {
	cl.rw.Lock()
	defer cl.rw.Unlock()
	now := time.Now()
	for reqId, pending := range cl.pendingPings {
		if now.After(pending.expire) {
			if log.Level().Enabled(zap.DebugLevel) {
				log.Debugf("ping timeout reqId: %d myself: %s local-addr: %s remote-addr: %s",
					reqId,
					cl.myself.Addr,
					pending.conn.LocalAddr(),
					pending.conn.RemoteAddr(),
				)
			}
			delete(cl.pendingPings, reqId)
			_ = pending.conn.Close()
		}
	}
}

func (cl *cluster) processPing() {
	cl.rw.Lock()
	defer cl.rw.Unlock()

	nodes := cl.randNodes(3, cl.myself.Id)
	now := time.Now()
	expire := now.Add(pingTimeout)
	for _, node := range nodes {
		pingNodes := cl.randNodes(3, node.Id)
		conn := cl.node2conn[node.Id]
		if conn != nil {
			reqId := genId()
			data := conn.encode(&Frame{
				ReqId: reqId,
				Cmd:   protocol.CMDType_TPing,
				Message: &protocol.Ping{
					Nodes: pingNodes,
					Self:  cl.myself,
				},
			})
			_, err := conn.Write(data)
			if err != nil {
				log.Errorf("process ping failed, myself id: %d addr: %s conn write err: %v", cl.myself.Id, cl.myself.Addr, err)
				_ = conn.Close()
				continue
			}

			cl.pendingPings[reqId] = &pendingPing{
				conn:   conn,
				expire: expire,
				ctime:  now,
			}
		} else {
			// 没有建立连接的, 需要尝试进行连接
			cc, err := net.DialTimeout("tcp", node.Addr, dialTimeout)
			if err != nil {
				log.Errorf("process ping dial node id: %d addr: %s err: %v", node.Id, node.Addr, err)
				continue
			}
			// 成功建立连接
			client := &connection{Conn: cc, handler: cl.handler, side: SideClient}
			// 加入连接中
			cl.node2conn[node.Id] = client
			cl.conn2node[client] = node
			node.State = protocol.NodeState_NSActive
			go client.serve(cl.ctx)
			// 如果在PFail队列中需要移除了
			delete(cl.pfails, node.Id)
			// fire OnActive
			cl.OnActive(node)
		}
	}
}
