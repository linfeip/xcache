package xcache

import (
	"google.golang.org/protobuf/proto"
	"xcache/protocol"
)

type CmdInfo struct {
	New func() proto.Message
	Cmd protocol.CMDType
}

var cmdInfos = map[protocol.CMDType]func() proto.Message{
	protocol.CMDType_TPing: func() proto.Message {
		return &protocol.Ping{}
	},
	protocol.CMDType_TPong: func() proto.Message {
		return &protocol.Pong{}
	},
	protocol.CMDType_TGet: func() proto.Message {
		return &protocol.GetArgs{}
	},
	protocol.CMDType_TGetReply: func() proto.Message {
		return &protocol.GetReply{}
	},
	protocol.CMDType_TSet: func() proto.Message {
		return &protocol.SetArgs{}
	},
	protocol.CMDType_TSetReply: func() proto.Message {
		return &protocol.SetReply{}
	},
	protocol.CMDType_TDel: func() proto.Message {
		return &protocol.DelArgs{}
	},
	protocol.CMDType_TDelReply: func() proto.Message {
		return &protocol.DelReply{}
	},
	protocol.CMDType_TClusterNodes: func() proto.Message {
		return &protocol.ClusterNodesArgs{}
	},
	protocol.CMDType_TClusterNodesReply: func() proto.Message {
		return &protocol.ClusterNodeReply{}
	},
}

type router struct {
	cache   *cache
	cluster *cluster
}

func (r *router) HandleRequest(conn *connection, request *protocol.ProtoFrame) {
	newMsg := cmdInfos[request.GetCmd()]
	if newMsg == nil {
		return
	}
	msg := newMsg()
	err := proto.Unmarshal(request.Data, msg)
	assert(err)
	var frame *Frame
	switch request.Cmd {
	case protocol.CMDType_TPing:
		pong := r.cluster.HandlePing(conn, msg.(*protocol.Ping))
		frame = &Frame{
			ReqId:   request.GetReqId(),
			Cmd:     protocol.CMDType_TPong,
			Message: pong,
		}
	case protocol.CMDType_TPong:
		r.cluster.HandlePong(conn, request.GetReqId(), msg.(*protocol.Pong))
	case protocol.CMDType_TGet:
		args := msg.(*protocol.GetArgs)
		v, err := r.cache.Get(args.Key)
		if err != nil {
			frame = &Frame{
				ReqId: request.GetReqId(),
				Cmd:   protocol.CMDType_TGetReply,
				Error: NewError(500, err),
			}
		} else {
			reply := &protocol.GetReply{Value: v}
			frame = &Frame{
				ReqId:   request.GetReqId(),
				Cmd:     protocol.CMDType_TGetReply,
				Message: reply,
			}
		}
	case protocol.CMDType_TSet:
		args := msg.(*protocol.SetArgs)
		r.cache.Set(args.Key, args.Value)
		frame = &Frame{
			ReqId:   request.GetReqId(),
			Cmd:     protocol.CMDType_TSetReply,
			Message: &protocol.SetReply{},
		}
	case protocol.CMDType_TClusterNodes:
		nodes := r.cluster.Nodes()
		frame = &Frame{
			ReqId: request.GetReqId(),
			Cmd:   protocol.CMDType_TClusterNodesReply,
			Message: &protocol.ClusterNodeReply{
				Nodes: nodes,
			},
		}
	}

	if frame != nil {
		data := conn.encode(frame)
		_, err = conn.Write(data)
		assert(err)
	}
}

func (r *router) HandleDisconnected(conn *connection, err error) {
	r.cluster.HandleDisconnected(conn, err)
}

type Frame struct {
	ReqId   uint64
	Cmd     protocol.CMDType
	Message proto.Message
	Error   *protocol.Error
}

func NewError(code int32, err error) *protocol.Error {
	return &protocol.Error{Code: code, Error: err.Error()}
}
