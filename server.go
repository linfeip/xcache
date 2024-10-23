package xcache

import (
	"context"
	"math/rand"
	"net"

	"go.uber.org/zap"
	"xcache/protocol"
)

type ServerOptions struct {
	id uint64
}

type ServerOption func(*ServerOptions)

func WithId(id uint64) ServerOption {
	return func(options *ServerOptions) {
		options.id = id
	}
}

type Server interface {
	Serve() error
	ServeAsync(call func(err error))
	Join(target *protocol.Node) error
	Myself() *protocol.Node
	NodeLen() int
	Shutdown() error
	Id() uint64
}

func NewServer(ctx context.Context, addr string, opts ...ServerOption) Server {
	s := &server{addr: addr}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.id = rand.Uint64()
	for _, opt := range opts {
		opt(&s.ServerOptions)
	}
	s.cluster = newCluster(s.ctx, s.id, s.addr)
	return s
}

type server struct {
	ServerOptions
	ctx      context.Context
	cancel   context.CancelFunc
	addr     string
	cluster  *cluster
	listener net.Listener
}

func (s *server) Id() uint64 {
	return s.id
}

func (s *server) Myself() *protocol.Node {
	return s.cluster.Myself()
}

func (s *server) Serve() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = lis
	go s.cluster.workLoop()

	if log.Level().Enabled(zap.DebugLevel) {
		log.Debugf("server listen addr: %s", s.addr)
	}
	defer func() {
		if log.Level().Enabled(zap.DebugLevel) {
			log.Debugf("server shutdown addr: %s", s.addr)
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			conn, err := lis.Accept()
			if err != nil {
				return err
			}
			if log.Level().Enabled(zap.DebugLevel) {
				log.Debugf("server accept myself: %s local-addr: %s remote-addr: %s", s.addr, conn.LocalAddr(), conn.RemoteAddr())
			}
			c := &connection{
				Conn:    conn,
				handler: s.cluster,
				side:    SideServer,
			}
			go c.serve(s.ctx)
		}
	}
}

func (s *server) ServeAsync(call func(err error)) {
	go func() {
		err := s.Serve()
		call(err)
	}()
}

func (s *server) Join(target *protocol.Node) error {
	return s.cluster.Join(target)
}

func (s *server) NodeLen() int {
	return s.cluster.Len()
}

func (s *server) Shutdown() error {
	s.cancel()
	return s.listener.Close()
}
