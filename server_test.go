package xcache

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"

	"xcache/protocol"
)

func TestServer(t *testing.T) {
	go func() {
		err := http.ListenAndServe(":6060", nil)
		assert(err)
	}()
	beginPort := 5001
	startServer := func(port int) Server {
		srv := NewServer(context.Background(), fmt.Sprintf(":%d", port), WithId(uint64(port)))
		srv.ServeAsync(func(err error) {
			log.Errorf("server error: %v", err)
		})
		return srv
	}

	node1 := &protocol.Node{
		Addr: ":5000",
		Id:   5000,
	}

	var servers []Server
	for i := 0; i < 3; i++ {
		srv := startServer(beginPort)
		beginPort++
		time.Sleep(time.Second)
		err := srv.Join(node1)
		if err != nil {
			log.Fatal(err)
		}
		servers = append(servers, srv)
	}

	for {
		time.Sleep(time.Second)
		println("=======================================================================================================================")
		for _, srv := range servers {
			log.Debugf("server: %s nodes len = %d", srv.Myself().Addr, srv.NodeLen())
		}
	}
}

func TestExample(t *testing.T) {
	srv := NewServer(context.Background(), ":5000", WithId(5000))
	err := srv.Serve()
	if err != nil {
		t.Fatal(err)
	}
}
