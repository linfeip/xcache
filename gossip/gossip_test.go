package gossip

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	beginPort := 6000
	ip := "127.0.0.1"
	c1 := NewCluster(fmt.Sprintf("%s:%d", ip, beginPort))
	beginPort++
	go func() {
		assert(c1.Serve())
	}()

	var locker sync.Mutex
	var clusters []*Cluster
	clusters = append(clusters, c1)

	go func() {
		for {
			t.Logf("start new cluster to join port: %d\n", beginPort)
			cluster := NewCluster(fmt.Sprintf("%s:%d", ip, beginPort))
			beginPort++
			locker.Lock()
			clusters = append(clusters, cluster)
			locker.Unlock()
			go func() {
				assert(cluster.Serve())
			}()
			time.Sleep(time.Second)
			assert(cluster.Join(c1.addr))
			time.Sleep(time.Second * 5)
		}
	}()

	for {
		time.Sleep(1 * time.Second)
		t.Logf("---------------------------------------------------------------------------------------------")
		locker.Lock()
		for _, cluster := range clusters {
			t.Logf("cluster %s nodeLinks len %d", cluster.addr, len(cluster.Nodes()))
		}
		locker.Unlock()
	}
}
