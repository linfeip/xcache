package xcache

import "sync/atomic"

var nextId uint64

func assert(err error) {
	if err != nil {
		panic(err)
	}
}

func genId() uint64 {
	return atomic.AddUint64(&nextId, 1)
}
