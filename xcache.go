package xcache

import "errors"

type ValType int32

const (
	ValTypeString ValType = iota
	ValTypeList
	ValTypeHashmap
)

type Value struct {
	Value any
	Type  ValType
}

type Cache interface {
	Get(k string) (string, error)
	Set(k, v string) error
}

func newCache() *cache {
	return &cache{kvs: make(map[string]string)}
}

type cache struct {
	kvs map[string]string
}

func (c *cache) Get(k string) (string, error) {
	v, ok := c.kvs[k]
	if !ok {
		return "", errors.New("key not found")
	}
	return v, nil
}

func (c *cache) Set(k string, v string) error {
	c.kvs[k] = v
	return nil
}
