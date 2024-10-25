# 简介

学习基于gossip实现一个简单的缓存集群

```go

var srv1, srv2, srv3 Server
var cli Cache

func init() {
	srv1 = startServer(7000)
	srv2 = startServer(7001)
	srv3 = startServer(7002)
	time.Sleep(time.Second)

	err := srv2.Join(srv1.Myself())
	if err != nil {
		panic(err)
	}

	err = srv3.Join(srv1.Myself())
	if err != nil {
		panic(err)
	}

	cli, err = NewClient(context.Background(), []string{":7000", ":7001"})
	if err != nil {
		panic(err)
	}
}

func TestClient(t *testing.T) {

	cli, err := NewClient(context.Background(), []string{":7000", ":7001"})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.Set("k1", "v1")
	if err != nil {
		t.Fatal(err)
	}

	v, err := cli.Get("k1")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("get value: ", v)
}
```