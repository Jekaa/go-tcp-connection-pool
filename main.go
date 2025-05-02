package main

import (
	"context"
	"fmt"
	"github.com/Jekaa/go-tcp-connection-pool/pool"
	"time"
)

func main() {
	cfg := pool.PoolConfig{
		MaxConns: 10,
		Addr:     "example.com:80",
		Timeout:  5 * time.Second,
		Prefill:  true,
	}

	p, err := pool.NewPool(cfg)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	conn, err := p.Get(context.Background())
	if err != nil {
		panic(err)
	}
	defer conn.Release()

	// Использование соединения
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
	if err != nil {
		conn.MarkUnusable()
		return
	}

	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		conn.MarkUnusable()
		return
	}

	fmt.Println("Response:", string(buf))
}
