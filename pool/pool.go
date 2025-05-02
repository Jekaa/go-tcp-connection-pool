package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed = errors.New("connection pool is closed")
	ErrTimeout    = errors.New("get connection timed out")
)

type Conn interface {
	net.Conn
	MarkUnusable()
	Release()
}

type ConnectionPool struct {
	mu          sync.RWMutex
	addr        string
	pool        chan *poolConn
	maxConns    int32
	activeConns int32
	timeout     time.Duration
	closed      int32
	dialer      func(context.Context) (net.Conn, error)
}

type poolConn struct {
	net.Conn
	pool     *ConnectionPool
	unusable int32
}

type PoolConfig struct {
	MaxConns int
	Addr     string
	Timeout  time.Duration
	Dialer   func(context.Context) (net.Conn, error)
	Prefill  bool
}

func NewPool(cfg PoolConfig) (*ConnectionPool, error) {
	if cfg.MaxConns <= 0 {
		return nil, errors.New("max connections must be positive")
	}

	pool := &ConnectionPool{
		addr:     cfg.Addr,
		maxConns: int32(cfg.MaxConns),
		pool:     make(chan *poolConn, cfg.MaxConns),
		timeout:  cfg.Timeout,
		dialer:   cfg.Dialer,
	}

	if pool.dialer == nil {
		pool.dialer = func(ctx context.Context) (net.Conn, error) {
			return net.DialTimeout("tcp", cfg.Addr, cfg.Timeout)
		}
	}

	if cfg.Prefill {
		for i := 0; i < cfg.MaxConns/2; i++ {
			conn, err := pool.createConn(context.Background())
			if err != nil {
				continue
			}
			pool.pool <- conn
		}
	}

	return pool, nil
}

func (p *ConnectionPool) Get(ctx context.Context) (Conn, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, ErrPoolClosed
	}

	select {
	case conn := <-p.pool:
		if conn.isUnusable() {
			conn.Close()
			return p.Get(ctx)
		}
		return conn, nil
	default:
		return p.createConn(ctx)
	}
}

func (p *ConnectionPool) createConn(ctx context.Context) (*poolConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32(&p.activeConns) >= p.maxConns {
		select {
		case conn := <-p.pool:
			if conn.isUnusable() {
				conn.Close()
				return p.createConn(ctx)
			}
			return conn, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(p.timeout):
			return nil, ErrTimeout
		}
	}

	conn, err := p.dialer(ctx)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&p.activeConns, 1)
	return &poolConn{
		Conn: conn,
		pool: p,
	}, nil
}

func (p *ConnectionPool) put(conn *poolConn) {
	if atomic.LoadInt32(&p.closed) == 1 || conn.isUnusable() {
		conn.Close()
		return
	}

	select {
	case p.pool <- conn:
	default:
		conn.Close()
	}
}

func (p *ConnectionPool) Close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}

	close(p.pool)
	for conn := range p.pool {
		conn.Close()
	}
}

func (c *poolConn) MarkUnusable() {
	atomic.StoreInt32(&c.unusable, 1)
}

func (c *poolConn) Release() {
	if atomic.LoadInt32(&c.unusable) == 1 {
		c.Close()
		return
	}
	c.pool.put(c)
}

func (c *poolConn) Close() error {
	if atomic.CompareAndSwapInt32(&c.unusable, 0, 1) {
		defer atomic.AddInt32(&c.pool.activeConns, -1)
	}
	return c.Conn.Close()
}

func (c *poolConn) isUnusable() bool {
	return atomic.LoadInt32(&c.unusable) == 1 ||
		!isConnAlive(c.Conn)
}

func isConnAlive(conn net.Conn) bool {
	if conn == nil {
		return false
	}

	err := conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	if err != nil {
		return false
	}

	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == io.EOF {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		conn.SetReadDeadline(time.Time{})
		return true
	}

	return false
}

func (p *ConnectionPool) Stats() (current, max int) {
	return int(atomic.LoadInt32(&p.activeConns)), int(p.maxConns)
}
