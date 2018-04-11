package bloomd

import (
	"net"
	"net/url"
	"sync"

	pool "gopkg.in/fatih/pool.v2"
)

// Factory a factory for conn for the Bloomd client
type Factory func() (net.Conn, error)

// Pool of bloomd clients
type Pool struct {
	connPool         pool.Pool
	clientStructPool *sync.Pool
}

// NewPoolFromAddr return a new pool of client for addr
func NewPoolFromAddr(initialCap, maxCap int, addr string) (*Pool, error) {
	l, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	return NewPoolFromFactory(initialCap, maxCap, func() (net.Conn, error) {
		return createSocket(l)
	})
}

// NewPoolFromURL return a new pool of client for locator
func NewPoolFromURL(initialCap, maxCap int, u *url.URL) (*Pool, error) {
	return NewPoolFromFactory(initialCap, maxCap, func() (net.Conn, error) {
		return createSocket(u)
	})
}

// NewPoolFromFactory returns a new pool of clients for a connection factory
func NewPoolFromFactory(initialCap, maxCap int, factory Factory) (*Pool, error) {
	p, err := pool.NewChannelPool(initialCap, maxCap, pool.Factory(factory))
	if err != nil {
		return nil, err
	}

	clientStructPool := &sync.Pool{}
	clientStructPool.New = func() interface{} {
		cli := newClient()
		cli.clientPool = clientStructPool
		return cli
	}

	return &Pool{
		connPool:         p,
		clientStructPool: clientStructPool,
	}, nil
}

// Get returns a new client from the pool. Client is returned to pool by calling client.Close()
func (p *Pool) Get() (*Client, error) {
	conn, err := p.connPool.Get()
	if err != nil {
		return nil, err
	}

	cli := p.clientStructPool.Get().(*Client)
	cli.reset(conn)

	return cli, nil
}

// Close closes pool
func (p *Pool) Close() {
	p.connPool.Close()
}

// Len returns pool length
func (p *Pool) Len() int {
	return p.connPool.Len()
}
