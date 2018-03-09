package bloomd

import (
	"net"
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
	return NewPoolFromFactory(initialCap, maxCap, func() (net.Conn, error) {
		return net.Dial("tcp", addr)
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
		return &Client{
			clientPool: clientStructPool,
		}
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

// Close close pool
func (p *Pool) Close() {
	p.connPool.Close()
}

// Len return pool length
func (p *Pool) Len() int {
	return p.connPool.Len()
}
