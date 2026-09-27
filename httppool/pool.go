// Package httppool reuses HTTP/1.1 TCP connections through the TLA pool.
package httppool

import (
	"context"
	"net"
	"net/http"

	"github.com/peacewalker122/conn-pool/pool"
)

type DialFunc func(ctx context.Context) (net.Conn, error)

type Pool struct {
	inner *pool.Pool[*persistConn]
}

func New(ctx context.Context, max int, dial DialFunc) (*Pool, error) {
	inner, err := pool.New(ctx, pool.Config[*persistConn]{
		Max: max,
		New: func(ctx context.Context) (*persistConn, error) {
			raw, err := dial(ctx)
			if err != nil {
				return nil, err
			}
			return newPersistConn(raw), nil
		},
		Close: func(c *persistConn) error {
			return c.Close()
		},
	})
	if err != nil {
		return nil, err
	}
	return &Pool{inner: inner}, nil
}

func (p *Pool) acquire(ctx context.Context, client pool.Client) (*persistConn, error) {
	return p.inner.Acquire(ctx, client)
}

func (p *Pool) release(client pool.Client) error {
	return p.inner.Release(client)
}

func (p *Pool) Close() error {
	return p.inner.Close()
}

func (p *Pool) Stats() (idle, borrowed, waiting int) {
	return p.inner.Stats()
}

func (p *Pool) Transport() http.RoundTripper {
	return &Transport{pool: p}
}
