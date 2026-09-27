// Package sqlpool holds database/sql connections in a TLA-faithful pool.
package sqlpool

import (
	"context"
	"database/sql"

	"github.com/peacewalker122/conn-pool/pool"
)

type Pool struct {
	inner *pool.Pool[*sql.Conn]
}

func New(ctx context.Context, db *sql.DB, max int) (*Pool, error) {
	db.SetMaxOpenConns(max)
	db.SetMaxIdleConns(0)

	inner, err := pool.New(ctx, pool.Config[*sql.Conn]{
		Max: max,
		New: func(ctx context.Context) (*sql.Conn, error) {
			return db.Conn(ctx)
		},
		Close: func(c *sql.Conn) error {
			return c.Close()
		},
	})
	if err != nil {
		return nil, err
	}
	return &Pool{inner: inner}, nil
}

func (p *Pool) Acquire(ctx context.Context, client pool.Client) (*sql.Conn, error) {
	return p.inner.Acquire(ctx, client)
}

func (p *Pool) Release(client pool.Client) error {
	return p.inner.Release(client)
}

func (p *Pool) Close() error {
	return p.inner.Close()
}

func (p *Pool) Snapshot() pool.Snapshot[*sql.Conn] {
	return p.inner.Snapshot()
}
