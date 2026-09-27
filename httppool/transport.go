package httppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/peacewalker122/conn-pool/pool"
)

type clientKey struct{}

func WithClient(ctx context.Context, client pool.Client) context.Context {
	return context.WithValue(ctx, clientKey{}, client)
}

func ClientFrom(ctx context.Context) (pool.Client, bool) {
	c, ok := ctx.Value(clientKey{}).(pool.Client)
	return c, ok
}

type Transport struct {
	pool *Pool
	seq  atomic.Uint64
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil {
		return nil, errors.New("httppool: nil URL")
	}
	if req.URL.Host == "" && req.Host == "" {
		return nil, errors.New("httppool: no Host")
	}

	client, ok := ClientFrom(req.Context())
	if !ok {
		client = pool.Client(fmt.Sprintf("req-%d", t.seq.Add(1)))
	}

	conn, err := t.pool.acquire(req.Context(), client)
	if err != nil {
		return nil, err
	}

	if err := req.Write(conn.bw); err != nil {
		_ = t.pool.release(client)
		return nil, err
	}
	if err := conn.bw.Flush(); err != nil {
		_ = t.pool.release(client)
		return nil, err
	}

	resp, err := http.ReadResponse(conn.br, req)
	if err != nil {
		_ = t.pool.release(client)
		return nil, err
	}

	resp.Body = &releasingBody{
		ReadCloser: resp.Body,
		release: func() {
			_ = t.pool.release(client)
		},
	}
	return resp, nil
}

type releasingBody struct {
	io.ReadCloser
	release  func()
	released atomic.Bool
}

func (b *releasingBody) Close() error {
	if !b.released.CompareAndSwap(false, true) {
		return nil
	}
	_, _ = io.Copy(io.Discard, b.ReadCloser)
	err := b.ReadCloser.Close()
	b.release()
	return err
}
