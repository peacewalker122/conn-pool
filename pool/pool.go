// Package pool implements the connection_pool TLA+ spec.
//
// State is (idle, borrowed, waiters). Acquire is allowed only when the
// waiter queue is empty. Otherwise a client Enqueues, and Grant gives the
// head waiter the next released connection (eager Grant, matching WF_Grant).
package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Client string

var (
	ErrClosed           = errors.New("pool: closed")
	ErrAlreadyBorrowed  = errors.New("pool: client already holds a connection")
	ErrAlreadyWaiting   = errors.New("pool: client is already waiting")
	ErrUnknownRelease   = errors.New("pool: client does not hold a connection")
	ErrInvalidMax       = errors.New("pool: max connections must be at least 1")
	ErrNilNew           = errors.New("pool: New is required")
)

type Config[T any] struct {
	Max   int
	New   func(ctx context.Context) (T, error)
	Close func(T) error
}

type waiter[T any] struct {
	client Client
	ch     chan result[T]
}

type result[T any] struct {
	val T
	err error
}

type Pool[T any] struct {
	closeSlot func(T) error

	mu       sync.Mutex
	closed   bool
	idle     []T
	borrowed map[Client]T
	waiters  []waiter[T]
}

func New[T any](ctx context.Context, cfg Config[T]) (*Pool[T], error) {
	if cfg.Max < 1 {
		return nil, ErrInvalidMax
	}
	if cfg.New == nil {
		return nil, ErrNilNew
	}
	p := &Pool[T]{
		closeSlot: cfg.Close,
		idle:      make([]T, 0, cfg.Max),
		borrowed:  make(map[Client]T, cfg.Max),
	}
	for range cfg.Max {
		conn, err := cfg.New(ctx)
		if err != nil {
			p.shutdownIdle()
			return nil, err
		}
		p.idle = append(p.idle, conn)
	}
	return p, nil
}

func (p *Pool[T]) Acquire(ctx context.Context, client Client) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return zero, ErrClosed
	}
	if _, ok := p.borrowed[client]; ok {
		p.mu.Unlock()
		return zero, ErrAlreadyBorrowed
	}
	if p.isWaiting(client) {
		p.mu.Unlock()
		return zero, ErrAlreadyWaiting
	}

	if len(p.waiters) == 0 && len(p.idle) > 0 {
		conn := p.popIdle()
		p.borrowed[client] = conn
		p.mu.Unlock()
		return conn, nil
	}

	if len(p.idle) > 0 {
		p.grantAllLocked()
		if _, ok := p.borrowed[client]; ok {
			p.mu.Unlock()
			return zero, ErrAlreadyBorrowed
		}
		if len(p.waiters) == 0 {
			conn := p.popIdle()
			p.borrowed[client] = conn
			p.mu.Unlock()
			return conn, nil
		}
	}

	w := waiter[T]{
		client: client,
		ch:     make(chan result[T], 1),
	}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	select {
	case r := <-w.ch:
		return r.val, r.err
	case <-ctx.Done():
		p.cancelWaiter(client)
		select {
		case r := <-w.ch:
			if r.err == nil {
				_ = p.Release(client)
			}
			return zero, ctx.Err()
		default:
			return zero, ctx.Err()
		}
	}
}

func (p *Pool[T]) Release(client Client) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, ok := p.borrowed[client]
	if !ok {
		return ErrUnknownRelease
	}
	delete(p.borrowed, client)

	if p.closed {
		p.closeVal(conn)
		p.failWaitersLocked()
		return ErrClosed
	}

	p.idle = append(p.idle, conn)
	p.grantAllLocked()
	return nil
}

func (p *Pool[T]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	p.closed = true
	p.failWaitersLocked()
	p.shutdownIdle()
	return nil
}

type Snapshot[T any] struct {
	Idle     []T
	Borrowed map[Client]T
	Waiters  []Client
}

func (p *Pool[T]) Stats() (idle, borrowed, waiting int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle), len(p.borrowed), len(p.waiters)
}

func (p *Pool[T]) Snapshot() Snapshot[T] {
	p.mu.Lock()
	defer p.mu.Unlock()

	idle := make([]T, len(p.idle))
	copy(idle, p.idle)

	borrowed := make(map[Client]T, len(p.borrowed))
	for c, conn := range p.borrowed {
		borrowed[c] = conn
	}

	waiters := make([]Client, len(p.waiters))
	for i, w := range p.waiters {
		waiters[i] = w.client
	}
	return Snapshot[T]{Idle: idle, Borrowed: borrowed, Waiters: waiters}
}

func NoDoubleBorrow[T comparable](s Snapshot[T]) error {
	seen := make(map[T]Client, len(s.Borrowed))
	for client, conn := range s.Borrowed {
		if other, ok := seen[conn]; ok {
			return fmt.Errorf("pool: connection borrowed by %q and %q", other, client)
		}
		seen[conn] = client
	}
	return nil
}

func BelowMaxConnections[T any](s Snapshot[T], max int) error {
	n := len(s.Idle) + len(s.Borrowed)
	if n > max {
		return fmt.Errorf("pool: idle+borrowed=%d exceeds max=%d", n, max)
	}
	return nil
}

func (p *Pool[T]) popIdle() T {
	i := len(p.idle) - 1
	conn := p.idle[i]
	p.idle = p.idle[:i]
	return conn
}

func (p *Pool[T]) isWaiting(client Client) bool {
	for _, w := range p.waiters {
		if w.client == client {
			return true
		}
	}
	return false
}

func (p *Pool[T]) grantAllLocked() {
	for len(p.waiters) > 0 && len(p.idle) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		conn := p.popIdle()
		if p.closed {
			p.closeVal(conn)
			w.ch <- result[T]{err: ErrClosed}
			continue
		}
		p.borrowed[w.client] = conn
		w.ch <- result[T]{val: conn}
	}
}

func (p *Pool[T]) cancelWaiter(client Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.waiters {
		if w.client == client {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

func (p *Pool[T]) failWaitersLocked() {
	for _, w := range p.waiters {
		w.ch <- result[T]{err: ErrClosed}
	}
	p.waiters = nil
}

func (p *Pool[T]) shutdownIdle() {
	for _, conn := range p.idle {
		p.closeVal(conn)
	}
	p.idle = nil
}

func (p *Pool[T]) closeVal(conn T) {
	if p.closeSlot != nil {
		_ = p.closeSlot(conn)
	}
}
