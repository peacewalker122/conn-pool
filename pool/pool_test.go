package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/peacewalker122/conn-pool/pool"
)

func newPool(t *testing.T, max int) *pool.Pool[string] {
	t.Helper()
	names := []string{"satu", "dua", "tiga", "empat", "lima"}
	if max > len(names) {
		t.Fatalf("max %d > %d named connections", max, len(names))
	}
	i := 0
	p, err := pool.New(t.Context(), pool.Config[string]{
		Max: max,
		New: func(context.Context) (string, error) {
			n := names[i]
			i++
			return n, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func assertInvariants[T comparable](t *testing.T, p *pool.Pool[T], max int) {
	t.Helper()
	s := p.Snapshot()
	if err := pool.NoDoubleBorrow(s); err != nil {
		t.Fatal(err)
	}
	if err := pool.BelowMaxConnections(s, max); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireWhenIdleAndNoWaiters(t *testing.T) {
	p := newPool(t, 5)
	conn, err := p.Acquire(t.Context(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if conn == "" {
		t.Fatal("empty connection")
	}
	s := p.Snapshot()
	if len(s.Idle) != 4 || len(s.Borrowed) != 1 || len(s.Waiters) != 0 {
		t.Fatalf("got idle=%d borrowed=%d waiters=%d", len(s.Idle), len(s.Borrowed), len(s.Waiters))
	}
	if s.Borrowed["alice"] != conn {
		t.Fatalf("borrowed %+v", s.Borrowed)
	}
	assertInvariants(t, p, 5)
}

func TestCannotDoubleBorrowSameClient(t *testing.T) {
	p := newPool(t, 5)
	if _, err := p.Acquire(t.Context(), "alice"); err != nil {
		t.Fatal(err)
	}
	_, err := p.Acquire(t.Context(), "alice")
	if err != pool.ErrAlreadyBorrowed {
		t.Fatalf("got %v", err)
	}
}

func TestEnqueueAndFIFOGrant(t *testing.T) {
	p := newPool(t, 1)
	if _, err := p.Acquire(t.Context(), "alice"); err != nil {
		t.Fatal(err)
	}

	bobGot := make(chan string, 1)
	charlieGot := make(chan string, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		conn, err := p.Acquire(t.Context(), "bob")
		if err != nil {
			t.Errorf("bob: %v", err)
			return
		}
		bobGot <- conn
	})
	waitUntilWaiting(t, p, "bob")
	wg.Go(func() {
		conn, err := p.Acquire(t.Context(), "charlie")
		if err != nil {
			t.Errorf("charlie: %v", err)
			return
		}
		charlieGot <- conn
	})
	waitUntilWaiting(t, p, "charlie")

	s := p.Snapshot()
	if len(s.Waiters) != 2 || s.Waiters[0] != "bob" || s.Waiters[1] != "charlie" {
		t.Fatalf("waiters %v", s.Waiters)
	}

	if err := p.Release("alice"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bobGot:
	case <-time.After(time.Second):
		t.Fatal("bob was not granted first")
	}
	select {
	case <-charlieGot:
		t.Fatal("charlie granted before second release")
	default:
	}

	if err := p.Release("bob"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-charlieGot:
	case <-time.After(time.Second):
		t.Fatal("charlie was not granted")
	}
	wg.Wait()
	assertInvariants(t, p, 1)
}

func TestWaitersBlockDirectAcquire(t *testing.T) {
	p := newPool(t, 1)
	if _, err := p.Acquire(t.Context(), "alice"); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := p.Acquire(t.Context(), "bob"); err != nil {
			t.Errorf("bob: %v", err)
		}
	}()
	waitUntilWaiting(t, p, "bob")

	daveDone := make(chan struct{})
	go func() {
		defer close(daveDone)
		if _, err := p.Acquire(t.Context(), "dave"); err != nil {
			t.Errorf("dave: %v", err)
		}
	}()
	waitUntilWaiting(t, p, "dave")

	s := p.Snapshot()
	if s.Waiters[0] != "bob" {
		t.Fatalf("queue jumped: %v", s.Waiters)
	}

	if err := p.Release("alice"); err != nil {
		t.Fatal(err)
	}
	<-done
	s = p.Snapshot()
	if _, ok := s.Borrowed["bob"]; !ok {
		t.Fatalf("bob should hold the connection, borrowed=%v waiters=%v", s.Borrowed, s.Waiters)
	}
	if _, ok := s.Borrowed["dave"]; ok {
		t.Fatal("dave jumped bob")
	}

	if err := p.Release("bob"); err != nil {
		t.Fatal(err)
	}
	<-daveDone
}

func TestReleaseUnknownClient(t *testing.T) {
	p := newPool(t, 1)
	if err := p.Release("alice"); err != pool.ErrUnknownRelease {
		t.Fatalf("got %v", err)
	}
}

func TestCancelWhileWaiting(t *testing.T) {
	p := newPool(t, 1)
	if _, err := p.Acquire(t.Context(), "alice"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := p.Acquire(ctx, "bob")
		errCh <- err
	}()
	waitUntilWaiting(t, p, "bob")
	cancel()
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	s := p.Snapshot()
	if len(s.Waiters) != 0 {
		t.Fatalf("waiter leaked: %v", s.Waiters)
	}
}

func TestConcurrentNoDoubleBorrow(t *testing.T) {
	p := newPool(t, 5)
	clients := []pool.Client{"alice", "bob", "charlie", "dave", "erin", "frank"}
	var wg sync.WaitGroup
	for range 40 {
		for _, c := range clients {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				if _, err := p.Acquire(ctx, c); err != nil {
					return
				}
				assertInvariants(t, p, 5)
				_ = p.Release(c)
			})
		}
	}
	wg.Wait()
	assertInvariants(t, p, 5)
	s := p.Snapshot()
	if len(s.Idle) != 5 || len(s.Borrowed) != 0 || len(s.Waiters) != 0 {
		t.Fatalf("final idle=%d borrowed=%d waiters=%d", len(s.Idle), len(s.Borrowed), len(s.Waiters))
	}
}

func waitUntilWaiting(t *testing.T, p *pool.Pool[string], client pool.Client) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s := p.Snapshot()
		for _, w := range s.Waiters {
			if w == client {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s never entered waiters: %+v", client, p.Snapshot().Waiters)
}
