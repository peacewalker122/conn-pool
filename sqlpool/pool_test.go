package sqlpool_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/peacewalker122/conn-pool/pool"
	"github.com/peacewalker122/conn-pool/sqlpool"
)

var driverSeq atomic.Int64

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return &fakeConn{}, nil
}

type fakeConn struct{}

func (*fakeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*fakeConn) Close() error                        { return nil }
func (*fakeConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (*fakeConn) Ping(context.Context) error          { return nil }

func openFake(t *testing.T) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("connpool-fake-%d", driverSeq.Add(1))
	sql.Register(name, fakeDriver{})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLAcquirePingRelease(t *testing.T) {
	db := openFake(t)
	p, err := sqlpool.New(t.Context(), db, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	conn, err := p.Acquire(t.Context(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.Release("alice"); err != nil {
		t.Fatal(err)
	}

	s := p.Snapshot()
	if err := pool.BelowMaxConnections(s, 5); err != nil {
		t.Fatal(err)
	}
	if len(s.Idle) != 5 {
		t.Fatalf("idle=%d", len(s.Idle))
	}
}

func TestSQLPoolCapsOpenConnections(t *testing.T) {
	db := openFake(t)
	p, err := sqlpool.New(t.Context(), db, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if _, err := p.Acquire(t.Context(), "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire(t.Context(), "bob"); err != nil {
		t.Fatal(err)
	}
	s := p.Snapshot()
	if len(s.Idle) != 0 || len(s.Borrowed) != 2 {
		t.Fatalf("idle=%d borrowed=%d", len(s.Idle), len(s.Borrowed))
	}
	if err := pool.BelowMaxConnections(s, 2); err != nil {
		t.Fatal(err)
	}
}
