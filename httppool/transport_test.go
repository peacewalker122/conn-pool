package httppool_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/peacewalker122/conn-pool/httppool"
	"github.com/peacewalker122/conn-pool/pool"
)

func TestHTTPRoundTripReusesPool(t *testing.T) {
	var hits int
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	srv.Start()
	t.Cleanup(srv.Close)

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Listener.Addr().String())
	}
	p, err := httppool.New(t.Context(), 2, dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	client := &http.Client{Transport: p.Transport()}
	for _, path := range []string{"/satu", "/dua"} {
		req, err := http.NewRequestWithContext(
			httppool.WithClient(t.Context(), "alice"),
			http.MethodGet,
			srv.URL+path,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if string(body) != path {
			t.Fatalf("got %q", body)
		}
	}
	if hits != 2 {
		t.Fatalf("hits=%d", hits)
	}
	idle, borrowed, waiting := p.Stats()
	if borrowed != 0 {
		t.Fatalf("leaked borrows: %d", borrowed)
	}
	if idle+borrowed > 2 {
		t.Fatalf("idle+borrowed=%d", idle+borrowed)
	}
	if waiting != 0 {
		t.Fatalf("waiters=%d", waiting)
	}
}

func TestHTTPConcurrentClientsStayWithinMax(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Start()
	t.Cleanup(srv.Close)

	p, err := httppool.New(t.Context(), 2, func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Listener.Addr().String())
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	httpClient := &http.Client{Transport: p.Transport()}
	done := make(chan struct{}, 4)
	for _, c := range []pool.Client{"alice", "bob", "charlie", "dave"} {
		go func() {
			defer func() { done <- struct{}{} }()
			req, err := http.NewRequestWithContext(
				httppool.WithClient(t.Context(), c),
				http.MethodGet,
				srv.URL+"/",
				nil,
			)
			if err != nil {
				t.Error(err)
				return
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
	}
	for range 4 {
		<-done
	}
	idle, borrowed, waiting := p.Stats()
	if idle+borrowed > 2 {
		t.Fatalf("idle+borrowed=%d", idle+borrowed)
	}
	if borrowed != 0 || waiting != 0 {
		t.Fatalf("borrowed=%d waiters=%d", borrowed, waiting)
	}
}
