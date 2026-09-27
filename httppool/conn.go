package httppool

import (
	"bufio"
	"net"
)

type persistConn struct {
	raw net.Conn
	br  *bufio.Reader
	bw  *bufio.Writer
}

func newPersistConn(raw net.Conn) *persistConn {
	return &persistConn{
		raw: raw,
		br:  bufio.NewReader(raw),
		bw:  bufio.NewWriter(raw),
	}
}

func (c *persistConn) Close() error {
	return c.raw.Close()
}
