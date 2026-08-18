// Package webrtc carries gRPC between two processes that cannot reach each
// other.
//
// # What it is for
//
// gRPC assumes one side can listen and the other can dial. Two peers behind
// household routers can do neither: neither has an address the other can
// connect to, and there is nothing in between that either controls. WebRTC is
// how the web solved that — each side describes itself, the two swap those
// descriptions by any means at all, and what results is a direct connection.
//
// This presents that connection to gRPC as what gRPC expects: a net.Conn on one
// side and a net.Listener on the other. Everything above it is ordinary gRPC —
// unary calls, streaming, interceptors, deadlines — because nothing above it
// knows what it is running on.
//
// # What it does not do
//
// It does not do the signalling. Two peers have to swap a session description
// before there is anything to carry gRPC over, and how they do that is not a
// transport's business: a mail, a chat window, a QR code, a rendezvous service
// somebody already runs. A library that chose one would be choosing for every
// caller, and the choice is usually already made by whatever the two peers are
// doing together.
//
// So this takes a peer connection that is already established. What that costs
// a caller is twenty lines of pion; what it buys is that this package has no
// opinion about how two people find each other.
package webrtc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pion/datachannel"
	pion "github.com/pion/webrtc/v4"
)

// ErrTransport reports a data channel that could not be turned into a
// connection.
var ErrTransport = errors.New("webrtc: transport")

// addr names one end of a data-channel connection. gRPC prints it in errors and
// logs, and it has to satisfy net.Addr; there is nothing meaningful to put in
// it, since neither peer has an address the other could dial.
type addr struct{ label string }

func (addr) Network() string  { return "webrtc" }
func (a addr) String() string { return "webrtc:" + a.label }

// Conn presents an established data channel as a net.Conn, which is what gRPC
// wants of a transport and all it wants.
//
// The channel is detached from pion's callback delivery first: a gRPC transport
// reads, and a channel that hands its data to a callback cannot be read from.
// It must therefore have been created on a peer connection configured with
// SettingEngine.DetachDataChannels, which is what pion requires and what the
// examples in this package do.
func Conn(dc *pion.DataChannel) (net.Conn, error) {
	rw, err := dc.Detach()
	if err != nil {
		return nil, fmt.Errorf("%w: detaching %q: %w", ErrTransport, dc.Label(), err)
	}
	return &dataConn{rw: rw, dc: dc, addr: addr{label: dc.Label()}}, nil
}

// dataConn is one data channel, as a net.Conn.
type dataConn struct {
	rw   datachannel.ReadWriteCloser
	dc   *pion.DataChannel
	addr addr

	closeOnce sync.Once
	closeErr  error
}

// Read fills b with the next message, or part of one.
//
// A data channel is message-oriented and a net.Conn is a stream, so a short
// buffer would truncate rather than resume: SCTP hands over one message at a
// time, and what does not fit is gone. gRPC reads into buffers of its own
// choosing, and this reports the mismatch rather than corrupting a frame.
func (c *dataConn) Read(b []byte) (int, error) {
	n, err := c.rw.Read(b)
	if errors.Is(err, io.ErrShortBuffer) {
		return n, fmt.Errorf("%w: a message did not fit in %d bytes", ErrTransport, len(b))
	}
	return n, err
}

func (c *dataConn) Write(b []byte) (int, error) { return c.rw.Write(b) }

func (c *dataConn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.rw.Close() })
	return c.closeErr
}

func (c *dataConn) LocalAddr() net.Addr  { return c.addr }
func (c *dataConn) RemoteAddr() net.Addr { return c.addr }

// SetDeadline and its two halves report that there are none.
//
// A net.Conn is allowed to have no deadlines — os.ErrNoDeadline says so — and
// gRPC does not need them: its own deadlines are per call and enforced above
// this, and a data channel that goes away tells the reader by ending. Returning
// an error rather than pretending to succeed is what keeps a caller that does
// need them from waiting for a timeout that will not come.
func (c *dataConn) SetDeadline(time.Time) error      { return os.ErrNoDeadline }
func (c *dataConn) SetReadDeadline(time.Time) error  { return os.ErrNoDeadline }
func (c *dataConn) SetWriteDeadline(time.Time) error { return os.ErrNoDeadline }
