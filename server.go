package webrtc

import (
	"errors"
	"net"
	"sync"

	pion "github.com/pion/webrtc/v4"
)

// ErrClosed is returned by a listener that has been closed.
var ErrClosed = errors.New("webrtc: listener closed")

// Listen presents a peer connection as a net.Listener, so that a standard
// grpc.Server can Serve it.
//
// Every data channel the peer opens becomes one connection. That is more than
// gRPC needs — it multiplexes its own streams over one — but it costs nothing
// to allow and it is what makes a second gRPC server, or a second client,
// possible over the same peer connection without a second negotiation.
//
// The peer connection must have been created with a setting engine that
// detaches data channels; a channel delivering to a callback cannot be read
// from, and pion says so at detach time rather than here.
//
// Closing the listener stops accepting. It does not close the peer connection,
// which the caller made and may still be using — for a video call, for
// instance, which is often exactly what two peers doing this are already on.
func Listen(pc *pion.PeerConnection) net.Listener {
	l := &listener{
		pc:     pc,
		accept: make(chan net.Conn),
		done:   make(chan struct{}),
	}
	pc.OnDataChannel(func(dc *pion.DataChannel) {
		// Detaching is only allowed once the channel is open, and OnOpen is
		// where pion says it is.
		dc.OnOpen(func() {
			conn, err := Conn(dc)
			if err != nil {
				// A channel that cannot be detached is one this listener
				// cannot serve. Closing it tells the peer, which is more use
				// than an error nobody is waiting for.
				_ = dc.Close()
				return
			}
			select {
			case l.accept <- conn:
			case <-l.done:
				// The listener closed while this channel was arriving, which
				// is a real order of events rather than a contrived one: a
				// peer opens a channel as the server shuts down and the two
				// cross. What can be done is to let go of it here.
				//
				// What cannot: the peer is not told. Closing a detached
				// channel does not reach the other side — measured, not
				// assumed, by watching a peer's channel stay open for five
				// seconds after both the connection and the channel were
				// closed here. That is pion's behaviour and not this
				// package's to fix, and it matters to a caller: a peer whose
				// channel is refused learns it from its own gRPC call failing,
				// not from the channel.
				_ = conn.Close()
				_ = dc.Close()
			}
		})
	})
	return l
}

type listener struct {
	pc     *pion.PeerConnection
	accept chan net.Conn
	done   chan struct{}
	once   sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.done:
		return nil, ErrClosed
	}
}

func (l *listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *listener) Addr() net.Addr { return addr{label: "listener"} }
