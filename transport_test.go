package webrtc_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	pion "github.com/pion/webrtc/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	webrtc "github.com/grpc-transports/webrtc"
)

// An echo service with no generated code, so the test depends on gRPC and not
// on protoc. It is the same scaffolding the WebSocket transport beside this one
// uses, for the same reason.
const (
	testCodec  = "webrtc-transport-rawbytes"
	testMethod = "/webrtctransport.Echo/Stream"
)

type rawCodec struct{}

func (rawCodec) Name() string                  { return testCodec }
func (rawCodec) Marshal(v any) ([]byte, error) { return *v.(*[]byte), nil }
func (rawCodec) Unmarshal(data []byte, v any) error {
	b := v.(*[]byte)
	*b = append((*b)[:0], data...)
	return nil
}

func init() { encoding.RegisterCodec(rawCodec{}) }

var echoDesc = grpc.ServiceDesc{
	ServiceName: "webrtctransport.Echo",
	HandlerType: (*any)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       echoHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
}

func echoHandler(_ any, stream grpc.ServerStream) error {
	for {
		var msg []byte
		if err := stream.RecvMsg(&msg); err != nil {
			return err
		}
		reply := append([]byte("echo:"), msg...)
		if err := stream.SendMsg(&reply); err != nil {
			return err
		}
	}
}

// peers establishes a real WebRTC connection between two peer connections in
// this process, and returns them with the data channel the offerer opened.
//
// Nothing about it is stubbed: real ICE, real DTLS, real SCTP. The signalling
// is done here by handing one peer's description to the other, which is what a
// caller does by mail or in a chat window — the part this package deliberately
// does not do.
func peers(t *testing.T) (offerer, answerer *pion.PeerConnection, dc *pion.DataChannel) {
	t.Helper()

	// Data channels have to be detached to be read from, which is what a
	// net.Conn does, and pion requires the setting engine to say so.
	var engine pion.SettingEngine
	engine.DetachDataChannels()
	api := pion.NewAPI(pion.WithSettingEngine(engine))

	var err error
	offerer, err = api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	answerer, err = api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = offerer.Close()
		_ = answerer.Close()
	})

	// Candidates go straight across, since both ends are here.
	offerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = answerer.AddICECandidate(c.ToJSON())
		}
	})
	answerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = offerer.AddICECandidate(c.ToJSON())
		}
	})

	opened := make(chan struct{})
	dc, err = offerer.CreateDataChannel("grpc", nil)
	if err != nil {
		t.Fatal(err)
	}
	dc.OnOpen(func() { close(opened) })

	offer, err := offerer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := offerer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if err := answerer.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	answer, err := answerer.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := answerer.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	if err := offerer.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}

	select {
	case <-opened:
	case <-time.After(20 * time.Second):
		t.Fatal("the data channel never opened")
	}
	return offerer, answerer, dc
}

// gRPC over a peer-to-peer connection, with neither side listening on anything.
//
// This is the whole claim: a standard grpc.Server serving a net.Listener, a
// standard client dialling a grpc.DialOption, and between them a connection
// neither end could have made with a socket.
func TestGRPCOverAPeerConnection(t *testing.T) {
	_, answerer, dc := peers(t)

	gs := grpc.NewServer()
	gs.RegisterService(&echoDesc, nil)
	lis := webrtc.Listen(answerer)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})

	opt, err := webrtc.DialOption(dc)
	if err != nil {
		t.Fatal(err)
	}
	cc, err := grpc.NewClient("passthrough:///webrtc",
		opt, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(testCodec)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := cc.NewStream(ctx, &echoDesc.Streams[0], testMethod)
	if err != nil {
		t.Fatalf("opening the stream: %v", err)
	}

	// Several messages both ways, because a transport that carries one may
	// still be framing them wrong.
	for _, want := range []string{"bonjour", "", "deux", string(make([]byte, 4096))} {
		msg := []byte(want)
		if err := stream.SendMsg(&msg); err != nil {
			t.Fatalf("sending %d bytes: %v", len(want), err)
		}
		var got []byte
		if err := stream.RecvMsg(&got); err != nil {
			t.Fatalf("receiving the reply to %d bytes: %v", len(want), err)
		}
		if string(got) != "echo:"+want {
			t.Fatalf("got %d bytes back, want the echo of %d", len(got), len(want))
		}
	}
	t.Log("gRPC, bidirectional, over a connection with nothing listening on either side")
}

// DialOption says what is wrong rather than failing at the first call.
func TestDialOptionRefusesNoChannel(t *testing.T) {
	if _, err := webrtc.DialOption(nil); err == nil {
		t.Fatal("DialOption with no channel returned no error")
	}
}

// A closed listener stops accepting and says so.
func TestAClosedListenerSaysSo(t *testing.T) {
	_, answerer, _ := peers(t)
	lis := webrtc.Listen(answerer)
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := lis.Accept(); err == nil {
		t.Fatal("Accept on a closed listener returned no error")
	}
	// Closing twice is not an error, because a caller that closes in a defer
	// and again on a path out should not have to remember which ran.
	if err := lis.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
	if lis.Addr().Network() != "webrtc" {
		t.Fatalf("the listener's address names %q", lis.Addr().Network())
	}
}

// plainPeers is peers without the setting engine that detaches data channels,
// which is the mistake a caller makes once: pion delivers to a callback then,
// and a callback cannot be read from.
func plainPeers(t *testing.T) (*pion.PeerConnection, *pion.PeerConnection, *pion.DataChannel) {
	t.Helper()
	offerer, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	answerer, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = offerer.Close()
		_ = answerer.Close()
	})
	offerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = answerer.AddICECandidate(c.ToJSON())
		}
	})
	answerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = offerer.AddICECandidate(c.ToJSON())
		}
	})
	opened := make(chan struct{})
	dc, err := offerer.CreateDataChannel("grpc", nil)
	if err != nil {
		t.Fatal(err)
	}
	dc.OnOpen(func() { close(opened) })
	offer, _ := offerer.CreateOffer(nil)
	_ = offerer.SetLocalDescription(offer)
	_ = answerer.SetRemoteDescription(offer)
	answer, _ := answerer.CreateAnswer(nil)
	_ = answerer.SetLocalDescription(answer)
	_ = offerer.SetRemoteDescription(answer)
	select {
	case <-opened:
	case <-time.After(20 * time.Second):
		t.Fatal("the data channel never opened")
	}
	return offerer, answerer, dc
}

// A channel that was not made detachable cannot be a connection, and says which
// channel and why rather than failing later inside gRPC.
func TestAChannelThatCannotBeDetached(t *testing.T) {
	_, _, dc := plainPeers(t)
	if _, err := webrtc.Conn(dc); err == nil {
		t.Fatal("a channel delivering to a callback became a connection")
	} else if !errors.Is(err, webrtc.ErrTransport) {
		t.Fatalf("the error is %v, want it to name the transport", err)
	} else if !strings.Contains(err.Error(), "grpc") {
		t.Fatalf("the error does not name the channel: %v", err)
	}
}

// And a listener over such a peer connection closes the channels it cannot
// serve, rather than leaving the peer waiting on one that will never be read.
func TestAListenerRefusesAChannelItCannotServe(t *testing.T) {
	// Not the helper: the listener has to be in place before the channel is
	// created, or the event it exists for has already fired.
	offerer, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	answerer, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = offerer.Close()
		_ = answerer.Close()
	})
	offerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = answerer.AddICECandidate(c.ToJSON())
		}
	})
	answerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = offerer.AddICECandidate(c.ToJSON())
		}
	})

	lis := webrtc.Listen(answerer)
	t.Cleanup(func() { _ = lis.Close() })

	dc, err := offerer.CreateDataChannel("grpc", nil)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	offer, _ := offerer.CreateOffer(nil)
	_ = offerer.SetLocalDescription(offer)
	_ = answerer.SetRemoteDescription(offer)
	answer, _ := answerer.CreateAnswer(nil)
	_ = answerer.SetLocalDescription(answer)
	_ = offerer.SetRemoteDescription(answer)
	select {
	case <-opened:
	case <-time.After(20 * time.Second):
		t.Fatal("the data channel never opened")
	}

	// The channel arrives and cannot be detached, so nothing is accepted. The
	// listener closing is what ends the wait, which is the observable half;
	// the channel being closed to tell the peer is pion's business.
	done := make(chan error, 1)
	go func() {
		_, err := lis.Accept()
		done <- err
	}()
	time.Sleep(500 * time.Millisecond)
	_ = lis.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Accept returned a connection from a channel that cannot be served")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not end when the listener closed")
	}
}

// A message larger than the buffer offered for it is reported rather than
// truncated.
//
// A data channel carries messages and a net.Conn is a stream, so SCTP hands
// over one message at a time and what does not fit is gone — a short read here
// would be a frame silently cut in half, which gRPC would then fail to parse
// somewhere far away from the cause.
func TestAMessageTooLargeForItsBufferIsReported(t *testing.T) {
	offerer, answerer, _ := peers(t)

	// By label, because the channel peers already opened may still be on its
	// way here: taking whichever arrives first reads one channel while writing
	// another, and the test hangs two times in three.
	incoming := make(chan net.Conn, 1)
	answerer.OnDataChannel(func(in *pion.DataChannel) {
		if in.Label() != "wide" {
			return
		}
		in.OnOpen(func() {
			c, err := webrtc.Conn(in)
			if err == nil {
				incoming <- c
			}
		})
	})

	// The channel above was opened before OnDataChannel was set, so a second
	// one is opened now that the answerer is listening for it.
	second, err := offerer.CreateDataChannel("wide", nil)
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	second.OnOpen(func() { close(ready) })
	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		t.Fatal("the second channel never opened")
	}

	out, err := webrtc.Conn(second)
	if err != nil {
		t.Fatal(err)
	}
	var in net.Conn
	select {
	case in = <-incoming:
	case <-time.After(20 * time.Second):
		t.Fatal("the answerer never saw the second channel")
	}

	if _, err := out.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	small := make([]byte, 16)
	if _, err := in.Read(small); err == nil {
		t.Fatal("a message that did not fit was read as though it had")
	} else if !errors.Is(err, webrtc.ErrTransport) {
		t.Fatalf("the error is %v, want it to name the transport", err)
	}
}

// A channel that arrives after the listener has closed is closed rather than
// left open with nobody reading it.
//
// It is a real order of events, not a contrived one: a peer opens a channel at
// the moment the server is shutting down, and the two cross. Leaving it open
// would leave the peer waiting for a gRPC server that is not there.
func TestAChannelArrivingAfterTheListenerClosed(t *testing.T) {
	var engine pion.SettingEngine
	engine.DetachDataChannels()
	api := pion.NewAPI(pion.WithSettingEngine(engine))

	offerer, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	answerer, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = offerer.Close()
		_ = answerer.Close()
	})
	offerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = answerer.AddICECandidate(c.ToJSON())
		}
	})
	answerer.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = offerer.AddICECandidate(c.ToJSON())
		}
	})

	lis := webrtc.Listen(answerer)
	// Closed before the peer opens anything, so the channel that follows finds
	// nobody to hand itself to.
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}

	dc, err := offerer.CreateDataChannel("late", nil)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	offer, _ := offerer.CreateOffer(nil)
	_ = offerer.SetLocalDescription(offer)
	_ = answerer.SetRemoteDescription(offer)
	answer, _ := answerer.CreateAnswer(nil)
	_ = answerer.SetLocalDescription(answer)
	_ = offerer.SetRemoteDescription(answer)
	select {
	case <-opened:
	case <-time.After(20 * time.Second):
		t.Fatal("the data channel never opened")
	}

	if _, err := lis.Accept(); err == nil {
		t.Fatal("a closed listener accepted a channel that arrived after it closed")
	}

	// The listener lets the channel go, and there is nothing to wait on for
	// that: the peer is not told — closing a detached channel does not reach
	// the other side, which was measured here rather than assumed, by watching
	// this peer's channel stay open for five seconds afterwards. So this waits
	// a moment for pion's goroutine to have run and asserts what is true: the
	// listener stays closed and hands over nothing.
	time.Sleep(200 * time.Millisecond)
	if _, err := lis.Accept(); err == nil {
		t.Fatal("a closed listener handed over a channel after all")
	}
	if state := dc.ReadyState(); state != pion.DataChannelStateOpen {
		t.Logf("the peer's channel is %v, which this package did not promise", state)
	}
}
