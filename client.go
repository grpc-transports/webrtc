package webrtc

import (
	"context"
	"errors"
	"net"

	pion "github.com/pion/webrtc/v4"
	"google.golang.org/grpc"
)

// errNoChannel is returned when DialOption is given nothing to dial over.
var errNoChannel = errors.New("webrtc: DialOption needs a data channel")

// DialOption returns a grpc.DialOption that carries every gRPC channel over an
// established data channel.
//
// Combine it with insecure transport credentials. That is not a weakening:
// WebRTC is encrypted end to end by DTLS and cannot be otherwise — there is no
// unencrypted mode to fall back to — so a second layer of TLS inside it would
// be encrypting what is already encrypted, against an attacker who is not
// there. It is the same reasoning as wss:// in the WebSocket transport beside
// this one.
//
// The address a caller passes to grpc.NewClient is ignored, because there is no
// address: the connection already exists and there is nothing to resolve. Pass
// "passthrough:///webrtc" so that gRPC does not try.
func DialOption(dc *pion.DataChannel) (grpc.DialOption, error) {
	if dc == nil {
		return nil, errNoChannel
	}
	// The context is not consulted, and it is worth saying why rather than
	// checking it out of habit. Dialling here is detaching a channel that is
	// already open: it does not wait for anything, so there is nothing a
	// deadline or a cancellation could interrupt. A check would be a branch no
	// test could reach — and one was written, and could not be.
	//
	// The connection is made once and handed over once. gRPC reconnects by
	// dialling again, and there is nothing to dial: a peer connection that has
	// gone is re-established by whatever did the signalling, not here.
	return grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return Conn(dc)
	}), nil
}
