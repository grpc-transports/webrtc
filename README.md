<p align="center"><img src="https://raw.githubusercontent.com/grpc-transports/brand/main/social/grpc-transports.png" alt="grpc-transports/webrtc" width="720"></p>

# webrtc

[![Go Reference](https://pkg.go.dev/badge/github.com/grpc-transports/webrtc.svg)](https://pkg.go.dev/github.com/grpc-transports/webrtc)
[![CI](https://github.com/grpc-transports/webrtc/actions/workflows/ci.yml/badge.svg)](https://github.com/grpc-transports/webrtc/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

WebRTC transport layer for gRPC — the carrier for two processes that **cannot reach each other**. The server exposes a `net.Listener` over an `RTCPeerConnection`; the client provides a `grpc.DialOption` over a data channel. Everything above it is ordinary gRPC — unary, streaming, interceptors, deadlines — because nothing above it knows what it is running on.

## Why

gRPC assumes one side can listen and the other can dial. Two peers behind household routers can do neither: neither has an address the other can connect to, and there is nothing in between that either controls. WebRTC is how the web solved that — each side describes itself, the two swap those descriptions by any means at all, and what results is a direct connection.

| | |
|---|---|
| **Encryption** | DTLS, end to end, not optional — WebRTC has no unencrypted mode |
| **Relay** | none; the connection is peer to peer |
| **Sidecar** | none |
| **Dependencies** | [pion/webrtc](https://github.com/pion/webrtc) and gRPC |

## What it does not do

**Signalling.** Two peers have to swap a session description before there is anything to carry gRPC over, and how they do that is not a transport's business: a mail, a chat window, a QR code, a rendezvous service somebody already runs. A library that chose one would be choosing for every caller — and the choice is usually already made by whatever the two peers are doing together.

So this takes a peer connection that is already established. What that costs a caller is twenty lines of pion; what it buys is that this package has no opinion about how two people find each other.

## Use

Both sides need data channels detached — a channel that delivers to a callback cannot be read from, and a `net.Conn` is read from:

```go
var engine webrtc.SettingEngine
engine.DetachDataChannels()
api := webrtc.NewAPI(webrtc.WithSettingEngine(engine))
```

The side that serves:

```go
lis := grpcwebrtc.Listen(peerConnection)
gs := grpc.NewServer()
pb.RegisterYourServiceServer(gs, impl)
go gs.Serve(lis)
```

The side that calls:

```go
opt, err := grpcwebrtc.DialOption(dataChannel)
cc, err := grpc.NewClient("passthrough:///webrtc",
    opt, grpc.WithTransportCredentials(insecure.NewCredentials()))
```

Insecure credentials are not a weakening here: WebRTC is encrypted end to end by DTLS and has no unencrypted mode, so a second layer of TLS inside it would be encrypting what is already encrypted, against an attacker who is not there. It is the same reasoning as `wss://` in [the WebSocket transport](https://github.com/grpc-transports/websocket) beside this one.

The address passed to `grpc.NewClient` is ignored, because there is no address: the connection already exists and there is nothing to resolve.

## Two things a caller should know

**A message that does not fit its buffer is an error, not a short read.** A data channel carries messages and a `net.Conn` is a stream, so SCTP hands over one message at a time and what does not fit is gone. This reports the mismatch rather than handing back half a frame that gRPC would fail to parse somewhere far from the cause.

**A refused channel does not tell its peer.** When a listener has closed and a channel arrives anyway, it is let go of here — but closing a detached channel does not reach the other side. That was measured rather than assumed, and it means a peer whose channel is refused learns it from its own call failing rather than from the channel.

## Sibling

[grpc-transports/websocket](https://github.com/grpc-transports/websocket) — the carrier that works inside the browser, including under `GOOS=js`.

## License

BSD-3-Clause.
