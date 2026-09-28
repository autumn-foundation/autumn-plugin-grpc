# ADR 0008 — Optional shared listener on Autumn's HTTP port

- Status: accepted
- Date: 2026-09-28
- Updates: [ADR 0002](0002-dedicated-listener.md)

## Context

Some teams want one port for HTTP and gRPC (one ingress rule, one TLS
setup). ADR 0002 names two blockers: Autumn 0.7 serves HTTP/1 only, and
Autumn's HTTP middleware (CSRF, timeouts, error pages) wraps every route.

- `AppBuilder::static_gate` puts a layer outside all of that middleware.
  Only the security headers, the startup barrier, trace context, the
  access-log and server-timing fallbacks and the `App::run` wrappers are
  outside it.
- With the axum `http2` feature, `axum::serve` also accepts HTTP/2 (h2c)
  by prior knowledge. hyper then keeps its own defaults (200 streams,
  1024 local resets).
- autumn-web 0.7 does not advertise ALPN `h2` on its TLS listener.
  autumn-foundation/autumn#2321 fixes this on `main`.

## Decision

Add `listener = "shared"`. The default stays `dedicated`.

- The crate feature `multiplex` turns on `axum/http2`. Without it, shared
  mode stops boot.
- The plugin registers a `static_gate` layer. The gate sends a request to
  the tonic routes when it is HTTP/2 and its content type is
  `application/grpc`, `application/grpc+*` or `application/grpc;*`. All
  other requests (HTTP/1.1, `application/grpc-web`) go to Autumn as before.
- The gate applies `timeout_ms` and the client's `grpc-timeout`, because
  tonic's transport does not run. It puts the peer address into
  `TcpConnectInfo`.
- The gate counts calls. Before `Serving` and after the drain starts, it
  answers `UNAVAILABLE`. After the grace period, it ends open response
  bodies with `UNAVAILABLE` trailers.
- The drain starts when Autumn stops its listener
  (`AppState::shutdown_token`, so `multiplex` turns on `autumn-web/ws`).
  Autumn waits for all HTTP/2 streams before it runs shutdown hooks, and
  its drain watchdog counts only HTTP requests. A drain that starts in the
  hook would never run while a stream (for example a health `Watch`) is
  open.
- Only one plugin in an app can use shared mode.
- Shared mode stops boot with `[server.tls]` on autumn-web 0.7, and with
  `[grpc.tls]`.

## Consequences

- HTTP/2 (h2c) turns on for the whole app, through Cargo feature
  unification.
- The plugin cannot set connection or HTTP/2 limits. Autumn's server owns
  them. The plugin logs one warning for such settings.
- Autumn's HTTP drain waits for gRPC streams too, inside
  `server.shutdown_timeout_secs`. The plugin grace fits inside it.
- hyper polls a body only while the client has flow-control window. A
  client that stops reading keeps its stream until Autumn ends the
  process.
- Metrics count a call that the grace ends before its response head as
  `CANCELLED`. The gate's own `UNAVAILABLE` answers are not counted.
- Health, reflection, metrics, guards and `AppState` work as in the
  dedicated mode. `GrpcHandle::local_addr()` is `None`.
- When Autumn releases ALPN `h2`, the TLS check can accept Autumn TLS.
