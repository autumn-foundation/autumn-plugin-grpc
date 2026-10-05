# ADR 0009 — gRPC client extractor

- Status: accepted
- Date: 2026-10-05
- Issue: #3. Plan: [plan-client.md](../plan-client.md)

## Context

Autumn handlers call downstream gRPC services. Each app wrote its own
channel setup, timeouts and error mapping. The handler sees the incoming
request, so it can send the request context downstream.

Facts that shape the design:

- Generated clients take any `GrpcService<tonic::body::Body>`.
- tonic's `Channel` reports its own timeout as `CANCELLED`. A tonic server
  also ends a call that runs out of time with `CANCELLED`.
- Autumn has a blanket `From<E: Error>` for `AutumnError` (status 500).
  The orphan rule blocks `From<tonic::Status>` in this crate.
- Autumn puts `RequestId` into the request extensions. It does not
  publish the request deadline or the current OpenTelemetry span.
- Autumn shows 5xx details only in `dev`. It shows 4xx details in all
  profiles.

## Decision

- Crate feature `client` (`tonic/channel`). The server does not compile
  or link the client code.
- `[grpc.clients.<name>]` in the plugin section: `endpoint`, `timeout_ms`
  (default 10 s), `connect_timeout_ms` (default 5 s), `tls.*`. The same
  layers as `[grpc]`. Env leaves come from the names in the files and in
  code, because a map has no leaves in the defaults.
- `GrpcPlugin::client(name, build)`. `build` is a closure, because
  generated clients share no constructor trait. One name can have more
  than one client type (several services on one endpoint).
- `GrpcChannel` wraps a lazy tonic `Channel`. For each call it adds
  `x-request-id`, a valid `traceparent` and `tracestate`, sets
  `grpc-timeout`, enforces it, and records metrics. A caller value stays.
  `grpc-timeout` is the smallest of the client timeout, the time left on
  the incoming request, and the caller's timeout. No time left: the call
  ends with `DEADLINE_EXCEEDED` and is not sent.
- A `static_gate` layer records the request start, before Autumn's
  timeout layer. The time left is start + `server.timeouts.request_timeout_ms`
  − now.
- A tonic timeout, or a trailers-only `CANCELLED` after the deadline, is
  `DEADLINE_EXCEEDED` (504), not `CANCELLED` (502).
- `GrpcClient<T>` gives the only client of type `T`. `GrpcClients` (in
  `AppState`, and an extractor) gives a client by name. A lookup failure
  is a 500. A config entry with no registration, and one type on two
  names, log a warning at boot.
- Errors: `.or_http()` and `status_to_error` use a fixed map (see the
  README). 4xx responses carry a fixed text. 5xx responses carry the code
  and message; Autumn hides them outside `dev`. The log has the full
  status.
- Boot errors: a registered client without an endpoint or a double, a
  duplicate name, a bad endpoint, `https` without the `tls` feature or
  without `tls.ca_path`, and a TLS file that cannot be read.
- No health indicator: a down endpoint does not change readiness.
- `grpc_client_*` metrics with a `client` label, as ADR 0004. Paths come
  from code, so no "known method" rule. A bad name is `unknown`, and the
  `max_metric_series` cap applies to each client.
- `client_double(name, service)` serves a tonic service over
  `tokio::io::duplex`. The double uses the same `GrpcChannel`, so tests
  see propagation, timeouts and metrics.

## Rejected

- `?` on `Status` with the code map. Not possible: the blanket `From`
  and the orphan rule. An exception filter sees only the message text.
  Parsing it is fragile.
- A `Channel` in `AppState` only. It cannot see the request.
- Native or webpki roots. They add dependencies. `tls.ca_path` with a
  system bundle works.

## Consequences

- Handlers write `.or_http()?`, not `?`. A bare `?` gives a 500.
- Autumn does not publish per-route `timeout` values. A route with
  `timeout = "off"` still gets the global deadline. A route with a longer
  override gets the shorter global one.
- The client forwards the incoming `traceparent`. With OpenTelemetry, the
  downstream span is a sibling of Autumn's server span, not a child.
- The deadline covers the response head. A stream body after the head
  has no client-side limit (as in tonic).
