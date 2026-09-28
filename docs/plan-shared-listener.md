# Plan — shared listener (issue #2)

This file records the planning for the shared-listener mode. It uses
brainstorming, reverse brainstorming and six thinking hats. The acceptance
criteria (AC) at the end come from issue #2, with changes from this plan.

## 1. Goal

Serve HTTP and gRPC on one port:

```toml
[grpc]
listener = "shared"
```

The dedicated listener (ADR 0002) stays the default.

## 2. Brainstorming

- Merge the tonic routes into the Autumn router with `merge`.
- Put a tower layer on `AppBuilder::static_gate`. It sees each request
  before CSRF, session, timeout, body limit and error pages. It sends gRPC
  requests to the tonic routes.
- Detect gRPC by `content-type: application/grpc*`.
- Detect gRPC by path: a known `/<service>/<method>`.
- Turn on HTTP/2 in axum with a crate feature (`multiplex`).
- Add gRPC-Web on the same port.
- Keep health, reflection, metrics, guards and `AppState` the same as in
  the dedicated mode.
- Count in-flight calls in the gate, so the drain can wait for them.
- Put the peer address into `TcpConnectInfo`, so `request.remote_addr()`
  works.
- Apply `timeout_ms` and the `grpc-timeout` header in the gate.

## 3. Reverse brainstorming

How can this mode fail?

| Failure | Prevention |
|---|---|
| `merge` puts gRPC inside CSRF: every call in `prod` gets 403 | Use `static_gate`, not `merge` |
| The request timeout ends long streams | The gate is outside the timeout layer |
| A browser sends `application/grpc` over HTTP/1.1 and skips CSRF | Dispatch only HTTP/2 requests. gRPC needs HTTP/2 |
| `application/grpc-web` goes to tonic, which does not speak it | Do not dispatch `grpc-web` content types |
| No HTTP/2 in axum: clients get a protocol error that is hard to read | `listener = "shared"` without `multiplex` stops boot |
| Autumn TLS without ALPN `h2` (autumn-web 0.7): TLS clients fail | Shared mode with `[server.tls]` stops boot. The message names the Autumn fix |
| `[grpc.tls]` is set but has no effect | Config error |
| Two shared plugins: both serve `grpc.health.v1.Health` | A second shared plugin stops boot |
| Listener settings (`bind`, `max_connections`) look active but are not | One warning that names them |
| Calls before start or after stop reach no server | The gate answers `UNAVAILABLE` |
| A shutdown waits for a stream that never ends | Grace period, then the gate ends the response body with `UNAVAILABLE` |
| Autumn's HTTP drain waits for gRPC streams before the shutdown hook runs, so the hook never runs (found in review) | Start the drain on Autumn's shutdown signal, not in the hook |
| New calls during the drain | The gate answers `UNAVAILABLE` |
| `grpc-timeout` is ignored, because tonic's transport does not run | The gate applies it |

## 4. Six thinking hats

- **White (facts).** `static_gate` layers wrap the whole Autumn router.
  Only these are outside: `SecurityHeadersLayer`, the startup barrier,
  trace context, the access-log and server-timing fallbacks, and the
  `App::run` wrappers (method override, trusted proxies, connect info).
  autumn-web 0.7 `axum` has no `http2` feature. With it, `axum::serve`
  accepts h2c by prior knowledge. hyper keeps its defaults there (200
  streams, 1024 local resets). Autumn `main` adds ALPN `h2`
  (autumn-foundation/autumn#2321).
- **Red (feelings).** One port feels simple. Users expect the same
  features in both modes. A silent loss of CSRF on HTTP would feel unsafe.
- **Black (risks).** HTTP/2 turns on for the whole app (Cargo feature
  unification). Autumn's HTTP drain also waits for gRPC streams, so a long
  stream uses the shutdown budget. The plugin cannot set HTTP/2 limits on
  Autumn's server.
- **Yellow (benefits).** One port, one TLS setup (after the Autumn
  release), one ingress rule. Health, reflection and metrics stay the same.
- **Green (ideas).** gRPC-Web in this mode later (a separate issue). A
  `plugin-contract` gate for the next Autumn release.
- **Blue (process).** TDD. Tests in `tests/shared.rs` boot a `TestApp`,
  then serve its router with `axum::serve`, as `App::run` does. ADR 0008.
  Review agents. PR.

## 5. Acceptance criteria

| ID | Criterion |
|---|---|
| AC1 | `[grpc] listener = "dedicated" \| "shared"`. Default `dedicated`. An env override works. A bad value stops boot. |
| AC2 | Feature `multiplex` turns on `axum/http2`. `listener = "shared"` without it stops boot with a clear message. |
| AC3 | Shared mode binds no port. A gRPC call (h2c) to Autumn's port reaches the tonic routes. `GrpcHandle::local_addr()` is `None`. |
| AC4 | gRPC calls skip Autumn HTTP middleware: a call succeeds with CSRF on, and a stream outlives `server.timeouts.request_timeout_ms`. |
| AC5 | Other requests do not change: the same routes answer, and CSRF still applies. HTTP/1.1 requests with `application/grpc` stay on HTTP. `application/grpc-web` stays on HTTP. |
| AC6 | Guards, `AppState`, metrics, health, reflection, `timeout_ms` (and `grpc-timeout`), the duplicate-service check, the route listing and `remote_addr()` work as in dedicated mode. |
| AC7 | Lifecycle is `Serving` after start. Health follows Autumn readiness. When Autumn stops its listener, the drain starts: `Watch` streams see `NOT_SERVING` and end, new calls get `UNAVAILABLE`, in-flight calls finish within the grace, then the plugin ends them with `UNAVAILABLE`. Autumn's HTTP drain then ends. A call before start gets `UNAVAILABLE`. |
| AC8 | Shared mode with `[server.tls]` stops boot on autumn-web 0.7, and the message names the Autumn fix. `[grpc.tls]` in shared mode stops boot. A second shared plugin stops boot. |
| AC9 | Dedicated-listener settings that are not at their defaults log one warning in shared mode. |
| AC10 | README section, ADR 0008, `docs/verification.md` rows, CI runs the `multiplex` feature. Quality gates pass. |
