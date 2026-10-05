# Plan — gRPC client extractor (issue #3)

This file records the planning for the gRPC client. It uses
brainstorming, reverse brainstorming and six thinking hats. The acceptance
criteria (AC) at the end come from issue #3, with changes from this plan.

## 1. Goal

An Autumn handler calls a downstream gRPC service with one extractor:

```rust
#[get("/invoice/{id}")]
async fn invoice(
    Path(id): Path<String>,
    GrpcClient(mut billing): GrpcClient<BillingClient<GrpcChannel>>,
) -> AutumnResult<Json<Invoice>> {
    let reply = billing.get_invoice(GetInvoice { id }).await.or_http()?;
    Ok(Json(reply.into_inner().into()))
}
```

Registration: `GrpcPlugin::new().client("billing", BillingClient::new)`.

## 2. Brainstorming

- A crate feature `client`. It turns on `tonic/channel`. Without it, no
  client code compiles.
- `[grpc.clients.<name>]`: `endpoint`, `timeout_ms`,
  `connect_timeout_ms`, `[grpc.clients.<name>.tls]`. The same file,
  profile and env layers as `[grpc]`.
- One channel for each name. `Endpoint::connect_lazy`, so boot does not
  wait for the downstream service.
- A tower service, `GrpcChannel`, wraps the tonic `Channel`. It adds the
  request metadata, sets `grpc-timeout`, applies the deadline and records
  metrics. Generated clients accept any tower service, so
  `BillingClient<GrpcChannel>` works.
- The extractor reads the request: `RequestId`, `traceparent` and
  `tracestate`. The time left starts at extraction. (A `static_gate` to
  record the request start was the first idea. Review found that it
  makes Autumn's idempotency replay fail closed for the whole app.)
- `GrpcClients` is in `AppState`, and it is also an extractor. The
  extractor form carries the request context, so
  `clients.get::<T>("billing_eu")` propagates too.
- A fixed map from `tonic::Code` to an HTTP status. A trait method,
  `.or_http()`, converts `Result<T, Status>`.
- `grpc_client_*` metrics with a `client` label. The label values come
  from code (registered names and generated paths), and a cap applies.
- A test double: `.client_double(name, service)` serves a tonic service
  in the process, over `tokio::io::duplex`. No port.
- gRPC-Web, retries, load balancing, service discovery (out of scope).

## 3. Reverse brainstorming

How can this feature fail?

| Failure | Prevention |
|---|---|
| A bare `?` on `Status` gives 500. Autumn has a blanket `From<E: Error>` for `AutumnError`, and the orphan rule blocks `From<Status>` | Give `.or_http()` and `status_to_error`. The README and ADR 0009 say why |
| A downstream 4xx message shows internal details to the browser in prod | 4xx: a fixed text for each code. 5xx: the code and message; Autumn hides 5xx details in prod. The full status goes to the log |
| tonic maps a client timeout to `CANCELLED` (502) | The channel maps a timeout to `DEADLINE_EXCEEDED` (504) |
| The client waits after the incoming request has ended | `grpc-timeout` is the smaller of the client timeout and the time left. No time left: `DEADLINE_EXCEEDED` with no call |
| A down endpoint stops boot or makes the app not ready | Lazy connect. No health indicator for clients |
| A bad endpoint fails only at the first call | Validate the URI at boot. A boot error |
| A missing CA file fails at the first call | Read TLS files at boot. A boot error |
| `https` with no `tls` feature silently uses plain text | Boot error |
| `[grpc.clients.x]` with no `.client("x", ..)` looks active | A warning at boot |
| A handler asks for a type that is not registered | A 500 with a clear message at request time |
| Two names have one type: the extractor cannot choose | A 500 that names `GrpcClients::get`, and a warning at boot |
| Env overrides need leaf keys, but a map has none | Leaves come from the names in the files and in code |
| A client sends any `traceparent` to the downstream service | Forward only a valid W3C `traceparent`, and `tracestate` of 512 bytes at most |
| User metadata is overwritten | The channel does not replace a value that the caller set. For `grpc-timeout`, it takes the smaller value |
| Client metric labels grow without a bound | Names come from code. Bad method names are `unknown`. `max_metric_series` caps label sets for each client |
| Two plugins register the same client name | Boot error |
| `enabled = false` turns off the clients too | Clients do not depend on the server. A test proves it |
| The in-memory double leaks a server task for each test | The server stops when the channel (and its connector) drops |
| A plugin layer changes app behavior (found in review: idempotency replay) | No app layer. The time left starts at extraction |
| `request_timeout_ms = 0` (Autumn: off) gives a zero deadline (found in review) | Treat `0` as no limit |
| `HTTPS://` skips the TLS checks (found in review) | Accept only lowercase schemes |
| A caller `traceparent` gets the incoming `tracestate` (found in review) | Send the trace pair only when the caller set neither |

## 4. Six thinking hats

- **White (facts).** Generated clients take any
  `GrpcService<tonic::body::Body>`. tonic's `Channel` enforces
  `grpc-timeout`, but it reports a timeout as `CANCELLED`. Autumn puts
  `RequestId` in the request extensions. Autumn reads `traceparent` only
  with its `telemetry-otlp` feature, and it has no public API for the
  current span. Autumn does not publish the request deadline; only the
  global `server.timeouts.request_timeout_ms` is visible. Per-route
  `timeout` values are private. Autumn's problem details hide 5xx
  details outside `dev`.
- **Red (feelings).** Users expect `?` to work. A name for each client
  feels natural. A mock with no port feels fast and safe.
- **Black (risks).** `.or_http()` is one more call than the issue shows.
  Per-route timeouts are not visible, so the deadline can be too long
  (an override) or too short (`timeout = "off"`). Propagation sends the
  incoming `traceparent`, not the server span; with OpenTelemetry the
  downstream span is a sibling, not a child.
- **Yellow (benefits).** One line to call a service. Request ID and trace
  context cross the hop. The deadline stops wasted work downstream. The
  same metrics style as the server.
- **Green (ideas).** `GrpcClients::for_request(&tonic::Request)` for calls
  from a gRPC service. Native or webpki roots. Retries with tower. A
  per-route deadline when Autumn publishes it.
- **Blue (process).** TDD: config tests and client tests first (RED), then
  the code (GREEN), then a refactor with shared metric helpers. ADR 0009.
  README section. Verification rows. Review agents. PR.

## 5. Acceptance criteria

| ID | Criterion |
|---|---|
| AC1 | Crate feature `client`. It adds no server-only cost, and the server builds without it. `clients` config without the feature stops boot. |
| AC2 | `[grpc.clients.<name>]`: `endpoint`, `timeout_ms`, `connect_timeout_ms`, `tls.ca_path`, `tls.cert_path`, `tls.key_path`, `tls.domain_name`. Same layers and strict validation as `[grpc]`. Env: `AUTUMN_GRPC__CLIENTS__<NAME>__<KEY>`. |
| AC3 | `GrpcClient<T>` gives a ready client. `GrpcClients::get::<T>(name)` covers two endpoints of one type. A missing registration is a clear 500 at request time and a warning at boot. |
| AC4 | Lazy connect. A down endpoint does not stop boot and does not change readiness. A call to it gives `UNAVAILABLE`. |
| AC5 | Autumn's request ID (`x-request-id`) and a valid `traceparent` go into outgoing metadata. `grpc-timeout` is the smaller of the client timeout and the time left on the incoming request. |
| AC6 | `.or_http()` and `status_to_error` convert `tonic::Status` to `AutumnError` with a fixed map: `InvalidArgument`, `OutOfRange`, `FailedPrecondition` → 400, `Unauthenticated` → 401, `PermissionDenied` → 403, `NotFound` → 404, `AlreadyExists`, `Aborted` → 409, `ResourceExhausted` → 429, `Unavailable` → 503, `DeadlineExceeded` → 504, others → 502. 4xx text is fixed. 5xx details show only in `dev`. |
| AC7 | `grpc_client_handled_total`, `grpc_client_handling_seconds_sum`, `grpc_client_handling_seconds_count` and `grpc_client_in_flight`, with a `client` label. Labels stay bounded. |
| AC8 | `.client_double(name, service)` points a client at an in-process tonic service over an in-memory transport. No port. |
| AC9 | README section, ADR 0009, verification rows, CI with the `client` feature. Quality gates pass. |
