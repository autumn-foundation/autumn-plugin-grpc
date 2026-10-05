# autumn-plugin-grpc

gRPC for [Autumn](https://autumn-web.app), built on [tonic](https://docs.rs/tonic) 0.14.

Add one plugin. Autumn serves HTTP on its port. The plugin serves gRPC on
a dedicated HTTP/2 port, or on Autumn's port (see
[Share Autumn's port](#share-autumns-port)). Handlers can also call other
gRPC services (see [Call gRPC services](#call-grpc-services)).

```rust
use autumn_plugin_grpc::GrpcPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(autumn_web::routes![index])
        .plugin(
            GrpcPlugin::new()
                .add_service(GreeterServer::new(MyGreeter))
                .file_descriptor_set(FILE_DESCRIPTOR_SET)
                .public(),
        )
        .run()
        .await;
}
```

## Features

| Feature | Default |
|---|---|
| Dedicated HTTP/2 listener | `127.0.0.1:50051` in `dev`/`test`, `0.0.0.0:50051` in other profiles |
| Shared listener on Autumn's HTTP port | off; crate feature `multiplex` |
| Health service `grpc.health.v1.Health` | on |
| Server reflection (v1 and v1alpha) | on in `dev`/`test`, off in other profiles |
| `grpc` indicator in `/actuator/health` | on |
| `grpc_server_*` metrics in `/actuator/prometheus` | on |
| `AppState` in the extensions of each request | always |
| Graceful drain on shutdown | 10 s grace |
| Guards: tower layers and tonic interceptors (user services only) | none |
| Resource limits: connections, HTTP/2 streams, stream resets | 1000, 200, 1024 |
| TLS and mTLS | crate feature `tls` |
| Clients for handlers: `GrpcClient<T>`, `GrpcClients` | off; crate feature `client` |

## Install

```toml
[dependencies]
autumn-plugin-grpc = "0.1"
# With TLS:
# autumn-plugin-grpc = { version = "0.1", features = ["tls"] }
# On Autumn's HTTP port:
# autumn-plugin-grpc = { version = "0.1", features = ["multiplex"] }
# To call gRPC services from handlers:
# autumn-plugin-grpc = { version = "0.1", features = ["client"] }
```

The crate re-exports `tonic`, `tonic_health` and `tonic_reflection`. Use
these re-exports, or use tonic 0.14 in your app.

## Generate code

Use `tonic-prost-build` in your `build.rs`. Also write the descriptor set,
for reflection and for exact metric labels:

```rust
// build.rs
let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
tonic_prost_build::configure()
    .file_descriptor_set_path(out.join("greeter_descriptor.bin"))
    .compile_protos(&["proto/greeter.proto"], &["proto"])?;
```

```rust
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/greeter_descriptor.bin"));
```

## Use app state

Each request has `AppState` in its extensions:

```rust
async fn say(&self, request: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
    let state = request.extensions().get::<AppState>().expect("set by the plugin");
    // Read app data, for example `state.extension::<MyStore>()`.
}
```

To build a service from `AppState` at startup, use `add_service_with`:

```rust
GrpcPlugin::new().add_service_with(|state| GreeterServer::new(MyGreeter::new(state.clone())))
```

## Guard calls

A guard wraps user services only. Health and reflection stay open, so
probes and tools work.

```rust
GrpcPlugin::new()
    .add_service(GreeterServer::new(MyGreeter))
    .guard_interceptor(
        |request: tonic::Request<()>| match request.metadata().get("authorization") {
            Some(token) if token == "Bearer secret" => Ok(request),
            _ => Err(tonic::Status::unauthenticated("token required")),
        },
        "bearer token",
    )
```

- `guard(layer, label)` takes any tower layer that axum accepts.
- `layer(l)` and `interceptor(i)` also wrap user services. Use them for
  work that is not access control, for example tracing.

### Route listing

`autumn routes` shows each service as `GRPC /<service>/*`. A plugin on a
named section, such as `grpc_admin`, shows `GRPC:grpc_admin` as the
method. Autumn 0.8 refuses two plugins that declare the same method and
path:

| Plugin call | Classification |
|---|---|
| `guard` or `guard_interceptor` | `gated`, with the label as middleware |
| `public()` | `public` |
| none of these | `unclassified`: `autumn routes audit` fails |
| health and reflection | `framework` |

The audit applies HTTP rules. Its CSRF and mTLS columns do not apply to
the gRPC listener.

## Configure

The plugin reads `[grpc]` from `autumn.toml`. It uses the same layers as
Autumn core: base file, `[profile.<name>.grpc]`, `autumn-<profile>.toml`,
then `AUTUMN_GRPC__*` environment variables. These stop boot:

- an unknown key,
- a bad value in a file or in an environment variable,
- an unreadable `.env` file.

```toml
[grpc]
enabled = true
listener = "dedicated"                  # "dedicated" or "shared" (feature `multiplex`)
bind = ""                               # IP:port; empty: the profile decides
health = true
reflection = "auto"                     # true, false or "auto"
metrics = true
max_metric_series = 1000
shutdown_grace_ms = 10000               # must be more than 0
timeout_ms = 0                          # unary calls; 0: off
max_connections = 1000                  # 0: off
concurrency_limit_per_connection = 0    # 0: off; a stream counts until it responds
max_concurrent_streams = 200            # must be more than 0
http2_max_local_error_reset_streams = 1024  # must be more than 0
tcp_nodelay = true
tcp_keepalive_ms = 0                    # 0: off
http2_keepalive_interval_ms = 60000     # 0: off
http2_keepalive_timeout_ms = 20000
max_connection_age_ms = 0               # 0: off

[grpc.tls]                              # needs the `tls` feature
cert_path = ""
key_path = ""
client_ca_path = ""                     # set it to require client certificates
client_auth_optional = false            # needs client_ca_path
handshake_timeout_ms = 10000            # must be more than 0

[grpc.clients.billing]                  # needs the `client` feature; one table for each client
endpoint = "http://billing:50051"       # http:// or https://, no path; necessary
timeout_ms = 10000                      # each call; 0: off
connect_timeout_ms = 5000               # must be more than 0

[grpc.clients.billing.tls]              # https only; needs the `tls` feature
ca_path = ""                            # necessary for https
cert_path = ""                          # client certificate (mTLS); needs key_path
key_path = ""
domain_name = ""                        # empty: the endpoint host
```

A `Toggle` (`reflection`) accepts `true`, `false`, `"auto"`, `"on"`,
`"off"`, `"yes"`, `"no"`, `"enabled"` and `"disabled"`.

Examples of environment overrides:

```sh
AUTUMN_GRPC__BIND=0.0.0.0:6000
AUTUMN_GRPC__TLS__CERT_PATH=/etc/certs/server.pem
AUTUMN_GRPC__CLIENTS__BILLING__ENDPOINT=http://billing:50051
```

A client env override works for each client in the files and for each
name that the code registers.

Set values in code, on top of the file values:

```rust
GrpcPlugin::new().bind("127.0.0.1:0").configure(|c| c.timeout_ms = 5_000)
```

If you set `[grpc.tls]` without the `tls` feature, boot stops. The plugin
does not fall back to plain text.

### Two servers

Use a second section. The environment prefix changes with the section.

```rust
.plugin(GrpcPlugin::new().add_service(public_api).public())
.plugin(GrpcPlugin::new().config_section("grpc_admin").add_service(admin_api).public())
// The second plugin reads [grpc_admin] and AUTUMN_GRPC_ADMIN__*.
```

- Each server needs its own section. The plugin name contains the
  section, and Autumn skips a second plugin with the same name.
- Metrics have a `server` label with the section.
- `AppState` has a `GrpcServers` value. `GrpcServers::get(section)` gives
  the handle of each server. `AppState` has a `GrpcHandle` only for the
  `grpc` section.

## Share Autumn's port

With the `multiplex` feature, gRPC can use Autumn's HTTP port:

```toml
[grpc]
listener = "shared"
```

- The plugin binds no port. `GrpcHandle::local_addr()` is `None`.
- HTTP/2 requests with a gRPC content type go to the gRPC services,
  before Autumn's HTTP middleware. No Autumn HTTP protection applies to
  them: CSRF, sessions, the request timeout, body limits, rate limits,
  load shedding, maintenance mode, trusted hosts, bot protection, CORS,
  error pages and HTTP metrics. Use `guard` and `layer` for gRPC. tonic's
  message size limits (4 MiB) still apply.
- All other requests go to Autumn as before. This includes HTTP/1.1 and
  `application/grpc-web` requests.
- Health, reflection, metrics, guards, `AppState`, `timeout_ms` and
  `grpc-timeout` work as with a dedicated listener.
- `request.remote_addr()` gives the TCP peer. `local_addr()` is `None`.
  On a Unix socket, `remote_addr()` is `None`.
- The feature turns on HTTP/2 (h2c) in axum for the whole app.
- Autumn's server owns the connections. These settings have no effect,
  and the plugin logs a warning when you change them: `bind`,
  `max_connections`, `concurrency_limit_per_connection`,
  `max_concurrent_streams`, `http2_max_local_error_reset_streams`,
  `tcp_nodelay`, `tcp_keepalive_ms`, `http2_keepalive_*`,
  `max_connection_age_ms` and `tls.handshake_timeout_ms`. hyper's defaults
  apply (200 streams, 1024 local resets).
- Only one plugin in an app can use shared mode.

These stop boot:

- `listener = "shared"` without the `multiplex` feature,
- `[grpc.tls]` in shared mode (use `[server.tls]`),
- `[server.tls]` on autumn-web 0.7. Its TLS listener does not offer
  HTTP/2 (ALPN `h2`). autumn-foundation/autumn#2321 fixes this in the next
  Autumn release. Until then, end TLS at a proxy or use a dedicated
  listener.

At shutdown:

1. Autumn reports not ready. Health reports `NOT_SERVING`. Calls still
   run during `server.prestop_grace_secs`.
2. Autumn stops its listener. The plugin starts its drain at the same
   time: health `Watch` streams end, new calls get `UNAVAILABLE`, and open
   calls get the grace period.
3. After the grace, the plugin ends open calls with `UNAVAILABLE`. Then
   Autumn's HTTP drain can finish.

A client that stops reading a stream can keep its connection open until
Autumn's shutdown timeout. See [ADR 0008](docs/adr/0008-shared-listener.md).

## Call gRPC services

With the `client` feature, a handler calls a downstream service with one
extractor:

```rust
use autumn_plugin_grpc::{GrpcChannel, GrpcClient, GrpcResultExt};

#[get("/invoice/{id}")]
async fn invoice(
    Path(id): Path<String>,
    GrpcClient(mut billing): GrpcClient<BillingClient<GrpcChannel>>,
) -> AutumnResult<Json<Invoice>> {
    let reply = billing.get_invoice(GetInvoice { id }).await.or_http()?;
    Ok(Json(reply.into_inner().into()))
}
```

```rust
GrpcPlugin::new().client("billing", BillingClient::new)
```

```toml
[grpc.clients.billing]
endpoint = "http://billing:50051"
```

- The client type is `<Generated>Client<GrpcChannel>`. `GrpcChannel`
  wraps a tonic `Channel`. It adds request context, a deadline and
  metrics to each call.
- `GrpcClient<T>` gives the only client of type `T`. For two endpoints of
  one type, register two names and use `GrpcClients`:

  ```rust
  async fn handler(clients: GrpcClients) -> AutumnResult<String> {
      let mut eu = clients.get::<BillingClient<GrpcChannel>>("billing_eu")?;
      // ...
  }
  ```

  `AppState` also has `GrpcClients`
  (`state.extension::<GrpcClients>()`). A client from `AppState` sends no
  request context.
- The clients do not need the server. They work with `enabled = false`.
- The channel connects at the first call. A down endpoint does not stop
  boot and does not change readiness. Its calls fail with `UNAVAILABLE`.

### Request context

Each call from an extractor sends:

- `x-request-id`: Autumn's request ID.
- `traceparent` and `tracestate`: the values of the incoming request.
  They go only when `traceparent` is a valid W3C value, and
  `tracestate` only when it has 512 bytes or less.
- `grpc-timeout`: the smallest of the client `timeout_ms`, the time left
  on the incoming request, and a timeout that the caller set with
  `Request::set_timeout`.

Rules:

- The channel does not replace metadata that the caller sets. The trace
  pair goes only when the caller set neither `traceparent` nor
  `tracestate`.
- The time left is `server.timeouts.request_timeout_ms` minus the time
  since the extractor ran. `0` or no value: no limit from the request.
  Autumn does not publish the request start or per-route `timeout`
  values, so the time before the extractor is not counted (ADR 0009).
- When no time is left, the call ends with `DEADLINE_EXCEEDED` and is not
  sent.
- The client trusts the incoming trace context, as Autumn does. The
  request ID and the trace pair go to every endpoint, also to services of
  other companies. Other headers, such as `authorization`, do not go.

### Errors

A bare `?` on `tonic::Status` gives a 500: Autumn converts every error
type to 500. Use `.or_http()?` (or `status_to_error`) for this map:

| gRPC code | HTTP |
|---|---|
| `INVALID_ARGUMENT`, `OUT_OF_RANGE`, `FAILED_PRECONDITION` | 400 |
| `UNAUTHENTICATED` | 401 |
| `PERMISSION_DENIED` | 403 |
| `NOT_FOUND` | 404 |
| `ALREADY_EXISTS`, `ABORTED` | 409 |
| `RESOURCE_EXHAUSTED` | 429 |
| `UNAVAILABLE` | 503 |
| `DEADLINE_EXCEEDED` (also a client timeout) | 504 |
| all others | 502 |

- A 4xx response has a fixed text, for example `not found`. The
  downstream message does not go to the HTTP caller.
- A 5xx response has the code and the downstream message. Autumn shows
  it only in `dev`.
- The log has the full status.
- `UNAUTHENTICATED` and `PERMISSION_DENIED` from the downstream service
  become 401 and 403. If the app's own service credentials fail, the
  HTTP caller also gets 401 or 403. Match on the `Status` yourself if you
  need a different result.

A failed client lookup gives a 500: no registration, the wrong type, or
two clients of one type for `GrpcClient<T>`. Autumn shows the message
only in `dev`.

These errors stop boot:

- a registered client with no endpoint and no test double,
- a client name that two registrations use (one type, or two plugins),
- a bad endpoint (only lowercase `http://` and `https://`),
- `https` without the `tls` feature, or without `tls.ca_path`,
- a TLS file that cannot be read.

These log a warning at boot: a `[<section>.clients.<name>]` table with no
registration, and one client type on two names.

### Test doubles

A test double is a fake service in the process. Point a client at it.
The client uses no port:

```rust
let mut config = GrpcConfig::default();
config.enabled = false; // this app serves no gRPC itself
let plugin = GrpcPlugin::new()
    .config(config)
    .development(true)
    .client("billing", BillingClient::new)
    .client_double("billing", BillingServer::new(FakeBilling));
let client = TestApp::new().routes(routes![invoice]).plugin(plugin).build();
```

The double uses the same `GrpcChannel`, so the test sees the metadata,
the timeouts and the metrics.

## Security

- The defaults limit connections, HTTP/2 streams and stream resets
  (a protection against reset floods). The TLS handshake has a timeout.
- The health and reflection services decode requests of 16 KiB at most.
- User services use the tonic decode limit (4 MiB). Set a lower limit on
  each generated server, for example
  `GreeterServer::new(g).max_decoding_message_size(64 * 1024)`.
- In `dev` and `test`, the listener is on loopback. The plugin logs a
  warning when reflection is on and the listener is not on loopback.
- With `client_auth_optional = true`, clients without a certificate can
  connect. Check `request.peer_certs()` in a guard. The plugin accepts
  every certificate that `client_ca_path` signs. Check the identity in a
  guard if you need more.
- The health `Check` call shows if a service name exists, also when
  reflection is off. This is standard gRPC behavior.
- Clients send the request ID and a valid `traceparent` (and a
  `tracestate` of 512 bytes or less) of the incoming request to each
  endpoint. They do not send other headers, such as `authorization`.
- Client endpoints accept only lowercase `http://` and `https://`. An
  `https` endpoint needs the `tls` feature and `tls.ca_path`. The plugin
  never falls back to plain text.
- The clients add no app layer. Autumn's idempotency replay does not
  change.

## Health

- The gRPC health service reports `SERVING` for `""` and for each user
  service after start.
- It follows Autumn readiness. When Autumn starts to shut down (or an
  operator drains it), it reports `NOT_SERVING` at once. Calls still run.
- `GrpcHandle::health_reporter()` lets the app change a status at runtime.
- The Autumn indicator (its name is the section, default `grpc`) is `UP`
  only while the server serves. It is in the readiness group. Set
  `[health] detailed = true` to see its `state` and `address`.

## Metrics

| Name | Type | Labels |
|---|---|---|
| `grpc_server_handled_total` | counter | `server`, `grpc_service`, `grpc_method`, `grpc_code` |
| `grpc_server_handling_seconds_sum` | counter | `server`, `grpc_service`, `grpc_method` |
| `grpc_server_handling_seconds_count` | counter | `server`, `grpc_service`, `grpc_method` |
| `grpc_server_in_flight` | gauge | `server` |
| `grpc_server_up` | gauge | `server` |
| `grpc_client_handled_total` | counter | `client`, `grpc_service`, `grpc_method`, `grpc_code` |
| `grpc_client_handling_seconds_sum` | counter | `client`, `grpc_service`, `grpc_method` |
| `grpc_client_handling_seconds_count` | counter | `client`, `grpc_service`, `grpc_method` |
| `grpc_client_in_flight` | gauge | `client` |

Remote callers control the request path. The plugin limits the label
sets:

- It labels an unknown service `unknown`.
- It labels a method `unknown` until the method is known. A method is
  known when a registered descriptor set lists it, or after its first
  `OK` response.
- After `max_metric_series` label sets, it labels new service and method
  pairs `other`. The code label stays.

Client labels: the `client` label is a registered name. App code sets the
path, so service and method keep their names. A value that is not a valid
name is `unknown`. `max_metric_series` caps the label sets of each
client.

## Shutdown

1. Autumn starts to shut down. Health reports `NOT_SERVING` at once.
2. Autumn drains HTTP, then runs the plugin shutdown hook, inside
   `server.shutdown_timeout_secs`.
3. The plugin stops accepting connections and sends HTTP/2 `GOAWAY`.
4. It waits for in-flight calls, up to the grace period.
5. It closes the connections that are still open.

The plugin caps the grace period: it must fit in
`server.shutdown_timeout_secs`, with 1 s left to close connections. A task
does the drain, so it completes also when Autumn stops the hook.

To drain earlier, call `GrpcHandle::shutdown()`. Get the handle from
`GrpcPlugin::handle()`, or from `AppState`:

```rust
let handle = state.extension::<autumn_plugin_grpc::GrpcHandle>();
```

## Tests

`bind("127.0.0.1:0")` picks a free port. `GrpcHandle::local_addr()` gives
the real address after boot. `autumn_web::test::TestApp` runs the startup
hook, so the server is up when `build()` returns. `TestApp` does not run
shutdown hooks: call `handle.shutdown().await` yourself.

## Example

```sh
cargo run --example client --features client   # a handler that calls gRPC
cargo run --example echo
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'authorization: Bearer demo' \
  -d '{"message":"hello"}' localhost:50051 autumn.echo.v1.Echo/Say
```

## Compatibility

| autumn-plugin-grpc | autumn-web | tonic | MSRV |
|---|---|---|---|
| 0.1 | 0.8 | 0.14 | 1.88 |

## Documents

- [Architecture](docs/architecture.md)
- [Plan and acceptance criteria](docs/plan.md)
- [Shared listener plan](docs/plan-shared-listener.md)
- [Client plan](docs/plan-client.md)
- [Verification](docs/verification.md)
- [Decisions](docs/adr/)

## License

Apache-2.0
