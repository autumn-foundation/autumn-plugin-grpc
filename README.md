# autumn-plugin-grpc

gRPC for [Autumn](https://autumn-web.app), built on [tonic](https://docs.rs/tonic) 0.14.

Add one plugin. Autumn serves HTTP on its port. The plugin serves gRPC on
a dedicated HTTP/2 port.

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
| Health service `grpc.health.v1.Health` | on |
| Server reflection (v1 and v1alpha) | on in `dev`/`test`, off in other profiles |
| `grpc` indicator in `/actuator/health` | on |
| `grpc_server_*` metrics in `/actuator/prometheus` | on |
| `AppState` in the extensions of each request | always |
| Graceful drain on shutdown | 10 s grace |
| Guards: tower layers and tonic interceptors (user services only) | none |
| Resource limits: connections, HTTP/2 streams, stream resets | 1000, 200, 1024 |
| TLS and mTLS | crate feature `tls` |

## Install

```toml
[dependencies]
autumn-plugin-grpc = "0.1"
# With TLS:
# autumn-plugin-grpc = { version = "0.1", features = ["tls"] }
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

`autumn routes` shows each service as `GRPC /<service>/*`:

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
```

A `Toggle` (`reflection`) accepts `true`, `false`, `"auto"`, `"on"`,
`"off"`, `"yes"`, `"no"`, `"enabled"` and `"disabled"`.

Examples of environment overrides:

```sh
AUTUMN_GRPC__BIND=0.0.0.0:6000
AUTUMN_GRPC__TLS__CERT_PATH=/etc/certs/server.pem
```

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

Clients control the request path. The plugin limits the label sets:

- It labels an unknown service `unknown`.
- It labels a method `unknown` until the method is known. A method is
  known when a registered descriptor set lists it, or after its first
  `OK` response.
- After `max_metric_series` label sets, it labels new service and method
  pairs `other`. The code label stays.

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
cargo run --example echo
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'authorization: Bearer demo' \
  -d '{"message":"hello"}' localhost:50051 autumn.echo.v1.Echo/Say
```

## Compatibility

| autumn-plugin-grpc | autumn-web | tonic | MSRV |
|---|---|---|---|
| 0.1 | 0.7 | 0.14 | 1.88 |

## Documents

- [Architecture](docs/architecture.md)
- [Plan and acceptance criteria](docs/plan.md)
- [Verification](docs/verification.md)
- [Decisions](docs/adr/)

## License

Apache-2.0
