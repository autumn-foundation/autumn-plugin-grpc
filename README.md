# autumn-plugin-grpc

gRPC for [Autumn](https://autumn-web.app), built on [tonic](https://docs.rs/tonic) 0.14.

Add one plugin. Autumn serves HTTP on its port and gRPC on a dedicated
HTTP/2 port.

```rust
use autumn_plugin_grpc::GrpcPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(autumn_web::routes![index])
        .plugin(
            GrpcPlugin::new()
                .add_service(GreeterServer::new(MyGreeter))
                .file_descriptor_set(FILE_DESCRIPTOR_SET),
        )
        .run()
        .await;
}
```

## Features

| Feature | Default |
|---|---|
| Dedicated HTTP/2 listener | `0.0.0.0:50051` |
| Health service `grpc.health.v1.Health` | on |
| Server reflection (v1 and v1alpha) | on in `dev`/`test`, off in other profiles |
| `grpc` indicator in `/actuator/health` | on |
| `grpc_server_*` metrics in `/actuator/prometheus` | on |
| `AppState` in each request's extensions | always |
| Graceful drain on shutdown | 10 s grace |
| Tower layers and tonic interceptors (user services only) | none |
| TLS and mTLS | crate feature `tls` |

## Install

```toml
[dependencies]
autumn-plugin-grpc = "0.1"
# With TLS:
# autumn-plugin-grpc = { version = "0.1", features = ["tls"] }
```

The crate re-exports `tonic`, `tonic_health` and `tonic_reflection`. Use
these re-exports, or pin the same tonic version (0.14) in your app.

## Generate code

Use `tonic-prost-build` in your `build.rs`. Write the descriptor set too,
for reflection:

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
    // state.extension::<MyStore>(), the database pool, the config ...
}
```

To build a service from `AppState` at startup, use `add_service_with`:

```rust
GrpcPlugin::new().add_service_with(|state| GreeterServer::new(MyGreeter::new(state.clone())))
```

## Guard calls

A layer or an interceptor wraps user services only. Health and
reflection stay open, so probes and tools work.

```rust
GrpcPlugin::new()
    .add_service(GreeterServer::new(MyGreeter))
    .interceptor(|request: tonic::Request<()>| {
        match request.metadata().get("authorization") {
            Some(token) if token == "Bearer secret" => Ok(request),
            _ => Err(tonic::Status::unauthenticated("token required")),
        }
    })
    .gated("bearer token") // shows the services as gated in `autumn routes`
```

`.layer(l)` accepts any tower layer that axum accepts.

## Configure

The plugin reads `[grpc]` from `autumn.toml`. It uses the same layers as
Autumn core: base file, `[profile.<name>.grpc]`, `autumn-<profile>.toml`,
then `AUTUMN_GRPC__*` environment variables. Unknown keys and bad values
stop boot.

```toml
[grpc]
enabled = true
bind = "0.0.0.0:50051"                  # IP:port; port 0 picks a free port
health = true
reflection = "auto"                     # true, false or "auto"
metrics = true
max_metric_series = 1000
shutdown_grace_ms = 10000
timeout_ms = 0                          # per call; 0 = none
concurrency_limit_per_connection = 0    # 0 = no limit
max_concurrent_streams = 0              # 0 = hyper default
tcp_nodelay = true
tcp_keepalive_ms = 0                    # 0 = off
http2_keepalive_interval_ms = 0         # 0 = off
http2_keepalive_timeout_ms = 0          # 0 = tonic default (20 s)
max_connection_age_ms = 0               # 0 = never

[grpc.tls]                              # needs the `tls` feature
cert_path = ""
key_path = ""
client_ca_path = ""                     # set it to require client certificates
client_auth_optional = false
```

Examples of environment overrides:

```sh
AUTUMN_GRPC__BIND=0.0.0.0:6000
AUTUMN_GRPC__TLS__CERT_PATH=/etc/certs/server.pem
```

Set values in code on top of the file values:

```rust
GrpcPlugin::new().bind("127.0.0.1:0").configure(|c| c.timeout_ms = 5_000)
```

If you set `[grpc.tls]` without the `tls` feature, boot stops. The plugin
does not fall back to plain text.

### Two servers

Use a second section. The env prefix follows the section name.

```rust
.plugin(GrpcPlugin::new().add_service(public_api))
.plugin(GrpcPlugin::new().config_section("grpc_admin").add_service(admin_api))
// reads [grpc_admin] and AUTUMN_GRPC_ADMIN__*
```

## Health

- The gRPC health service reports `SERVING` for `""` and for each user
  service after start.
- At shutdown, it reports `NOT_SERVING` first. Then the server drains.
- `GrpcHandle::health_reporter()` lets the app change a status at runtime.
- The Autumn indicator (named like the section, default `grpc`) is `UP`
  only while the server serves. It is in the readiness group. Set
  `[health] detailed = true` to see its `state` and `address`.

## Metrics

| Name | Type | Labels |
|---|---|---|
| `grpc_server_handled_total` | counter | `grpc_service`, `grpc_method`, `grpc_code` |
| `grpc_server_handling_seconds_sum` | counter | `grpc_service`, `grpc_method` |
| `grpc_server_handling_seconds_count` | counter | `grpc_service`, `grpc_method` |
| `grpc_server_in_flight` | gauge | — |
| `grpc_server_up` | gauge | — |

Clients control the request path. The label set is bounded:

- An unknown service is `unknown`.
- A call that returns `UNIMPLEMENTED` has method `unknown`.
- After `max_metric_series` label sets, new calls count as `other`.

## Shutdown

Autumn runs plugin shutdown hooks after its HTTP drain, inside
`server.shutdown_timeout_secs`. The plugin then:

1. Sets every health status to `NOT_SERVING`.
2. Stops accepting connections and sends HTTP/2 `GOAWAY`.
3. Waits for in-flight calls, up to `shutdown_grace_ms`.
4. Closes the connections that are still open.

If `shutdown_grace_ms` is longer than `server.shutdown_timeout_secs`, the
plugin uses the shorter value and logs a warning.

To drain earlier (for example from a readiness probe), call
`GrpcHandle::shutdown()`. Get the handle from `GrpcPlugin::handle()` or
from `AppState`:

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
