# Plan — autumn-plugin-grpc

This file records the planning phase. It uses three methods:
brainstorming, reverse brainstorming and six thinking hats.
The acceptance criteria (AC) at the end are the contract for v0.1.

No GitHub issue exists for this work. This file is the source of the AC.

## 1. Goal

Serve gRPC services from an Autumn app with one line of code:

```rust
autumn_web::app()
    .plugin(GrpcPlugin::new().add_service(GreeterServer::new(MyGreeter)))
    .run()
    .await;
```

The plugin uses [tonic] 0.14. The plugin owns the gRPC listener, the
lifecycle, health, reflection, metrics and configuration.

[tonic]: https://docs.rs/tonic

## 2. Brainstorming

Ideas, not filtered:

- Mount tonic services on Autumn's HTTP port (h2c).
- Run tonic on a dedicated port (50051).
- Standard health service `grpc.health.v1.Health`.
- Server reflection, so `grpcurl` works with no `.proto` files.
- Bridge gRPC state into Autumn `/actuator/health`.
- Prometheus metrics: calls by service, method and status code.
- Graceful drain on shutdown.
- `[grpc]` section in `autumn.toml`, with profiles and env overrides.
- Build services from `AppState` (database pool, config).
- Put `AppState` into each request, so handlers can read it.
- Tower layers for auth, rate limits, tracing.
- TLS and mTLS.
- gRPC-Web for browsers.
- Compression (gzip, zstd).
- Code generation helper (`build.rs`).
- Port 0 for tests, with a handle that gives the bound address.
- Test helpers: a connected client channel.

## 3. Reverse brainstorming

Question: "How can we make this plugin fail in production?"
Each answer gives a countermeasure.

| How to fail | Countermeasure |
|---|---|
| Share Autumn's port. CSRF (on in `prod`) rejects every gRPC `POST`. | Use a dedicated listener. Autumn HTTP middleware does not touch gRPC. (ADR 0002) |
| Autumn's error-page and compression layers rewrite gRPC bodies. | Same: dedicated listener. |
| Port is in use. App boots, but gRPC is silently down. | Bind in the startup hook. A bind error aborts boot. |
| Typo in `[grpc]`. Defaults apply silently. | Unknown keys and bad values abort boot. |
| Health says SERVING during shutdown. Load balancer sends new calls. | Set NOT_SERVING first, then drain. |
| Shutdown waits forever for a long stream. | Drain has a grace period. After it, abort. |
| Attacker sends random method paths. Metric labels explode. | Label unknown services and `UNIMPLEMENTED` calls as `unknown`. Cap the series count. |
| Reflection leaks the API shape in production. | Reflection is `auto`: on in `dev`/`test`, off in other profiles. |
| Auth layer blocks Kubernetes health probes. | User layers wrap user services only. Health and reflection are not wrapped. |
| Metric names start with `autumn_`. Autumn drops them. | Use the `grpc_server_` prefix. |
| Handler panics or `unwrap` in library code. | No `unwrap`/`expect`/`panic!` in library code. Clippy warns, and CI makes warnings errors. |
| TLS paths set, but the crate is built without TLS. Traffic is plain text. | This is a config error. Boot stops. |
| Invalid lifecycle order (serve after stop). | A pure state machine owns the lifecycle. Tests check every transition. |

## 4. Six thinking hats

**White (facts).**
Autumn 0.7 serves with `axum::serve`, HTTP/1 only by default.
Autumn's `prod` profile turns on CSRF for `POST`.
tonic 0.14 uses axum 0.8, the same version as Autumn.
`tonic::service::Routes` converts to and from `axum::Router`.
`MetricsSource` supports counters and gauges only.
`TestApp` runs startup hooks but not shutdown hooks.
`protoc` and Verus are not in the build environment.

**Red (feelings).**
Install with one line, as other Autumn plugins do. `grpcurl list` must
work in dev with no setup. Operators must trust the health signal.

**Black (risks).**
Two ports need two firewall rules. Feature unification can change
axum features. The config loader is a copy of core logic and can drift.
Verus is not available, so no machine-checked proof ships.

**Yellow (benefits).**
The dedicated port isolates gRPC from HTTP middleware. It also permits
gRPC-specific HTTP/2 tuning. The health bridge gives one readiness
signal for HTTP and gRPC.

**Green (creative).**
Put `AppState` in every request. Build services lazily from `AppState`.
Expose a `GrpcHandle` for port-0 tests and manual drain.
Model the lifecycle as a finite state machine and check all
state × event pairs (a complete model check for a finite system).

**Blue (process).**
Decisions:

1. Dedicated listener only in v0.1 (ADR 0002).
2. Scope: services, health, reflection, metrics, lifecycle, config,
   layers, TLS (feature `tls`).
3. Out of scope for v0.1: gRPC-Web, compression switches, shared port,
   codegen helpers. Record as follow-ups.
4. Work in SPEC → RED → GREEN → REFACTOR order.
5. Verus is not available. The lifecycle spec is an exhaustive
   transition-table test plus property tests (ADR 0003).

## 5. Acceptance criteria

| ID | Criterion |
|---|---|
| AC1 | `GrpcPlugin::new().add_service(svc)` serves any tonic service on a dedicated HTTP/2 listener. The default address is `0.0.0.0:50051`. |
| AC2 | Configuration comes from `[grpc]` in `autumn.toml`, with profile layering and `AUTUMN_GRPC__*` env overrides. Code setters apply on top. Invalid configuration aborts boot with a clear message. |
| AC3 | `add_service_with(|state| svc)` builds a service from `AppState`. Each request carries `AppState` in its extensions. |
| AC4 | The standard health service `grpc.health.v1.Health` is on by default. It reports SERVING for each service after start, and NOT_SERVING when shutdown starts. |
| AC5 | Server reflection (v1 and v1alpha) serves the registered descriptor sets. It is `auto`: on in `dev`/`test`, off in other profiles. |
| AC6 | The plugin reports to Autumn: a `grpc` health indicator in `/actuator/health`, and `grpc_server_*` Prometheus metrics by service, method and code. Metric label count is bounded. |
| AC7 | On shutdown, the plugin sets NOT_SERVING, stops new connections, drains in-flight calls for `shutdown_grace_ms`, then closes the open connections. Shutdown is idempotent. |
| AC8 | A bind failure aborts boot. |
| AC9 | Transport settings are configurable: timeout, concurrency limit, max concurrent streams, HTTP/2 keepalive, max connection age, TCP nodelay and keepalive. |
| AC10 | Feature `tls` enables TLS and mTLS from file paths. TLS paths without the feature abort boot. |
| AC11 | `.layer(l)` wraps all user services and not health or reflection. |
| AC12 | A pure lifecycle state machine has all transitions tested and its invariants property-tested. |
| AC13 | The plugin passes Autumn's plugin conformance harness. It declares its config section and has a stable, unique `name()`. |
| AC14 | Quality gates: `cargo fmt`, clippy pedantic + nursery with `-D warnings`, no `unwrap` in library code, ≥ 85 % line coverage, CI workflow, README, CLAUDE.md, ADRs, a runnable example. Docs use ASD-STE100. |
