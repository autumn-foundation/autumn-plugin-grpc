# Architecture

## Components

| File | Job |
|---|---|
| `src/plugin.rs` | `GrpcPlugin` builder. `Plugin::build` registers hooks, the health indicator, the metrics source and the route listing. Route assembly. |
| `src/config.rs` | `[grpc]` section: types, defaults, validation, layered resolution. |
| `src/server.rs` | Bind, serve, drain, close. `GrpcHandle`. |
| `src/lifecycle.rs` | Lifecycle state machine. The only way state changes. |
| `src/metrics.rs` | Metrics layer and bounded series store. |
| `src/health.rs` | Autumn `HealthIndicator` and `MetricsSource`. |
| `src/tls.rs` | TLS config from files (feature `tls`). |
| `src/error.rs` | `GrpcError`: startup failures. |

## Boot

```mermaid
sequenceDiagram
    participant App as AppBuilder
    participant P as GrpcPlugin
    participant H as startup hook
    participant S as tonic Server
    App->>P: build(app)
    P->>P: resolve [grpc] (files, profile, env, code)
    P->>App: config_section, declare_plugin_routes
    P->>App: health_indicator, metrics_source
    P->>App: on_startup, on_shutdown
    App->>H: run(AppState)
    H->>H: check TLS
    H->>H: build user services from AppState
    H->>H: user layers, then health + reflection
    H->>H: AppState layer, metrics layer
    H->>S: bind listener (error aborts boot)
    H->>S: spawn serve task
    H->>App: insert GrpcHandle into AppState
```

## Request path

```mermaid
flowchart LR
    C[client] --> T[TCP + optional TLS]
    T --> K[Killable I/O]
    K --> TS[tonic Server: timeout, concurrency limit, trace span]
    TS --> M[MetricsLayer]
    M --> E[Extension AppState]
    E --> R{route}
    R -->|user service| L[user layers / interceptor] --> U[tonic service]
    R -->|health| HS[grpc.health.v1]
    R -->|reflection| RS[grpc.reflection]
    R -->|unknown| F[UNIMPLEMENTED]
```

User layers wrap only user services. axum applies `layer` to the routes
that exist when it is called. The plugin adds health and reflection after
the user layers.

## Shutdown

```mermaid
sequenceDiagram
    participant A as Autumn
    participant G as GrpcHandle::shutdown
    participant S as tonic Server
    A->>A: HTTP drain
    A->>G: on_shutdown hook
    G->>G: Serving -> Draining
    G->>G: health NOT_SERVING
    G->>S: stop token: close listener, GOAWAY
    alt calls end within grace
        S-->>G: task ends
    else grace expires
        G->>S: kill token: fail I/O on open connections
        S-->>G: task ends (abort after 1 s more)
    end
    G->>G: Draining -> Stopped
```

tonic spawns a task per connection. Aborting the server task does not
stop them. The `Killable` I/O wrapper closes them when the kill token
fires.

## Lifecycle

See `src/lifecycle.rs`. States: `Idle`, `Serving`, `Draining`,
`Stopped`, `Failed`. `LifecycleCell` applies events with a
compare-and-swap loop. `tests/lifecycle.rs` checks all 25
state × event pairs against the spec table and property-tests the
invariants.
