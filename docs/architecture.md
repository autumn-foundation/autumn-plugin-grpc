# Architecture

## Components

| File | Job |
|---|---|
| `src/plugin.rs` | `GrpcPlugin` builder. `Plugin::build` registers hooks, the health indicator, the metrics source and the route listing. Route assembly. |
| `src/config.rs` | `[grpc]` section: types, defaults, validation, layered resolution. |
| `src/server.rs` | Bind, serve, drain, close. `GrpcHandle`. |
| `src/gate.rs` | Shared listener: the `static_gate` layer on Autumn's router (ADR 0008). |
| `src/lifecycle.rs` | Lifecycle state machine. The only way state changes. |
| `src/metrics.rs` | Metrics layer and bounded series store. |
| `src/health.rs` | Autumn `HealthIndicator`. |
| `src/registry.rs` | `GrpcServers`: all servers of an app, and the one Autumn `MetricsSource`. |
| `src/tls.rs` | TLS config from files (feature `tls`). |
| `src/error.rs` | `GrpcError`: startup failures. |
| `src/timeout.rs` | `grpc-timeout`: parse and encode. |
| `src/client/mod.rs` | Feature `client`: `GrpcClients` registry and extractor, `GrpcClient<T>`, `GrpcPlugin::client*`, boot checks (ADR 0009). |
| `src/client/channel.rs` | `GrpcChannel`: propagation, deadline and metrics around a tonic `Channel`. |
| `src/client/context.rs` | Request context (request ID, trace headers, deadline) and the request-start gate. |
| `src/client/status.rs` | `tonic::Status` → `AutumnError` code map, `.or_http()`. |
| `src/client/metrics.rs` | `grpc_client_*` metrics. |
| `src/client/memory.rs` | In-memory test doubles over `tokio::io::duplex`. |

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
    K[Killable I/O]
    T --> A[accept: max_connections permit]
    A --> K
    K --> TS[tonic Server: stream + reset limits, timeout, trace span]
    TS --> M[MetricsLayer]
    M --> E[Extension AppState]
    E --> R{route}
    R -->|user service| L[user layers / interceptor] --> U[tonic service]
    R -->|health| HS[grpc.health.v1]
    R -->|reflection| RS[grpc.reflection]
    R -->|unknown| F[UNIMPLEMENTED]
```

### Shared listener

```mermaid
flowchart LR
    C[client] --> AS[Autumn server: axum::serve, h1 + h2c]
    AS --> O[security headers, startup barrier, access log]
    O --> G{GrpcGate}
    G -->|HTTP/2 + application/grpc*| D[state check, call token, timeout]
    D --> M[MetricsLayer and the routes above]
    G -->|all other requests| H[Autumn middleware: CSRF, session, timeout, ...] --> HR[HTTP routes]
```

The gate answers `UNAVAILABLE` when the state is not `Serving`. The
response body keeps the call token until it ends. The drain waits for the
tokens, then ends the open bodies.

User layers wrap only user services. axum applies a layer only to the
routes that exist when the code calls `layer`. The plugin adds health and
reflection after the user layers.

## Client call

```mermaid
sequenceDiagram
    participant R as HTTP request
    participant G as request-start gate
    participant H as handler
    participant X as GrpcClient extractor
    participant C as GrpcChannel
    participant S as downstream service
    R->>G: record start time (before Autumn's timeout)
    G->>H: Autumn middleware, then the handler
    H->>X: extract
    X->>X: request ID, traceparent, deadline = start + request_timeout_ms
    X-->>H: client over GrpcChannel (with context)
    H->>C: call
    C->>C: add metadata (caller values stay)
    C->>C: grpc-timeout = min(client timeout, time left, caller timeout)
    alt no time left
        C-->>H: DEADLINE_EXCEEDED (not sent)
    else
        C->>S: lazy tonic Channel (TCP, TLS or in-memory double)
        S-->>C: response or status
        C-->>H: reply, or Status (timeout: DEADLINE_EXCEEDED)
    end
    H->>H: .or_http()? maps Status to HTTP
```

Each call goes into `grpc_client_*` metrics when its response body ends.

## Shutdown

```mermaid
sequenceDiagram
    participant A as Autumn
    participant G as GrpcHandle::shutdown
    participant S as tonic Server
    A->>A: begin shutdown (readiness 503)
    Note over G: readiness task: health NOT_SERVING
    A->>A: HTTP drain
    A->>G: on_shutdown hook
    G->>G: Serving -> Draining, spawn drain task
    G->>G: health NOT_SERVING, then clear (ends Watch streams)
    G->>S: stop token: close listener, GOAWAY
    alt calls end within grace
        S-->>G: task ends
    else grace expires
        G->>S: kill token: fail I/O on open connections
        S-->>G: task ends (abort after 1 s more)
    end
    G->>G: Draining -> Stopped
```

tonic spawns a task for each connection. An abort of the server task
does not stop them. The `Killable` I/O wrapper closes them when the kill
token fires. Each connection has a child token, so each poll locks only
its own token.

The drain runs in its own task. If Autumn drops the hook future, the
drain continues (ADR 0007).

## Lifecycle

See `src/lifecycle.rs`. States: `Idle`, `Serving`, `Draining`,
`Stopped`, `Failed`. `LifecycleCell` applies events with a
compare-and-swap loop. `tests/lifecycle.rs` checks all 25
state × event pairs against the spec table. Property tests check the
invariants.
