# Changelog

## 0.1.0 — unreleased

- `GrpcPlugin`: serve tonic 0.14 services on a dedicated HTTP/2 listener.
- Health service, server reflection (v1 and v1alpha).
- Autumn health indicator and `grpc_server_*` metrics with bounded labels.
- Graceful drain with a grace period and forced close.
- `[grpc]` configuration with profile layering and env overrides.
- `add_service_with` (build from `AppState`), `AppState` in requests.
- `guard`, `guard_interceptor` and `public` set the route classification.
  `layer` and `interceptor` wrap user services without a classification.
- `GrpcServers` in `AppState` for apps with more than one server.
- Safe transport defaults: 200 streams, 1024 local resets, 1000
  connections, HTTP/2 keepalive, 10 s TLS handshake timeout, 16 KiB
  requests for health and reflection. Loopback bind in `dev`/`test`.
- Health follows Autumn readiness. The drain runs in its own task.
- `tls` feature: TLS and mTLS from PEM files.
- `multiplex` feature and `listener = "shared"`: serve gRPC on Autumn's
  HTTP port (ADR 0008).
