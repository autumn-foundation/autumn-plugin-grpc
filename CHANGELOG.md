# Changelog

## 0.1.0 — unreleased

- `GrpcPlugin`: serve tonic 0.14 services on a dedicated HTTP/2 listener.
- Health service, server reflection (v1 and v1alpha).
- Autumn health indicator and `grpc_server_*` metrics with bounded labels.
- Graceful drain with a grace period and forced close.
- `[grpc]` configuration with profile layering and env overrides.
- `add_service_with` (build from `AppState`), `AppState` in requests.
- `layer`, `interceptor` and `gated` for user services.
- `tls` feature: TLS and mTLS from PEM files.
