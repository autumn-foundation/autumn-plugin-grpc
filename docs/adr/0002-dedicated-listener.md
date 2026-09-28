# ADR 0002 — Dedicated gRPC listener, not Autumn's HTTP port

- Status: accepted
- Date: 2026-09-28

## Context

gRPC needs HTTP/2. Autumn 0.7 serves with `axum::serve` and turns on
HTTP/1 only. Autumn's global middleware wraps every route on its router:

- CSRF (on in `prod`) rejects a `POST` without a token. Every gRPC call
  is a `POST`.
- Error pages, compression and body limits can change a gRPC response.

## Decision

Run tonic on its own listener (default `0.0.0.0:50051`). Autumn HTTP
middleware does not touch gRPC traffic.

## Consequences

- Two ports: firewalls and service definitions need both.
- gRPC gets its own HTTP/2 settings (keepalive, streams, connection age).
- The plugin must do its own lifecycle, health and metrics.
- ADR 0008 adds an optional shared-port mode. The dedicated listener
  stays the default.
