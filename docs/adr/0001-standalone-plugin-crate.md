# ADR 0001 — Standalone plugin crate on tonic 0.14

- Status: accepted
- Date: 2026-09-28

## Context

Autumn plugins are crates with a `<Name>Plugin` type. Third-party crates
use the name `autumn-plugin-<name>`. tonic is the standard Rust gRPC stack.
tonic 0.14 uses axum 0.8 and hyper 1, the same versions as Autumn 0.7.

## Decision

- Publish `autumn-plugin-grpc` with `GrpcPlugin`.
- Use tonic 0.14 with `default-features = false` and features `server`,
  `router`, `codegen`. Use `tonic-health` and `tonic-reflection`.
- Depend on `autumn-web = "0.7"` with `default-features = false`.
- Re-export `tonic`, `tonic_health` and `tonic_reflection`.

## Consequences

- Apps must use tonic 0.14 for their generated code.
- A tonic major release needs a plugin release.
