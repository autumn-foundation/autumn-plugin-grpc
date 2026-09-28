# ADR 0004 — Bounded metric labels

- Status: accepted
- Date: 2026-09-28

## Context

The request path sets the `grpc_service` and `grpc_method` labels. A
client can send any path. Unbounded labels exhaust memory in the process
and in Prometheus. Autumn's `MetricsSource` has counters and gauges only.

## Decision

- Only registered services (user, health, reflection) keep their name.
  Other services are `unknown`.
- A call that returns `UNIMPLEMENTED` has method `unknown`.
- `max_metric_series` (default 1000) caps label sets. Extra calls count
  under `other`.
- Latency is a `_sum` and `_count` counter pair, not a histogram.
- Names start with `grpc_server_`. Autumn drops names that start with
  `autumn_`.
- The layer reads `grpc-status` from trailers, or from headers for
  trailers-only responses. A body that ends with no status is `UNKNOWN`.
  A body dropped before its end is `CANCELLED`.

## Consequences

- No latency buckets. Use tracing spans for per-call detail.
