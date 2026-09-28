# ADR 0007 — Drain in a task, health follows readiness

- Status: accepted
- Date: 2026-09-28

## Context

The correctness review found two problems:

- Autumn drops a shutdown hook future when its time budget ends. The
  first version ran the drain inside that future. A drop left the state
  in `Draining` for good, and a later `shutdown()` waited forever.
- Autumn runs plugin hooks after its HTTP drain. gRPC health reported
  `SERVING` until then, so load balancers had no time to react.

## Decision

- `GrpcHandle::shutdown` moves the state to `Draining` and spawns the
  drain as a task. Each caller waits for the `stopped` signal. A dropped
  caller does not stop the drain.
- The drain sets `NOT_SERVING`, then clears the statuses. This ends open
  health `Watch` streams, so they do not hold the drain open.
- A task follows Autumn readiness (`probes().is_shutting_down()`). When
  Autumn starts to shut down, or an operator drains it, health reports
  `NOT_SERVING` at once. It reports `SERVING` again if readiness comes
  back. The server still runs calls until the hook drains it.
- A drop guard in the server task applies `ServerExited`, also on a panic.
- A start failure moves the state to `Failed` and releases waiters.

## Consequences

- Health can say `NOT_SERVING` while calls still run. That is the purpose.
- The readiness task reads a flag each 250 ms.
