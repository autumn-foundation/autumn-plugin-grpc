# ADR 0003 — Lifecycle as a finite state machine, checked by exhaustive tests

- Status: accepted
- Date: 2026-09-28

## Context

The project standard asks for a Verus spec and proof for critical
invariants. Verus is not available in the build environment, and CI
cannot download it from this repository's setup.

The lifecycle has 5 states and 5 events: 25 pairs.

## Decision

- `Lifecycle::next` is a pure, total, `const fn` transition function.
- `LifecycleCell` applies it with compare-and-swap. No other code writes
  the state.
- `tests/lifecycle.rs` holds the spec as a table and checks all 25 pairs.
  For a finite machine this is a complete model check.
- Property tests check the invariants over random event sequences:
  terminal states absorb, progress is monotonic, `Serving` comes only from
  `Idle` + `Bound`, and the cell agrees with the pure function.

## Consequences

- No machine-checked proof ships. Add a Verus proof when Verus is in CI.
