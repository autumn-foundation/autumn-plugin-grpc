# ADR 0005 — Configuration layering and strictness

- Status: accepted
- Date: 2026-09-28

## Context

Autumn 0.7 has no public API that gives a plugin its merged section. If
the plugin reads only the base file, `[profile.prod.grpc]` has no effect.

## Decision

- Merge like core: `autumn.toml` `[grpc]`, `[profile.<name>.grpc]`,
  `autumn-<profile>.toml`, then `AUTUMN_<SECTION>__*` env variables.
- `deny_unknown_fields`, and `validate()` for values. An error stops boot
  from the startup hook.
- A bad env override stops boot. Core logs and ignores it, but a bad
  value must not leave a weaker file value in place (for example
  `AUTUMN_GRPC__REFLECTION=0` must not keep `reflection = true`).
- An unreadable `.env` file stops boot.
- `[grpc.tls]` without the `tls` feature stops boot. No silent plain text.
- `server.shutdown_timeout_secs` caps the grace period, because Autumn
  runs plugin hooks inside that budget. The cap keeps 1 s (or half the
  budget, if less) to close killed connections.

## Consequences

- The merge code copies core logic. If core changes its layers, update
  `src/config.rs` and `tests/config.rs`.
