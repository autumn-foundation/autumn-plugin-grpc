# CLAUDE.md — autumn-plugin-grpc

gRPC plugin for the Autumn web framework, built on tonic 0.14. One crate:
`autumn-plugin-grpc`.

## Commands

- Format: `cargo fmt --all` (CI: `--check`)
- Lint: `cargo clippy --all-targets --features tls,multiplex -- -D warnings`
  (pedantic + nursery are on in `Cargo.toml`). Also run it with no features.
- Test: `cargo test` and `cargo test --features tls,multiplex`
- Coverage: `cargo llvm-cov --features tls,multiplex --fail-under-lines 85`
- Regenerate test code from `proto/echo.proto`:
  `UPDATE_GENERATED=1 cargo test --test codegen`
- Example: `cargo run --example echo`
- Pre-commit hook: `git config core.hooksPath .githooks`

No `protoc` is necessary. `tests/codegen.rs` uses `protox`.

## Layout

See `docs/architecture.md`. `plugin.rs` (builder, `Plugin`, route
assembly), `config.rs`, `server.rs` (listener, `GrpcHandle`, drain),
`lifecycle.rs`, `metrics.rs`, `registry.rs` (`GrpcServers`, the one
metrics source), `gate.rs` (shared listener, ADR 0008), `health.rs`,
`tls.rs`, `error.rs`.

## Rules

- **State changes only through `LifecycleCell::apply`.** Change the spec
  table in `tests/lifecycle.rs` first.
- **User layers wrap user services only.** Add plugin services after the
  user layers in `build_routes`. Auth goes through `guard*`, so
  `autumn routes` shows the service as gated.
- **Shared mode dispatches HTTP/2 `application/grpc*` only.** Other
  requests must reach Autumn's middleware (CSRF). Tests: `tests/shared.rs`.
- **Shared mode drains on Autumn's shutdown signal**, not only in the
  hook. Autumn waits for all streams before hooks run (ADR 0008).
- **Set every hyper limit explicitly** on the dedicated listener. tonic passes `None`, and `None`
  removes the hyper default (ADR 0006).
- **Shutdown work runs in the drain task**, not in the caller. Autumn can
  drop the hook future (ADR 0007).
- **Metric labels stay bounded.** A method label needs a descriptor or an
  `OK` response. New labels need a bound and a test in `tests/metrics.rs`.
  Names must not start with `autumn_`.
- **Config:** a new key goes in `config.rs` with a safe default, a doc
  comment, validation if a value can fail, and a line in the README TOML
  block. Env overrides come from the leaf keys. Use `0`/`""` for "unset",
  not `Option` (env derivation needs a leaf).
- **Startup errors abort boot.** Return `GrpcError` from the startup hook.
  Never fall back silently (for example, to plain text). A bad env
  override is an error too.
- **Tests:** pin `.development(..)` on each plugin that boots. Wait with
  `common::settle` or `common::eventually`, not a fixed sleep.
- No `unwrap`/`expect`/`panic!` in library code. Tests may use them.
- No behavior without a test. Record decisions in `docs/adr/NNNN-*.md`.
- Docs and comments: short, ASD-STE100 style (simple words, active voice,
  short sentences).

## Autumn API notes (0.8.0)

- `AppBuilder::run` panics with no typed routes. Plugin routers do not count.
- `TestApp` runs startup hooks (in a thread, with `block_on`), not shutdown
  hooks. Tests call `GrpcHandle::shutdown` themselves. Use
  `#[tokio::test(flavor = "multi_thread")]`.
- Autumn runs shutdown hooks after the HTTP drain, inside
  `server.shutdown_timeout_secs`.
- Health details show only with `[health] detailed = true`.
