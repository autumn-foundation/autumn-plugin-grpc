# CLAUDE.md — autumn-plugin-grpc

gRPC plugin for the Autumn web framework, built on tonic 0.14. One crate:
`autumn-plugin-grpc`.

## Commands

- Format: `cargo fmt --all` (CI: `--check`)
- Lint: `cargo clippy --all-targets --features tls -- -D warnings`
  (pedantic + nursery are on in `Cargo.toml`). Also run it with no features.
- Test: `cargo test` and `cargo test --features tls`
- Coverage: `cargo llvm-cov --features tls --fail-under-lines 85`
- Regenerate test code from `proto/echo.proto`:
  `UPDATE_GENERATED=1 cargo test --test codegen`
- Example: `cargo run --example echo`
- Pre-commit hook: `git config core.hooksPath .githooks`

No `protoc` is necessary. `tests/codegen.rs` uses `protox`.

## Layout

See `docs/architecture.md`. `plugin.rs` (builder, `Plugin`, route
assembly), `config.rs`, `server.rs` (listener, `GrpcHandle`, drain),
`lifecycle.rs`, `metrics.rs`, `health.rs`, `tls.rs`, `error.rs`.

## Rules

- **State changes only through `LifecycleCell::apply`.** Change the spec
  table in `tests/lifecycle.rs` first.
- **User layers wrap user services only.** Add plugin services after the
  user layers in `build_routes`.
- **Metric labels stay bounded.** New labels need a bound and a test in
  `tests/metrics.rs`. Names must not start with `autumn_`.
- **Config:** a new key goes in `config.rs` with a safe default, a doc
  comment, validation if a value can fail, and a line in the README TOML
  block. Env overrides come from the leaf keys. Use `0`/`""` for "unset",
  not `Option` (env derivation needs a leaf).
- **Startup errors abort boot.** Return `GrpcError` from the startup hook.
  Never fall back silently (for example, to plain text).
- No `unwrap`/`expect`/`panic!` in library code. Tests may use them.
- No behavior without a test. Record decisions in `docs/adr/NNNN-*.md`.
- Docs and comments: short, ASD-STE100 style (simple words, active voice,
  short sentences).

## Autumn API notes (0.7.0)

- `AppBuilder::run` panics with no typed routes. Plugin routers do not count.
- `TestApp` runs startup hooks (in a thread, with `block_on`), not shutdown
  hooks. Tests call `GrpcHandle::shutdown` themselves. Use
  `#[tokio::test(flavor = "multi_thread")]`.
- Autumn runs shutdown hooks after the HTTP drain, inside
  `server.shutdown_timeout_secs`.
- Health details show only with `[health] detailed = true`.
