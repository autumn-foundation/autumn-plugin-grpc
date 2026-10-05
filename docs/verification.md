# Verification

This file maps each acceptance criterion (AC) in [plan.md](plan.md),
[plan-shared-listener.md](plan-shared-listener.md) and
[plan-client.md](plan-client.md) to its evidence. Test names are
`file::test`. All tests run in CI on Linux, macOS and Windows, with no
features, with `tls`, with `multiplex`, with `tls,multiplex`, with
`client` and with `tls,multiplex,client`.

## Acceptance criteria

| AC | Criterion (short) | Evidence | Verdict |
|---|---|---|---|
| AC1 | `add_service` serves any tonic service on a dedicated HTTP/2 listener | `serve::serves_a_unary_call_on_the_dedicated_listener`, `serve::serves_a_server_stream`, `serve::serves_several_user_services`, `serve::an_unknown_method_is_unimplemented`. Manual run of `examples/echo.rs` with a tonic client (below). Default address: `config::defaults_are_production_safe` | Met |
| AC2 | `[grpc]` config, profile layers, `AUTUMN_GRPC__*`, code on top, bad config stops boot | `config::each_layer_beats_the_one_before`, `config::resolves_profiles_files_and_environment_in_order`, `config::profile_aliases_and_names_resolve`, `config::nested_env_overrides_apply`, `config::a_custom_section_uses_its_own_env_prefix`, `config::code_overrides_apply_on_top_of_the_config`, `config::unknown_keys_are_rejected`, `config::bad_values_are_rejected`, `config::a_bad_env_override_stops_boot`, `config::an_invalid_override_is_reported_and_aborts_boot`, `config::an_unreadable_file_is_an_error` | Met |
| AC3 | `add_service_with(\|state\| …)`; `AppState` in each request | `serve::add_service_with_builds_the_service_from_app_state`, `serve::handlers_read_app_state_from_request_extensions`, `serve::the_handle_is_published_in_app_state` | Met |
| AC4 | Health service on by default: SERVING after start, NOT_SERVING at shutdown | `health::reports_serving_for_the_server_and_each_service`, `health::watch_sees_not_serving_when_shutdown_starts` (strict), `health::the_health_service_can_be_turned_off`, `health::the_reporter_lets_the_app_mark_a_service_down`, `shutdown::health_follows_autumn_readiness` | Met |
| AC5 | Reflection v1 + v1alpha; `auto` = on in dev/test only | `reflection::development_lists_user_health_and_reflection_services`, `reflection::v1alpha_is_served_too`, `reflection::production_turns_reflection_off_by_default`, `reflection::production_can_force_reflection_on`, `reflection::a_corrupt_descriptor_set_aborts_boot`, `config::profile_aliases_and_names_resolve` (`test` is development) | Met |
| AC6 | `grpc` indicator in `/actuator/health`; bounded `grpc_server_*` metrics | `health::autumn_actuator_health_includes_the_grpc_indicator`, `shutdown::draining_refuses_new_connections_and_reports_down` (DOWN, `up` = 0), `metrics::counts_calls_by_service_method_and_code`, `metrics::the_in_flight_gauge_counts_open_calls`, `metrics::unknown_paths_collapse_into_one_series`, `metrics::unauthenticated_random_methods_do_not_use_the_label_budget`, `metrics::series_are_capped`, `metrics::the_metrics_source_is_registered_with_autumn`, `serve::two_servers_run_side_by_side` (`server` label) | Met |
| AC7 | Shutdown: NOT_SERVING, no new connections, drain for the grace, then close; idempotent | `shutdown::drains_an_in_flight_stream_before_it_stops`, `shutdown::draining_refuses_new_connections_and_reports_down`, `shutdown::aborts_calls_that_outlive_the_grace_period`, `shutdown::refuses_new_connections_after_shutdown`, `shutdown::shutdown_is_idempotent_and_concurrent_safe`, `shutdown::a_dropped_shutdown_future_does_not_stop_the_drain`, `shutdown::the_grace_fits_inside_the_autumn_shutdown_budget`. Manual SIGINT run of the example: clean exit | Met |
| AC8 | A bind failure stops boot | `serve::a_bind_failure_aborts_boot` (state `Failed`, message has the address) | Met |
| AC9 | Transport settings | `transport::the_server_announces_max_concurrent_streams`, `transport::the_default_stream_limit_is_on`, `transport::max_connection_age_closes_old_connections`, `transport::the_concurrency_limit_queues_calls_on_one_connection`, `transport::without_a_limit_calls_run_side_by_side`, `config::transport_settings_apply_to_the_server` (timeout), `serve::the_connection_limit_holds_new_connections`. TCP nodelay/keepalive, HTTP/2 keepalive and the reset limit: set in `server::start`; no behavior test (see gaps) | Met, with gaps noted |
| AC10 | `tls` feature: TLS and mTLS; TLS paths without the feature stop boot | `tls::serves_over_tls`, `tls::mtls_requires_a_client_certificate`, `tls::optional_mtls_accepts_both_kinds_of_client`, `tls::a_missing_certificate_file_aborts_boot`, `tls::a_missing_client_ca_file_aborts_boot`, `config::tls_paths_without_the_feature_abort_boot` | Met |
| AC11 | `.layer()` wraps user services, not health or reflection | `serve::a_tower_layer_wraps_user_services`, `serve::an_interceptor_guards_user_services_but_not_health`, `serve::the_last_layer_is_the_outermost_and_reflection_is_not_wrapped` | Met |
| AC12 | Lifecycle state machine: all transitions tested, invariants property-tested | `lifecycle::every_state_event_pair_matches_the_spec` (25 pairs), `lifecycle::progress_is_monotonic_and_terminals_absorb`, `lifecycle::serving_is_reached_only_through_bound`, `lifecycle::the_cell_agrees_with_the_pure_function`, `server::tests::*` (task exit and panic) | Met (tests, not Verus: ADR 0003) |
| AC13 | Autumn plugin conformance; config section; stable unique `name()` | `conformance::passes_the_framework_conformance_harness`, `conformance::declarations_follow_the_configuration`, `conformance::declares_its_config_section`, `conformance::the_name_is_keyed_by_config_section`, `serve::a_duplicate_plugin_is_skipped_at_boot` | Met |
| AC14 | Quality gates and docs | See below | Met |

## AC14 details

| Gate | Evidence |
|---|---|
| `cargo fmt` | CI job "Format"; `.githooks/pre-commit` |
| clippy pedantic + nursery, `-D warnings` | CI jobs "Clippy ()", "Clippy (tls)", "Clippy (multiplex)", "Clippy (tls,multiplex)" |
| No `unwrap`/`expect`/`panic!` in library code | `Cargo.toml` `[lints.clippy]` + `-D warnings`. Test modules allow them explicitly |
| Line coverage ≥ 85 % | CI job "Coverage": `cargo llvm-cov --features tls,multiplex --fail-under-lines 85` |
| MSRV 1.88 | CI job "MSRV (1.88)" |
| Cross-platform | CI test matrix: Ubuntu, macOS, Windows |
| Docs | `README.md`, `CLAUDE.md`, `CHANGELOG.md`, `docs/architecture.md` (Mermaid), `docs/adr/0001`–`0009`, `examples/echo.rs` |
| Generated code is fresh | `codegen::generated_code_is_fresh` (`.gitattributes` keeps LF on Windows) |

## Shared listener (issue #2)

| AC | Criterion (short) | Evidence | Verdict |
|---|---|---|---|
| S1 | `listener` key, default `dedicated`, env override, bad value stops boot | `config::the_listener_defaults_to_dedicated`, `config::shared_mode_parses_from_files_and_env` | Met |
| S2 | `multiplex` feature; shared mode without it stops boot | `config::shared_mode_without_the_multiplex_feature_stops_boot` (boots, checks the message and `Failed`) | Met |
| S3 | No own port; h2c gRPC on Autumn's port; `local_addr()` is `None` | `shared::a_grpc_call_reaches_the_services_on_the_http_port` | Met |
| S4 | gRPC skips HTTP middleware (CSRF, request timeout) | `shared::grpc_skips_csrf_and_the_request_timeout` (a 500 ms unary call with a 200 ms request timeout; the same timeout ends `/slow`) | Met |
| S5 | HTTP routes do not change; HTTP/1.1 gRPC and grpc-web stay on HTTP | `shared::http_routes_answer_over_http1_and_http2`, `shared::grpc_over_http1_and_grpc_web_stay_on_http`, `gate::tests::only_http2_grpc_content_types_are_grpc` | Met |
| S6 | Guards, `AppState`, metrics, health, reflection, timeouts, duplicate check, route listing, `remote_addr` | `shared::guards_health_reflection_and_metrics_work`, `shared::the_server_timeout_applies`, `shared::the_grpc_timeout_header_applies` (raw h2, so the client does not enforce it), `gate::tests::grpc_timeout_values_parse_as_in_the_spec`, `gate::tests::the_deadline_is_the_smaller_timeout`, `shared::remote_addr_is_the_peer_address`, `gate::tests::the_peer_is_the_tcp_peer_but_not_the_unix_socket_stamp`, `shared::a_duplicate_service_stops_boot_in_shared_mode`, `shared::the_health_indicator_names_the_shared_listener` | Met |
| S7 | Lifecycle, readiness, drain, `UNAVAILABLE` for new and ended calls, call before start | `shared::autumn_shutdown_drains_grpc_so_the_http_drain_can_end`, `shared::health_follows_autumn_readiness_in_shared_mode`, `shared::shutdown_drains_in_flight_calls_and_refuses_new_ones`, `shared::the_grace_period_ends_calls_that_run_too_long`, `gate::tests::a_call_before_start_is_unavailable` | Met |
| S8 | Boot errors: Autumn TLS on 0.7, `[grpc.tls]`, a second shared plugin | `shared::autumn_tls_stops_boot_in_shared_mode`, `config::grpc_tls_is_an_error_in_shared_mode`, `shared::a_second_shared_plugin_stops_boot`, `shared::a_dedicated_and_a_shared_plugin_run_side_by_side` | Met |
| S9 | One warning for dedicated-only settings | `config::tests::dedicated_only_settings_names_each_changed_key` (all 11 keys), `plugin::tests::shared_mode_warns_once_for_ignored_settings`, `plugin::tests::shared_mode_warns_for_reflection_off_loopback` | Met (log output not read; see gaps) |
| S10 | Docs, CI, quality gates | README "Share Autumn's port", ADR 0008, `docs/architecture.md`, CHANGELOG, CI matrix with `multiplex` | Met |

Mutation checks: each change below made its test fail. The changes were:
no HTTP/2 check, no deadline, `grpc-timeout` ignored, no body kill, a kill
that resets the stream, no state check, and no drain on Autumn's shutdown
signal.

### Shared listener review

| Angle | Finding | Result |
|---|---|---|
| Security, correctness | An open stream (for example a health `Watch`) keeps Autumn's HTTP drain open. Autumn runs the hook only after that drain, and its watchdog does not count gRPC. Shutdown hangs until SIGKILL | Fixed: the drain starts on `AppState::shutdown_token`. `shared::autumn_shutdown_drains_grpc_so_the_http_drain_can_end` |
| Correctness | An ended stream got RST (`INTERNAL`), not `UNAVAILABLE` | Fixed: `UNAVAILABLE` trailers. `shared::the_grace_period_ends_calls_that_run_too_long` |
| Security | A Unix socket gives `127.0.0.1:0` for every caller as `remote_addr` | Fixed: not reported. `gate::tests::the_peer_is_the_tcp_peer_but_not_the_unix_socket_stamp` |
| Security | README did not list all skipped HTTP protections | Fixed |
| Security | Reflection warning for `localhost` | Fixed. `plugin::tests::shared_mode_warns_for_reflection_off_loopback` |
| Security | A client that stops reading cannot be ended | Documented (ADR 0008, gaps) |
| Tests | The request-timeout proof used a stream; Autumn's timeout covers only the head | Fixed: a slow unary call |
| Tests | tonic's client enforces `grpc-timeout` itself | Fixed: raw h2 call |
| Tests | No readiness test, no proof that the drain waits, tight time bounds, JSON from a chunked body | Fixed |
| API | `GrpcError::Shared(String)`; public `dedicated_only_settings`; no `listener()` setter; no `Hash` | Fixed: typed variants, `pub(crate)`, setter, `Hash` |
| Correctness | `child_token()` per call | Fixed: `clone()` |
| Correctness | Metrics count a call ended before its head as `CANCELLED`; gate refusals not counted | Documented (ADR 0008) |

## gRPC client (issue #3)

| AC | Criterion (short) | Evidence | Verdict |
|---|---|---|---|
| C1 | Feature `client`; no server-only cost; the server builds without it; `clients` without the feature stops boot | CI "MSRV": `cargo check` (library, no dev-dependency features) and `cargo check --features tls,multiplex,client`. `cargo tree`: `hyper-timeout` (tonic channel) only with `client`. Clippy matrix rows `client`, `tls,client`, `multiplex,client`. `config::clients_without_the_feature_stop_boot` (config error and plugin boot, state `Failed`) | Met |
| C2 | `[grpc.clients.<name>]` keys, same layers, strict validation, env leaves | `config::client_defaults_are_safe`, `config::clients_parse_from_toml`, `config::bad_client_values_are_rejected` (bad URI, scheme, `HTTPS://`, path, query, host, `connect_timeout_ms = 0`, name, unknown key, TLS on http, cert without key and key without cert), `config::https_needs_a_ca_bundle`, `config::client_settings_use_the_same_layers` (file, profile, env, a bad env value), `plugin::tests::a_registered_client_reads_env_overrides_and_resets_the_cache`, `client::https_without_the_tls_feature_stops_boot` | Met |
| C3 | `GrpcClient<T>`; `GrpcClients::get` for two endpoints of one type; missing registration is a 500 and a boot warning | `client::a_handler_calls_the_double_through_the_extractor`, `client::get_by_name_covers_two_endpoints_of_one_type` (also the ambiguous 500), `client::a_missing_registration_is_a_clear_500`, `client::a_wrong_type_and_no_clients_are_500s`, `client::one_name_serves_two_client_types`, `client::app_state_has_the_clients`, `client::two_plugins_share_one_registry_and_one_metrics_source`, boot errors `client::a_client_without_an_endpoint_stops_boot`, `client::a_duplicate_client_stops_boot`, `client::a_double_without_a_client_stops_boot`. Warnings: `client::tests::config_with_no_registration_is_named_with_its_section`, `client::tests::one_type_on_two_names_warns_also_across_plugins` | Met (warning text tested; log output not read) |
| C4 | Lazy connect; a down endpoint does not stop boot or change readiness | `client::a_down_endpoint_does_not_stop_boot_or_change_readiness` (health `UP`, no client part, call 503), `client::a_connect_timeout_is_unavailable_not_a_deadline` | Met |
| C5 | Request ID and `traceparent` downstream; `grpc-timeout` = smaller of client timeout and time left | `client::the_request_id_and_trace_context_go_downstream`, `client::a_bad_traceparent_is_not_forwarded`, `client::tracestate_is_forwarded_up_to_512_bytes`, `client::the_client_timeout_is_sent_as_grpc_timeout`, `client::grpc_timeout_is_the_time_left_when_that_is_smaller`, `client::a_request_timeout_of_zero_is_off`, `client::caller_metadata_wins_and_the_smaller_timeout_applies`, `client::a_slow_call_ends_with_deadline_exceeded_as_504`, `client::channel::tests::a_past_deadline_ends_the_call_before_it_is_sent`, `client::channel::tests::a_call_with_time_left_succeeds`, `client::channel::tests::only_a_cancel_after_the_sent_timeout_is_a_deadline`, `client::channel::tests::a_tonic_timeout_anywhere_in_the_chain_is_a_timeout`, `client::context::tests::*`, `timeout::tests::*` | Met (time left counts from extraction; see gaps) |
| C6 | Fixed code map; no internals in prod | `client::status::tests::every_code_has_a_fixed_status` (all 17 codes), `client::status::tests::or_http_passes_ok_values_through`, `client::statuses_map_to_http_and_hide_details_outside_dev` (prod: no downstream secret anywhere in the body; 4xx fixed text in prod and dev; 5xx details only in dev) | Met |
| C7 | `grpc_client_*` metrics; bounded labels | `client::client_calls_are_counted`, `metrics::client_labels_are_bounded` (bad method `unknown`, cap `other`), `metrics::client_metrics_can_be_turned_off`, `client::metrics::tests::*`, `client::two_plugins_share_one_registry_and_one_metrics_source` (one family) | Met |
| C8 | In-memory test double; no port | Most `client::*` tests use `client_double` with the server off (`local_addr()` is `None`). `client::one_name_serves_two_client_types` (two doubles on one name) | Met |
| C9 | Docs and quality gates | README "Call gRPC services", ADR 0009, `docs/plan-client.md`, `docs/architecture.md`, CHANGELOG, CLAUDE.md, `examples/client.rs`. Gates below. Coverage 95 % lines (all features) | Met |

Mutation checks: each change below made its test fail: `request_timeout_ms = 0` as a zero deadline, the trace pair added one header at a time, and the scheme check from the case-insensitive URI parser. `resolve` with no client names fails the plugin env test.

Manual run: `cargo run --example client --features client`. `GET /say/hello` gives `echo: hello` and an `x-request-id`. `GET /say/fail` gives 400 `invalid argument` (the downstream message stays out). `/actuator/prometheus` shows `grpc_client_*` with codes `OK` and `INVALID_ARGUMENT`.

### Client review

Four review agents read the change: security, correctness, tests, and API and docs.

| Angle | Finding | Result |
|---|---|---|
| Correctness (high) | `request_timeout_ms = 0` means "off" in Autumn, but gave a zero deadline: every call failed with 504 | Fixed. `client::a_request_timeout_of_zero_is_off` |
| Correctness | The request-start `static_gate` made Autumn's idempotency replay fail closed for the whole app | Fixed: no app layer; the time left starts at extraction. ADR 0009 |
| Security | `HTTPS://` passed as non-https: no TLS check, and plain text without the `tls` feature | Fixed: lowercase schemes only. `config::bad_client_values_are_rejected` |
| Security, API, tests | A caller `traceparent` got the incoming `tracestate` of another trace | Fixed: the pair goes only when the caller set neither. `client::context::tests::caller_values_stay_and_the_trace_pair_stays_whole` |
| Correctness | A late `CANCELLED` compared with the unrounded timeout | Fixed: compare with the sent value. `client::channel::tests::only_a_cancel_after_the_sent_timeout_is_a_deadline` |
| Correctness | The shared-type warning did not see other plugins | Fixed. `client::tests::one_type_on_two_names_warns_also_across_plugins` |
| Correctness | Messages named `[grpc.clients.*]` for every section | Fixed: the section is in the text |
| Security | Trace context and request ID go to every endpoint | Documented (README Security, ADR 0009). Autumn trusts incoming trace context too |
| Security | Downstream 401 and 403 reach the HTTP caller | Documented. The issue asks for this map |
| API | `BoxError` and `ResponseBody` public; `GrpcClients::only` public; trait not sealed; `Ambiguous` names in one string; no `#[non_exhaustive]` on variants | Fixed |
| API | `GrpcClients::metric_families` public | Kept: same as `GrpcHandle::metric_families` |
| API, docs | README test double bound port 50051; no `AppState` example; "caller value stays" too broad; `DuplicateClient` text; ASD-STE100 rewrites; CLAUDE.md layout; CI row `multiplex,client` | Fixed |
| Tests | No test for: env override through the plugin, two working plugins, one name with two types, `WrongType`, `NotInstalled`, wrong CA, server-only TLS, `tracestate` limit, metrics off, connect timeout; weak leak check; loop did not check each call; missing `.development(..)` pins | Added or fixed (tests named above) |
| Security, CLAUDE.md | The client startup hook built its own error text | Fixed: uses `startup_error` |
| Correctness (nit) | A `poll_ready` error (closed tonic worker) is not in `grpc_client_*` metrics | Accepted: no call starts; tonic reports it |

## Manual run

`cargo run --example echo`, then a tonic client:

- `Say` without a token: `UNAUTHENTICATED`. With `Bearer demo`: `"echo: hi"`
  (the prefix comes from `AppState`).
- Health `Check`: `SERVING`. Reflection lists the Echo, health and
  reflection services.
- `/actuator/health` shows `grpc: UP` with the address.
  `/actuator/prometheus` shows `grpc_server_*` samples.
- SIGINT: Autumn logs "gRPC server stopped", then exits with code 0.

## Code review

Four review agents read the code. Each covered one angle. Each finding
has a fix and a test, or a reason.

| Angle | Finding | Result |
|---|---|---|
| Security | tonic passes `None` to hyper, so there was no stream limit and no reset-flood limit | Fixed: always set (ADR 0006). `transport::the_default_stream_limit_is_on` |
| Security | No connection cap, no TLS handshake timeout, no keepalive | Fixed: `max_connections`, `tls.handshake_timeout_ms`, keepalive on. `serve::the_connection_limit_holds_new_connections` |
| Security | Health and reflection decode 4 MiB with no guard | Fixed: 16 KiB. `serve::health_rejects_a_large_request` |
| Security | Random method names fill the label budget | Fixed: methods are known from descriptors or an `OK`. `metrics::unauthenticated_random_methods_do_not_use_the_label_budget` |
| Security | Dev listens on all interfaces with reflection on | Fixed: loopback in dev/test. A warning when reflection is on and the listener is not on loopback |
| Security | A bad env override is ignored with a warning | Fixed: boot stops. `config::a_bad_env_override_stops_boot` |
| Security | `client_auth_optional` without a CA is ignored | Fixed: config error. `config::bad_values_are_rejected` |
| Security | Unreadable `.env` is ignored | Fixed: boot stops |
| Correctness | A dropped shutdown future leaves `Draining` for good | Fixed: drain task (ADR 0007). `shutdown::a_dropped_shutdown_future_does_not_stop_the_drain` |
| Correctness | A duplicate service makes axum panic | Fixed: `GrpcError::DuplicateService`. `serve::a_duplicate_service_is_a_boot_error_not_a_panic_in_axum` |
| Correctness | All connections lock one token on each poll | Fixed: a child token for each connection |
| Correctness | Health `Watch` streams hold the drain open | Fixed: clear statuses after `NOT_SERVING`. `health::watch_sees_not_serving_when_shutdown_starts` |
| Correctness | `NOT_SERVING` comes only after the HTTP drain | Fixed: health follows Autumn readiness. `shutdown::health_follows_autumn_readiness` |
| Correctness | Race between start and an early shutdown | Fixed: `GrpcError::NotIdle`. Metrics `up` comes from the lifecycle |
| Correctness | A server task panic leaves `Serving` | Fixed: drop guard. `server::tests::a_panic_in_the_server_task_still_fails_the_lifecycle` |
| Correctness | Metric codes in edge cases; over the cap the code is lost | Fixed: body error and end-of-stream are `UNKNOWN`; the code label stays. `metrics::series_are_capped` |
| Correctness | `shutdown_timeout_secs = 0` skips all hooks | Not fixable in the plugin: Autumn skips the hook. Health still follows readiness |
| Tests | The listener stays open during the drain | Fixed: a task drops it when `stop` fires. `shutdown::draining_refuses_new_connections_and_reports_down` |
| Tests | The watch test could not fail | Fixed: strict asserts |
| Tests | Weak AC1, AC2, AC6, AC9, AC10, AC11 tests | Added the tests named in the table above |
| Tests | Fixed `sleep` before metric asserts | Fixed: `common::settle` waits for `in_flight == 0` |
| Tests | TLS tests use `localhost` | Fixed: `127.0.0.1` with `domain_name("localhost")` |
| Tests | CRLF on Windows breaks the codegen check | Fixed: `.gitattributes` |
| API | Route classification separate from the guard; health shown as public | Fixed: `guard`, `guard_interceptor`, `public`; health and reflection are `framework`; default `unclassified` |
| API | Two servers: duplicate metric families; one `GrpcHandle` in `AppState` | Fixed: one metrics source with a `server` label; `GrpcServers` |
| API | Public items that must not be public; semver hazards | Fixed: `env_prefix` and `Resolved` fields private; `ConfigError::message()`; `#[non_exhaustive]` on config and lifecycle types; `BindFailed` renamed `StartFailed`; `service_names()` returns a slice |
| API | No `plugin-contract` feature | Not done: it needs Autumn `main`. Follow-up |
| Docs | ASD-STE100 violations and doc/code mismatches | Fixed in README, ADRs, architecture, plan and doc comments |

## Known gaps

- TCP nodelay and keepalive, HTTP/2 keepalive and the local reset limit
  have no behavior test. The code sets them in `server::start`. The
  config tests check their values.
- No Verus proof (ADR 0003).
- No `plugin-contract` feature. It needs an Autumn release after 0.7.0.
- gRPC-Web and compression switches are out of scope (plan, blue hat).
- Shared mode: a client that stops reading a stream keeps it open until
  Autumn ends the process (hyper polls a body only with flow-control
  window). ADR 0008.
- Shared mode: the AC9 warning text is unit-tested
  (`plugin::tests::shared_mode_warns_*`). No test reads the log output.
- Shared mode adds a `static_gate` (`GrpcGate`). Autumn then makes
  idempotency replay fail closed for the whole app. This is older than
  issue #3 and needs an Autumn API to mark a gate transparent. Follow-up.
- Client: the time left counts from extraction, not from the request
  start. Autumn publishes neither the start nor per-route `timeout`
  values (ADR 0009).
- Client: boot warnings are unit-tested. No test reads the log output.
