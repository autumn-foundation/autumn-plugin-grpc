//! Configuration: AC2, AC9 and AC10 (TLS without the feature).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use autumn_plugin_grpc::{GrpcConfig, GrpcPlugin, Toggle};
use autumn_web::config::MockEnv;

fn temp_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("autumn-grpc-config-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn env_for(dir: &std::path::Path) -> MockEnv {
    MockEnv::new().with("AUTUMN_MANIFEST_DIR", dir.to_str().unwrap())
}

#[test]
fn defaults_are_production_safe() {
    let config = GrpcConfig::default();
    assert!(config.enabled);
    assert_eq!(config.bind, "", "the profile decides");
    assert_eq!(
        config.bind_addr(true).unwrap().to_string(),
        "127.0.0.1:50051"
    );
    assert_eq!(
        config.bind_addr(false).unwrap().to_string(),
        "0.0.0.0:50051"
    );
    assert_eq!(config.max_concurrent_streams, 200);
    assert_eq!(config.http2_max_local_error_reset_streams, 1024);
    assert_eq!(config.max_connections, 1000);
    assert_eq!(
        config.http2_keepalive_interval(),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        config.http2_keepalive_timeout(),
        Some(Duration::from_secs(20))
    );
    assert_eq!(config.tls.handshake_timeout(), Duration::from_secs(10));
    assert!(config.health);
    assert_eq!(config.reflection, Toggle::Auto);
    assert!(config.metrics);
    assert_eq!(config.shutdown_grace_ms, 10_000);
    assert!(config.max_metric_series > 0);
    assert!(config.tls.cert_path.is_empty());
    config.validate().unwrap();
}

#[test]
fn reads_the_section_from_toml() {
    let text = r#"
        [grpc]
        bind = "127.0.0.1:6000"
        reflection = false
        timeout_ms = 1500
        concurrency_limit_per_connection = 32
        max_concurrent_streams = 64
        tcp_nodelay = false
        tcp_keepalive_ms = 30000
        http2_keepalive_interval_ms = 10000
        http2_keepalive_timeout_ms = 5000
        max_connection_age_ms = 600000
    "#;
    let config = GrpcConfig::from_toml_str(text, "grpc").unwrap();
    assert_eq!(config.bind, "127.0.0.1:6000");
    assert_eq!(config.reflection, Toggle::Off);
    assert_eq!(config.timeout(), Some(Duration::from_millis(1500)));
    assert_eq!(config.concurrency_limit_per_connection, 32);
    assert_eq!(config.max_concurrent_streams, 64);
    assert!(!config.tcp_nodelay);
    assert_eq!(config.tcp_keepalive(), Some(Duration::from_secs(30)));
    assert_eq!(
        config.http2_keepalive_interval(),
        Some(Duration::from_secs(10))
    );
    assert_eq!(
        config.http2_keepalive_timeout(),
        Some(Duration::from_secs(5))
    );
    assert_eq!(config.max_connection_age(), Some(Duration::from_secs(600)));
}

#[test]
fn zero_means_unset_for_durations() {
    let config = GrpcConfig::default();
    assert_eq!(config.timeout(), None);
    assert_eq!(config.max_connection_age(), None);
    assert_eq!(config.tcp_keepalive(), None);
}

#[test]
fn unknown_keys_are_rejected() {
    let error = GrpcConfig::from_toml_str("[grpc]\nbindd = \"x\"", "grpc").unwrap_err();
    assert!(error.to_string().contains("bindd"), "{error}");
}

#[test]
fn bad_values_are_rejected() {
    for (text, needle) in [
        ("[grpc]\nbind = \"not an address\"", "bind"),
        ("[grpc]\nshutdown_grace_ms = 0", "shutdown_grace_ms"),
        ("[grpc]\nmax_metric_series = 0", "max_metric_series"),
        (
            "[grpc]\nmax_concurrent_streams = 0",
            "max_concurrent_streams",
        ),
        (
            "[grpc]\nhttp2_max_local_error_reset_streams = 0",
            "http2_max_local_error_reset_streams",
        ),
        (
            "[grpc.tls]\nhandshake_timeout_ms = 0",
            "handshake_timeout_ms",
        ),
        ("[grpc.tls]\nclient_auth_optional = true", "client_ca_path"),
        ("[grpc]\nreflection = \"sometimes\"", "sometimes"),
        ("[grpc.tls]\ncert_path = \"a.pem\"", "key_path"),
        ("[grpc.tls]\nclient_ca_path = \"ca.pem\"", "cert_path"),
    ] {
        let error = GrpcConfig::from_toml_str(text, "grpc").unwrap_err();
        assert!(error.to_string().contains(needle), "{text}: {error}");
    }
}

#[test]
fn toggles_accept_booleans_and_words() {
    for (raw, expected) in [
        ("true", Toggle::On),
        ("false", Toggle::Off),
        ("\"auto\"", Toggle::Auto),
        ("\"on\"", Toggle::On),
        ("\"off\"", Toggle::Off),
    ] {
        let config =
            GrpcConfig::from_toml_str(&format!("[grpc]\nreflection = {raw}"), "grpc").unwrap();
        assert_eq!(config.reflection, expected, "{raw}");
    }
    assert!(Toggle::Auto.resolve(true));
    assert!(!Toggle::Auto.resolve(false));
    assert!(Toggle::On.resolve(false));
    assert!(!Toggle::Off.resolve(true));
}

#[test]
fn resolves_profiles_files_and_environment_in_order() {
    let dir = temp_dir("layers");
    std::fs::write(
        dir.join("autumn.toml"),
        r#"
        [grpc]
        bind = "127.0.0.1:1000"
        timeout_ms = 100
        shutdown_grace_ms = 111

        [profile.prod.grpc]
        timeout_ms = 200
        "#,
    )
    .unwrap();
    std::fs::write(
        dir.join("autumn-prod.toml"),
        "[grpc]\nshutdown_grace_ms = 333\n",
    )
    .unwrap();

    let env = env_for(&dir)
        .with("AUTUMN_ENV", "prod")
        .with("AUTUMN_GRPC__BIND", "127.0.0.1:2000");
    let resolved = GrpcConfig::resolve_with_env("grpc", &env).unwrap();
    assert_eq!(resolved.profile(), "prod");
    assert!(!resolved.is_development());
    assert_eq!(resolved.config().bind, "127.0.0.1:2000", "env wins");
    assert_eq!(resolved.config().timeout_ms, 200, "inline profile applies");
    assert_eq!(
        resolved.config().shutdown_grace_ms,
        333,
        "profile file applies"
    );

    let dev = GrpcConfig::resolve_with_env("grpc", &env_for(&dir)).unwrap();
    assert!(dev.is_development());
    assert_eq!(dev.config().timeout_ms, 100);
    assert_eq!(dev.config().shutdown_grace_ms, 111);
}

#[test]
fn nested_env_overrides_apply() {
    let dir = temp_dir("env");
    let env = env_for(&dir)
        .with("AUTUMN_GRPC__TLS__CERT_PATH", "cert.pem")
        .with("AUTUMN_GRPC__TLS__KEY_PATH", "key.pem")
        .with("AUTUMN_GRPC__REFLECTION", "true");
    let resolved = GrpcConfig::resolve_with_env("grpc", &env).unwrap();
    assert_eq!(resolved.config().tls.cert_path, "cert.pem");
    assert_eq!(resolved.config().tls.key_path, "key.pem");
    assert_eq!(resolved.config().reflection, Toggle::On);
}

#[test]
fn a_bad_env_override_stops_boot() {
    let dir = temp_dir("bad-env");
    std::fs::write(dir.join("autumn.toml"), "[grpc]\nreflection = true\n").unwrap();
    for (key, value) in [
        ("AUTUMN_GRPC__TIMEOUT_MS", "not-a-number"),
        // `0` is an integer, not a toggle: the file value must not survive.
        ("AUTUMN_GRPC__REFLECTION", "0"),
        ("AUTUMN_GRPC__TLS__CLIENT_AUTH_OPTIONAL", "False"),
    ] {
        let env = env_for(&dir).with(key, value);
        let error = GrpcConfig::resolve_with_env("grpc", &env).unwrap_err();
        assert!(error.message().contains(key), "{key}: {error}");
    }
}

#[test]
fn a_custom_section_uses_its_own_env_prefix() {
    let dir = temp_dir("section");
    std::fs::write(
        dir.join("autumn.toml"),
        "[grpc_admin]\nbind = \"127.0.0.1:7000\"\n",
    )
    .unwrap();
    let env = env_for(&dir).with("AUTUMN_GRPC_ADMIN__TIMEOUT_MS", "9");
    let resolved = GrpcConfig::resolve_with_env("grpc_admin", &env).unwrap();
    assert_eq!(resolved.config().bind, "127.0.0.1:7000");
    assert_eq!(resolved.config().timeout_ms, 9);
}

#[test]
fn an_unreadable_file_is_an_error() {
    let dir = temp_dir("broken");
    std::fs::write(dir.join("autumn.toml"), "[grpc\nbroken").unwrap();
    let error = GrpcConfig::resolve_with_env("grpc", &env_for(&dir)).unwrap_err();
    assert!(error.to_string().contains("autumn.toml"), "{error}");
}

#[test]
fn a_non_table_section_is_an_error() {
    let dir = temp_dir("scalar");
    std::fs::write(dir.join("autumn.toml"), "grpc = 5\n").unwrap();
    let error = GrpcConfig::resolve_with_env("grpc", &env_for(&dir)).unwrap_err();
    assert!(error.to_string().contains("table"), "{error}");
}

#[test]
fn code_overrides_apply_on_top_of_the_config() {
    let plugin = GrpcPlugin::new()
        .config(common::local_config())
        .bind("127.0.0.1:4321")
        .configure(|c| c.timeout_ms = 5);
    let config = plugin.effective_config().unwrap();
    assert_eq!(config.bind, "127.0.0.1:4321");
    assert_eq!(config.timeout_ms, 5);
}

#[test]
fn an_invalid_override_is_reported_and_aborts_boot() {
    let plugin = GrpcPlugin::new()
        .config(common::local_config())
        .configure(|c| c.shutdown_grace_ms = 0);
    let error = plugin.effective_config().unwrap_err();
    assert!(error.to_string().contains("shutdown_grace_ms"));
    let outcome = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            let _ = autumn_web::test::TestApp::new().plugin(plugin).build();
        }))
    })
    .join()
    .unwrap();
    assert!(outcome.is_err());
}

#[cfg(not(feature = "tls"))]
#[test]
fn tls_paths_without_the_feature_abort_boot() {
    let plugin = common::echo_plugin().configure(|c| {
        c.tls.cert_path = "cert.pem".to_owned();
        c.tls.key_path = "key.pem".to_owned();
    });
    let handle = plugin.handle();
    let outcome = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            let _ = autumn_web::test::TestApp::new().plugin(plugin).build();
        }))
    })
    .join()
    .unwrap();
    assert!(outcome.is_err(), "plain text must not replace TLS silently");
    assert!(handle.local_addr().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn transport_settings_apply_to_the_server() {
    let plugin = common::echo_plugin().configure(|c| {
        c.timeout_ms = 100;
        c.concurrency_limit_per_connection = 8;
        c.max_concurrent_streams = 16;
        c.tcp_keepalive_ms = 10_000;
        c.http2_keepalive_interval_ms = 10_000;
        c.http2_keepalive_timeout_ms = 5_000;
        c.max_connection_age_ms = 60_000;
    });
    let (_http, handle) = common::boot(plugin);
    let mut client = common::EchoClient::new(common::channel(&handle).await);
    let reply = client
        .say(common::pb::SayRequest {
            message: "tuned".into(),
        })
        .await
        .unwrap();
    assert_eq!(reply.into_inner().message, "tuned");

    // "slow" sleeps longer than the 100 ms server timeout.
    let status = client
        .say(common::pb::SayRequest {
            message: "slow".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            status.code(),
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
        ),
        "{status:?}"
    );
    handle.shutdown().await;
}

#[test]
fn each_layer_beats_the_one_before() {
    let dir = temp_dir("precedence");
    std::fs::write(
        dir.join("autumn.toml"),
        "[grpc]\ntimeout_ms = 1\n[profile.prod.grpc]\ntimeout_ms = 2\n",
    )
    .unwrap();
    let prod = || env_for(&dir).with("AUTUMN_ENV", "prod");
    let timeout = |env: &MockEnv| {
        GrpcConfig::resolve_with_env("grpc", env)
            .unwrap()
            .config()
            .timeout_ms
    };
    assert_eq!(timeout(&env_for(&dir)), 1, "base");
    assert_eq!(timeout(&prod()), 2, "inline profile beats base");
    std::fs::write(dir.join("autumn-prod.toml"), "[grpc]\ntimeout_ms = 3\n").unwrap();
    assert_eq!(timeout(&prod()), 3, "profile file beats inline profile");
    assert_eq!(
        timeout(&prod().with("AUTUMN_GRPC__TIMEOUT_MS", "4")),
        4,
        "env beats profile file"
    );
}

#[test]
fn profile_aliases_and_names_resolve() {
    let dir = temp_dir("aliases");
    std::fs::write(
        dir.join("autumn.toml"),
        "[profile.production.grpc]\ntimeout_ms = 7\n[profile.development.grpc]\ntimeout_ms = 8\n",
    )
    .unwrap();
    let resolve = |env: MockEnv| GrpcConfig::resolve_with_env("grpc", &env).unwrap();

    let prod = resolve(env_for(&dir).with("AUTUMN_PROFILE", "production"));
    assert_eq!(prod.profile(), "prod");
    assert!(!prod.is_development());
    assert_eq!(prod.config().timeout_ms, 7, "legacy `production` alias");

    let dev = resolve(env_for(&dir));
    assert_eq!(dev.profile(), "dev");
    assert_eq!(dev.config().timeout_ms, 8, "legacy `development` alias");

    let test = resolve(env_for(&dir).with("AUTUMN_ENV", "test"));
    assert!(test.is_development(), "`test` gets development defaults");

    let debug_off = resolve(env_for(&dir).with("AUTUMN_IS_DEBUG", "0"));
    assert_eq!(debug_off.profile(), "prod");
}
