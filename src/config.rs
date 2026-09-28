//! The `[grpc]` section of `autumn.toml`.
//!
//! Each key has a safe default. The plugin merges the section in the same
//! order as Autumn core:
//!
//! 1. `[grpc]` in `autumn.toml`.
//! 2. `[profile.<name>.grpc]` in `autumn.toml`.
//! 3. `[grpc]` in `autumn-<profile>.toml`.
//! 4. `AUTUMN_GRPC__*` environment variables (also from `.env` files).
//!
//! ```toml
//! [grpc]
//! bind = "0.0.0.0:50051"
//! reflection = "auto"        # on in dev/test, off in other profiles
//! shutdown_grace_ms = 10000
//!
//! [grpc.tls]                 # needs the `tls` feature
//! cert_path = "certs/server.pem"
//! key_path = "certs/server.key"
//! ```

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use autumn_web::config::Env;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The default config section.
pub const DEFAULT_SECTION: &str = "grpc";

/// A three-state switch. In TOML, write a boolean or `"auto"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Toggle {
    /// The context decides. See each field.
    #[default]
    Auto,
    /// Always on.
    On,
    /// Always off.
    Off,
}

impl Toggle {
    /// The value, with `auto` set to `auto_value`.
    #[must_use]
    pub const fn resolve(self, auto_value: bool) -> bool {
        match self {
            Self::Auto => auto_value,
            Self::On => true,
            Self::Off => false,
        }
    }
}

impl From<bool> for Toggle {
    fn from(value: bool) -> Self {
        if value { Self::On } else { Self::Off }
    }
}

impl fmt::Display for Toggle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Auto => "auto",
            Self::On => "true",
            Self::Off => "false",
        })
    }
}

impl Serialize for Toggle {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Auto => serializer.serialize_str("auto"),
            Self::On => serializer.serialize_bool(true),
            Self::Off => serializer.serialize_bool(false),
        }
    }
}

impl<'de> Deserialize<'de> for Toggle {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Bool(bool),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Bool(value) => Ok(value.into()),
            Raw::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
                "auto" => Ok(Self::Auto),
                "true" | "on" | "enabled" | "yes" => Ok(Self::On),
                "false" | "off" | "disabled" | "no" => Ok(Self::Off),
                other => Err(serde::de::Error::custom(format!(
                    "expected true, false or \"auto\", found \"{other}\""
                ))),
            },
        }
    }
}

/// Settings for one gRPC server.
///
/// For each `*_ms` duration, `0` means "not set" (the tonic default).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrpcConfig {
    /// Start the server. Default: `true`.
    pub enabled: bool,
    /// Listen address, `IP:port`. Port `0` picks a free port.
    /// Default: `"0.0.0.0:50051"`.
    pub bind: String,
    /// Serve `grpc.health.v1.Health`. Default: `true`.
    pub health: bool,
    /// Serve server reflection. `auto` is on in `dev`/`test` only.
    pub reflection: Toggle,
    /// Record `grpc_server_*` metrics. Default: `true`.
    pub metrics: bool,
    /// Maximum label sets per metric. Extra calls count as `other`.
    /// Default: `1000`.
    pub max_metric_series: usize,
    /// Time for in-flight calls to finish at shutdown. Default: `10000`.
    pub shutdown_grace_ms: u64,
    /// Per-call timeout. Default: `0` (none).
    pub timeout_ms: u64,
    /// Concurrent calls per connection. Default: `0` (no limit).
    pub concurrency_limit_per_connection: usize,
    /// HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`. Default: `0` (hyper default).
    pub max_concurrent_streams: u32,
    /// Set `TCP_NODELAY`. Default: `true`.
    pub tcp_nodelay: bool,
    /// TCP keepalive idle time. Default: `0` (off).
    pub tcp_keepalive_ms: u64,
    /// HTTP/2 PING interval. Default: `0` (off).
    pub http2_keepalive_interval_ms: u64,
    /// HTTP/2 PING ack timeout. Default: `0` (tonic default, 20 s).
    pub http2_keepalive_timeout_ms: u64,
    /// Close connections after this age. Default: `0` (never).
    pub max_connection_age_ms: u64,
    /// TLS settings. Empty paths mean plain text.
    pub tls: TlsConfig,
}

/// TLS settings. Needs the `tls` crate feature.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain of the server.
    pub cert_path: String,
    /// PEM private key of the server.
    pub key_path: String,
    /// PEM CA bundle. When set, clients must show a certificate (mTLS).
    pub client_ca_path: String,
    /// With `client_ca_path`, also accept clients without a certificate.
    pub client_auth_optional: bool,
}

impl TlsConfig {
    /// `true` when a certificate is set.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !self.cert_path.trim().is_empty()
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: "0.0.0.0:50051".to_owned(),
            health: true,
            reflection: Toggle::Auto,
            metrics: true,
            max_metric_series: 1000,
            shutdown_grace_ms: 10_000,
            timeout_ms: 0,
            concurrency_limit_per_connection: 0,
            max_concurrent_streams: 0,
            tcp_nodelay: true,
            tcp_keepalive_ms: 0,
            http2_keepalive_interval_ms: 0,
            http2_keepalive_timeout_ms: 0,
            max_connection_age_ms: 0,
            tls: TlsConfig::default(),
        }
    }
}

/// A configuration problem. Boot stops. The plugin does not fall back to
/// defaults.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid gRPC configuration: {0}")]
pub struct ConfigError(pub String);

/// A resolved configuration and the active profile.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// The merged, validated configuration.
    pub config: GrpcConfig,
    /// The canonical active profile (`dev`, `prod`, `test`, ...).
    pub profile: String,
}

impl Resolved {
    /// Configuration from code. The plugin reads no files. The profile
    /// still comes from the environment.
    #[must_use]
    pub fn explicit(config: GrpcConfig) -> Self {
        let profile = autumn_web::dotenv::os_env_with_dotenv().map_or_else(
            |_| resolve_active_profile(&autumn_web::config::OsEnv).1,
            |env| resolve_active_profile(&env).1,
        );
        Self { config, profile }
    }

    /// `true` for the `dev` and `test` profiles.
    #[must_use]
    pub fn is_development(&self) -> bool {
        matches!(self.profile.as_str(), "dev" | "test")
    }
}

const fn millis(ms: u64) -> Option<Duration> {
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

impl GrpcConfig {
    /// The parsed listen address.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when `bind` is not `IP:port`.
    pub fn bind_addr(&self) -> Result<SocketAddr, ConfigError> {
        self.bind.trim().parse().map_err(|_| {
            ConfigError(format!(
                "`bind` must be IP:port (for example \"0.0.0.0:50051\"), found \"{}\"",
                self.bind
            ))
        })
    }

    /// Per-call timeout.
    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        millis(self.timeout_ms)
    }

    /// Shutdown grace period.
    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        Duration::from_millis(self.shutdown_grace_ms)
    }

    /// TCP keepalive idle time.
    #[must_use]
    pub const fn tcp_keepalive(&self) -> Option<Duration> {
        millis(self.tcp_keepalive_ms)
    }

    /// HTTP/2 PING interval.
    #[must_use]
    pub const fn http2_keepalive_interval(&self) -> Option<Duration> {
        millis(self.http2_keepalive_interval_ms)
    }

    /// HTTP/2 PING ack timeout.
    #[must_use]
    pub const fn http2_keepalive_timeout(&self) -> Option<Duration> {
        millis(self.http2_keepalive_timeout_ms)
    }

    /// Maximum connection age.
    #[must_use]
    pub const fn max_connection_age(&self) -> Option<Duration> {
        millis(self.max_connection_age_ms)
    }

    /// Parse `[section]` from a whole `autumn.toml` text. No profiles and
    /// no environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for bad TOML, unknown keys, wrong types or
    /// values that [`validate`](Self::validate) rejects.
    pub fn from_toml_str(text: &str, section: &str) -> Result<Self, ConfigError> {
        let document: toml::Table = toml::from_str(text).map_err(|e| ConfigError(e.to_string()))?;
        let config = Self::from_section(document.get(section))?;
        config.validate()?;
        Ok(config)
    }

    fn from_section(section: Option<&toml::Value>) -> Result<Self, ConfigError> {
        section.map_or_else(
            || Ok(Self::default()),
            |value| {
                value
                    .clone()
                    .try_into()
                    .map_err(|e: toml::de::Error| ConfigError(e.to_string()))
            },
        )
    }

    /// Resolve `[section]` from the app's files and the process environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a file cannot be read or parsed, or when
    /// the merged section is not valid.
    pub fn resolve(section: &str) -> Result<Resolved, ConfigError> {
        autumn_web::dotenv::os_env_with_dotenv().map_or_else(
            |_| Self::resolve_with_env(section, &autumn_web::config::OsEnv),
            |env| Self::resolve_with_env(section, &env),
        )
    }

    /// Like [`resolve`](Self::resolve), but reads only `env`.
    ///
    /// # Errors
    ///
    /// See [`resolve`](Self::resolve).
    pub fn resolve_with_env(section: &str, env: &dyn Env) -> Result<Resolved, ConfigError> {
        let (selected, canonical) = resolve_active_profile(env);
        let mut merged = toml::Value::Table(toml::map::Map::new());

        if let Some(base) = read_optional_toml(&find_config_file("autumn.toml", env))? {
            deep_merge(&mut merged, base.clone());
            for name in profile_inline_lookup_names(&canonical) {
                if let Some(profile) = profile_section(&base, name) {
                    deep_merge(&mut merged, profile);
                }
            }
        }
        for name in autumn_web::config::profile_override_file_lookup_names(&canonical, &selected) {
            let path = find_config_file(&format!("autumn-{name}.toml"), env);
            if let Some(overlay) = read_optional_toml(&path)? {
                deep_merge(&mut merged, overlay);
                break;
            }
        }

        let mut section_value = merged
            .get(section)
            .cloned()
            .unwrap_or_else(|| toml::Value::Table(toml::map::Map::new()));
        if !section_value.is_table() {
            return Err(ConfigError(format!("`{section}` must be a table")));
        }
        let mut config = Self::from_section(Some(&section_value))?;
        apply_env_overrides(section, &mut section_value, &mut config, env);
        config.validate()?;
        Ok(Resolved {
            config,
            profile: canonical,
        })
    }

    /// Reject values that fail at runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] that names the bad key.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.bind_addr()?;
        if self.shutdown_grace_ms == 0 {
            return Err(ConfigError(
                "`shutdown_grace_ms` must be greater than 0".to_owned(),
            ));
        }
        if self.max_metric_series == 0 {
            return Err(ConfigError(
                "`max_metric_series` must be greater than 0".to_owned(),
            ));
        }
        let tls = &self.tls;
        let has = |value: &str| !value.trim().is_empty();
        match (has(&tls.cert_path), has(&tls.key_path)) {
            (true, false) => {
                return Err(ConfigError(
                    "`tls.cert_path` is set, so `tls.key_path` is necessary".to_owned(),
                ));
            }
            (false, true) => {
                return Err(ConfigError(
                    "`tls.key_path` is set, so `tls.cert_path` is necessary".to_owned(),
                ));
            }
            _ => {}
        }
        if has(&tls.client_ca_path) && !has(&tls.cert_path) {
            return Err(ConfigError(
                "`tls.client_ca_path` needs `tls.cert_path` and `tls.key_path`".to_owned(),
            ));
        }
        Ok(())
    }
}

/// The env prefix for a section: `grpc` → `AUTUMN_GRPC__`.
#[must_use]
pub fn env_prefix(section: &str) -> String {
    format!("AUTUMN_{}__", section.to_ascii_uppercase())
}

/// Apply `AUTUMN_<SECTION>__<PATH>` overrides for each known leaf key.
///
/// Values are TOML literals (`true`, `10`, `"x"`) or bare strings. An
/// override that does not type-check is logged and ignored, as in core.
fn apply_env_overrides(
    section_name: &str,
    section: &mut toml::Value,
    config: &mut GrpcConfig,
    env: &dyn Env,
) {
    let Ok(defaults) = toml::Value::try_from(GrpcConfig::default()) else {
        return;
    };
    let prefix = env_prefix(section_name);
    let mut leaves = Vec::new();
    collect_leaves(&defaults, &mut Vec::new(), &mut leaves);
    for path in leaves {
        let key = format!(
            "{prefix}{}",
            path.iter()
                .map(|segment| segment.to_ascii_uppercase())
                .collect::<Vec<_>>()
                .join("__")
        );
        let Some(raw) = env_trimmed(env, &key) else {
            continue;
        };
        let mut candidate = section.clone();
        set_path(&mut candidate, &path, parse_env_value(&raw));
        match GrpcConfig::from_section(Some(&candidate)) {
            Ok(parsed) => {
                *section = candidate;
                *config = parsed;
            }
            Err(error) => tracing::warn!(
                variable = %key,
                %error,
                "ignoring a gRPC environment override that does not type-check"
            ),
        }
    }
}

fn parse_env_value(raw: &str) -> toml::Value {
    toml::from_str::<toml::Table>(&format!("v = {raw}"))
        .ok()
        .and_then(|mut table| table.remove("v"))
        .unwrap_or_else(|| toml::Value::String(raw.to_owned()))
}

fn collect_leaves(value: &toml::Value, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    if let toml::Value::Table(table) = value {
        for (key, child) in table {
            prefix.push(key.clone());
            collect_leaves(child, prefix, out);
            prefix.pop();
        }
    } else {
        out.push(prefix.clone());
    }
}

fn set_path(root: &mut toml::Value, path: &[String], value: toml::Value) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut cursor = root;
    for segment in parents {
        let Some(table) = cursor.as_table_mut() else {
            return;
        };
        cursor = table
            .entry(segment.clone())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    }
    if let Some(table) = cursor.as_table_mut() {
        table.insert(last.clone(), value);
    }
}

/// A non-blank env value. A blank value counts as unset, as in core.
fn env_trimmed(env: &dyn Env, key: &str) -> Option<String> {
    env.var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// `(selected spelling, canonical name)` of the active profile.
///
/// Same order as core: `AUTUMN_ENV`, `AUTUMN_PROFILE`, `--profile`,
/// `AUTUMN_IS_DEBUG=0` (→ `prod`), then `dev`.
fn resolve_active_profile(env: &dyn Env) -> (String, String) {
    let selected = resolve_profile_input(env);
    let canonical =
        autumn_web::config::normalize_profile_name(&selected).unwrap_or_else(|| "dev".to_owned());
    (selected, canonical)
}

fn resolve_profile_input(env: &dyn Env) -> String {
    if let Some(value) = env_trimmed(env, "AUTUMN_ENV") {
        return value;
    }
    if let Some(value) = env_trimmed(env, "AUTUMN_PROFILE") {
        return value;
    }
    let args: Vec<String> = std::env::args().collect();
    for (index, arg) in args.iter().enumerate() {
        if arg == "--profile"
            && let Some(profile) = args.get(index.saturating_add(1))
            && !profile.trim().is_empty()
        {
            return profile.trim().to_owned();
        }
        if let Some(profile) = arg.strip_prefix("--profile=")
            && !profile.trim().is_empty()
        {
            return profile.trim().to_owned();
        }
    }
    if env_trimmed(env, "AUTUMN_IS_DEBUG").as_deref() == Some("0") {
        return "prod".to_owned();
    }
    "dev".to_owned()
}

/// Find a config file as core does: `AUTUMN_MANIFEST_DIR` first, then the
/// working directory.
fn find_config_file(filename: &str, env: &dyn Env) -> PathBuf {
    if let Some(dir) = env_trimmed(env, "AUTUMN_MANIFEST_DIR") {
        let candidate = PathBuf::from(dir).join(filename);
        if candidate.exists() {
            return candidate;
        }
    }
    PathBuf::from(filename)
}

fn read_optional_toml(path: &Path) -> Result<Option<toml::Value>, ConfigError> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let table = toml::from_str::<toml::Table>(&contents)
                .map_err(|e| ConfigError(format!("cannot parse {}: {e}", path.display())))?;
            Ok(Some(toml::Value::Table(table)))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ConfigError(format!(
            "cannot read {}: {error}",
            path.display()
        ))),
    }
}

fn profile_inline_lookup_names(canonical: &str) -> Vec<&str> {
    match canonical {
        "prod" => vec!["production", "prod"],
        "dev" => vec!["development", "dev"],
        other => vec![other],
    }
}

fn profile_section(base: &toml::Value, profile: &str) -> Option<toml::Value> {
    base.get("profile")
        .and_then(toml::Value::as_table)
        .and_then(|profiles| profiles.get(profile))
        .and_then(toml::Value::as_table)
        .map(|table| toml::Value::Table(table.clone()))
}

/// Merge `overlay` into `base`. Tables merge; other values replace.
fn deep_merge(base: &mut toml::Value, overlay: toml::Value) {
    deep_merge_at(base, overlay, 0);
}

fn deep_merge_at(base: &mut toml::Value, overlay: toml::Value, depth: usize) {
    const MAX_MERGE_DEPTH: usize = 16;
    if depth > MAX_MERGE_DEPTH {
        return;
    }
    let toml::Value::Table(overlay_table) = overlay else {
        return;
    };
    let Some(base_table) = base.as_table_mut() else {
        return;
    };
    for (key, overlay_value) in overlay_table {
        let recurse =
            overlay_value.is_table() && base_table.get(&key).is_some_and(toml::Value::is_table);
        if recurse {
            if let Some(base_value) = base_table.get_mut(&key) {
                deep_merge_at(base_value, overlay_value, depth.saturating_add(1));
            }
        } else {
            base_table.insert(key, overlay_value);
        }
    }
}
