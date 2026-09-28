//! Framework fit: AC13.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod common;

use autumn_plugin_grpc::{GrpcPlugin, PLUGIN_NAME};
use autumn_web::plugin::Plugin;
use autumn_web::plugin_conformance::{ConformanceConfig, run_conformance};

#[test]
fn passes_the_framework_conformance_harness() {
    let plugin = common::echo_plugin();
    let name = plugin.name().into_owned();
    assert_eq!(name, format!("{PLUGIN_NAME}@grpc"));
    // gRPC uses its own listener, so the plugin mounts no HTTP routes.
    let report = run_conformance(&ConformanceConfig::new(&name), &[]);
    assert!(report.passed(), "{}", report.to_text_report());
}

#[test]
fn the_name_is_keyed_by_config_section() {
    let plugin = GrpcPlugin::new().config_section("grpc_admin");
    assert_eq!(plugin.name(), format!("{PLUGIN_NAME}@grpc_admin"));
}

#[test]
fn declares_its_config_section() {
    let app = autumn_web::app().plugin(GrpcPlugin::new());
    assert!(app.has_config_section("grpc"));
    assert!(app.has_plugin(&format!("{PLUGIN_NAME}@grpc")));
    let app = autumn_web::app().plugin(GrpcPlugin::new().config_section("grpc_admin"));
    assert!(app.has_config_section("grpc_admin"));
}

#[test]
fn a_duplicate_registration_is_skipped() {
    // The second plugin has an invalid config. If it were built, boot would
    // fail. Autumn skips it because the name is the same.
    let app = autumn_web::app()
        .plugin(GrpcPlugin::new())
        .plugin(GrpcPlugin::new().configure(|c| c.shutdown_grace_ms = 0));
    assert!(app.has_plugin(&format!("{PLUGIN_NAME}@grpc")));
}

#[test]
fn service_names_list_what_was_added() {
    let plugin = common::echo_plugin();
    assert_eq!(plugin.service_names(), ["autumn.echo.v1.Echo"]);
    assert!(GrpcPlugin::default().service_names().is_empty());
}
