//! Framework fit: AC13.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use autumn_plugin_grpc::{GrpcPlugin, PLUGIN_NAME};
use autumn_web::plugin::Plugin;
use autumn_web::plugin_conformance::{ConformanceConfig, run_conformance};
use autumn_web::route_listing::{RouteClassification, RouteInfo, RouteSource};

/// The routes as `declare_plugin_routes` attributes them.
fn manifest(plugin: &GrpcPlugin) -> Vec<RouteInfo> {
    let name = plugin.name().into_owned();
    plugin
        .route_infos()
        .into_iter()
        .map(|mut route| {
            route.source = RouteSource::Plugin(name.clone());
            route
        })
        .collect()
}

fn listed(routes: &[RouteInfo]) -> Vec<String> {
    let mut listed: Vec<String> = routes
        .iter()
        .map(|r| format!("{} {} {}", r.method, r.path, r.classification.as_str()))
        .collect();
    listed.sort();
    listed
}

#[test]
fn passes_the_framework_conformance_harness() {
    let plugin = common::echo_plugin();
    let name = plugin.name().into_owned();
    assert_eq!(name, format!("{PLUGIN_NAME}@grpc"));
    let routes = manifest(&plugin);
    let report = run_conformance(&ConformanceConfig::new(&name), &routes);
    assert!(report.passed(), "{}", report.to_text_report());
    assert_eq!(
        listed(&routes),
        [
            "GRPC /autumn.echo.v1.Echo/* unclassified",
            "GRPC /grpc.health.v1.Health/* framework",
            "GRPC /grpc.reflection.v1.ServerReflection/* framework",
            "GRPC /grpc.reflection.v1alpha.ServerReflection/* framework",
        ]
    );
}

#[test]
fn declarations_follow_the_configuration() {
    let plugin = common::echo_plugin()
        .development(false)
        .guard_interceptor(Ok, "bearer token")
        .configure(|c| c.health = false);
    let routes = manifest(&plugin);
    assert_eq!(listed(&routes), ["GRPC /autumn.echo.v1.Echo/* gated"]);
    assert_eq!(routes[0].classification, RouteClassification::Gated);
    assert_eq!(routes[0].middleware, ["bearer token"]);
    let report = run_conformance(&ConformanceConfig::new(plugin.name()), &routes);
    assert!(report.passed(), "{}", report.to_text_report());

    let public = common::echo_plugin().public().development(false);
    assert_eq!(
        listed(&manifest(&public)),
        [
            "GRPC /autumn.echo.v1.Echo/* public",
            "GRPC /grpc.health.v1.Health/* framework",
        ]
    );

    let disabled = common::echo_plugin().configure(|c| c.enabled = false);
    assert!(disabled.route_infos().is_empty());
    let invalid = common::echo_plugin().configure(|c| c.shutdown_grace_ms = 0);
    assert!(invalid.route_infos().is_empty());
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
fn two_plugins_with_one_section_share_a_name() {
    // Autumn skips the second (see `a_duplicate_plugin_is_skipped_at_boot`).
    let first = GrpcPlugin::new();
    let second = GrpcPlugin::new().configure(|c| c.timeout_ms = 1);
    assert_eq!(first.name(), second.name());
}

#[test]
fn service_names_list_what_was_added() {
    let plugin = common::echo_plugin();
    assert_eq!(plugin.service_names(), ["autumn.echo.v1.Echo"]);
    assert!(GrpcPlugin::default().service_names().is_empty());
}
