//! Checks that the checked-in code in `tests/generated/` matches
//! `proto/echo.proto`. Set `UPDATE_GENERATED=1` to write new files.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::path::{Path, PathBuf};

use prost::Message as _;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn generate(out: &Path) {
    let fds = protox::compile(["echo.proto"], [root().join("proto")]).expect("compile proto");
    std::fs::write(out.join("echo_descriptor.bin"), fds.encode_to_vec()).expect("write fds");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .out_dir(out)
        .compile_fds(fds)
        .expect("generate code");
}

#[test]
fn generated_code_is_fresh() {
    let checked_in = root().join("tests/generated");
    if std::env::var_os("UPDATE_GENERATED").is_some() {
        generate(&checked_in);
        return;
    }
    let scratch = std::env::temp_dir().join(format!("autumn-grpc-codegen-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).unwrap();
    generate(&scratch);
    for file in ["autumn.echo.v1.rs", "echo_descriptor.bin"] {
        let fresh = std::fs::read(scratch.join(file)).unwrap();
        let stored = std::fs::read(checked_in.join(file)).unwrap_or_else(|_| {
            panic!("missing tests/generated/{file}; run with UPDATE_GENERATED=1")
        });
        assert!(
            fresh == stored,
            "tests/generated/{file} is stale; run with UPDATE_GENERATED=1"
        );
    }
    let _ = std::fs::remove_dir_all(&scratch);
}
