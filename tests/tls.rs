//! TLS and mTLS: AC10 (feature `tls`).

#![cfg(feature = "tls")]
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{EchoClient, pb};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

struct Pki {
    dir: PathBuf,
    ca_pem: String,
    client_cert: String,
    client_key: String,
}

fn pki(name: &str) -> Pki {
    let dir = std::env::temp_dir().join(format!("autumn-grpc-tls-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

    let mut server = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server.signed_by(&server_key, &ca).unwrap();

    let mut client = CertificateParams::new(vec!["client".to_owned()]).unwrap();
    client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client.signed_by(&client_key, &ca).unwrap();

    std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
    std::fs::write(dir.join("server.pem"), server_cert.pem()).unwrap();
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).unwrap();
    Pki {
        dir,
        ca_pem: ca.pem(),
        client_cert: client_cert.pem(),
        client_key: client_key.serialize_pem(),
    }
}

fn path(dir: &Path, file: &str) -> String {
    dir.join(file).to_string_lossy().into_owned()
}

async fn tls_channel(addr: std::net::SocketAddr, tls: ClientTlsConfig) -> Result<Channel, tonic::transport::Error> {
    Channel::from_shared(format!("https://localhost:{}", addr.port()))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
}

async fn say(channel: Channel) -> Result<String, tonic::Status> {
    EchoClient::new(channel)
        .say(pb::SayRequest {
            message: "secure".into(),
        })
        .await
        .map(|r| r.into_inner().message)
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_over_tls() {
    let pki = pki("server");
    let dir = pki.dir.clone();
    let plugin = common::echo_plugin().configure(move |c| {
        c.tls.cert_path = path(&dir, "server.pem");
        c.tls.key_path = path(&dir, "server.key");
    });
    let (_http, handle) = common::boot(plugin);
    let addr = handle.local_addr().unwrap();

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .domain_name("localhost");
    let channel = tls_channel(addr, tls).await.unwrap();
    assert_eq!(say(channel).await.unwrap(), "secure");

    let plain = common::connect(addr).await;
    assert!(say(plain).await.is_err(), "plain text is refused");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mtls_requires_a_client_certificate() {
    let pki = pki("mtls");
    let dir = pki.dir.clone();
    let plugin = common::echo_plugin().configure(move |c| {
        c.tls.cert_path = path(&dir, "server.pem");
        c.tls.key_path = path(&dir, "server.key");
        c.tls.client_ca_path = path(&dir, "ca.pem");
    });
    let (_http, handle) = common::boot(plugin);
    let addr = handle.local_addr().unwrap();

    let anonymous = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .domain_name("localhost");
    let refused = match tls_channel(addr, anonymous).await {
        Ok(channel) => say(channel).await.is_err(),
        Err(_) => true,
    };
    assert!(refused, "a client without a certificate is refused");

    let identified = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .identity(Identity::from_pem(&pki.client_cert, &pki.client_key))
        .domain_name("localhost");
    let channel = tls_channel(addr, identified).await.unwrap();
    assert_eq!(say(channel).await.unwrap(), "secure");
    handle.shutdown().await;
}

#[test]
fn a_missing_certificate_file_aborts_boot() {
    let plugin = common::echo_plugin().configure(|c| {
        c.tls.cert_path = "/nonexistent/cert.pem".to_owned();
        c.tls.key_path = "/nonexistent/key.pem".to_owned();
    });
    let outcome = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            autumn_web::test::TestApp::new().plugin(plugin).build();
        }))
    })
    .join()
    .unwrap();
    assert!(outcome.is_err());
}
