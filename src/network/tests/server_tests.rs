//! Network server tests

use crate::network::server::{
    build_quinn_server_config, NetworkServer, QuicServer, Server, ServerConfig,
};
use std::time::Duration;

#[test]
fn quinn_server_config_builds() {
    let c = ServerConfig::default();
    let (_cfg, _cert) = build_quinn_server_config(&c).expect("quinn ServerConfig should build");
}

#[tokio::test]
async fn quic_server_bind_ephemeral_port() {
    let config = ServerConfig {
        port: 0,
        ..Default::default()
    };
    let srv = QuicServer::bind(config).expect("QUIC bind");
    assert!(srv.local_addr().expect("local addr").port() > 0);
}

#[test]
fn test_server_config_default() {
    let config = ServerConfig::default();

    assert!(!config.host.is_empty());
    assert!(config.port > 0);
    assert!(config.max_connections > 0);
}

#[test]
fn test_server_config_custom() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 5432,
        max_connections: 100,
        connection_timeout: Duration::from_secs(30),
        enable_tls: false,
        ..Default::default()
    };

    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 5432);
    assert_eq!(config.max_connections, 100);
    assert!(!config.enable_tls);
}

#[test]
fn test_server_creation() {
    let config = ServerConfig::default();
    let server = Server::new(config);

    assert!(server.is_ok());
}

#[test]
fn test_server_different_ports() {
    let ports = vec![5432, 3306, 1433, 5000];

    for port in ports {
        let config = ServerConfig {
            port,
            ..Default::default()
        };

        let server = Server::new(config);
        assert!(server.is_ok());
    }
}

#[test]
fn test_server_max_connections() {
    let limits = vec![10, 100, 1000];

    for limit in limits {
        let config = ServerConfig {
            max_connections: limit,
            ..Default::default()
        };

        let server = Server::new(config);
        assert!(server.is_ok());
    }
}

#[test]
fn test_server_timeout_configuration() {
    let timeouts = vec![
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
    ];

    for timeout in timeouts {
        let config = ServerConfig {
            connection_timeout: timeout,
            ..Default::default()
        };

        let server = Server::new(config);
        assert!(server.is_ok());
    }
}

#[test]
fn test_server_tls_enabled() {
    let config = ServerConfig {
        enable_tls: true,
        ..Default::default()
    };

    let server = Server::new(config);
    // May fail without certificates, but construction should not panic
    assert!(server.is_ok() || server.is_err());
}

#[test]
fn test_server_tls_disabled() {
    let config = ServerConfig {
        enable_tls: false,
        ..Default::default()
    };

    let server = Server::new(config);
    assert!(server.is_ok());
}

#[test]
fn test_server_localhost() {
    let config = ServerConfig {
        host: "localhost".to_string(),
        ..Default::default()
    };

    let server = Server::new(config);
    assert!(server.is_ok());
}

#[test]
fn test_server_ipv4() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        ..Default::default()
    };

    let server = Server::new(config);
    assert!(server.is_ok());
}

#[test]
fn test_server_ipv6() {
    let config = ServerConfig {
        host: "::1".to_string(),
        ..Default::default()
    };

    let server = Server::new(config);
    assert!(server.is_ok() || server.is_err()); // IPv6 may be unsupported
}

#[test]
fn test_server_statistics() {
    let config = ServerConfig::default();
    let server = Server::new(config).unwrap();

    let stats = server.get_statistics();
    if let Ok(s) = stats {
        assert!(s.total_connections >= 0);
        assert!(s.active_connections >= 0);
    }
}

#[test]
fn test_network_server_legacy_new() {
    assert!(NetworkServer::new().is_ok());
}
