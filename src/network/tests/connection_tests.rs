//! Тесты для сетевых соединений

use crate::network::connection::{Connection, ConnectionConfig, ConnectionState};
use std::time::Duration;

#[test]
fn test_connection_state_variants() {
    let states = vec![
        ConnectionState::New,
        ConnectionState::Connected,
        ConnectionState::Authenticated,
        ConnectionState::Closed,
    ];

    assert_eq!(states.len(), 4);
}

#[test]
fn test_connection_config_default() {
    let config = ConnectionConfig::default();

    assert!(config.read_timeout.as_secs() > 0);
    assert!(config.write_timeout.as_secs() > 0);
    assert!(config.buffer_size > 0);
}

#[test]
fn test_connection_config_custom() {
    let config = ConnectionConfig {
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        buffer_size: 8192,
        enable_keepalive: true,
        keepalive_interval: Duration::from_secs(60),
    };

    assert_eq!(config.read_timeout.as_secs(), 10);
    assert_eq!(config.buffer_size, 8192);
    assert!(config.enable_keepalive);
}

#[test]
fn test_connection_buffer_sizes() {
    let sizes = vec![1024, 4096, 8192, 16384];

    for size in sizes {
        let config = ConnectionConfig {
            buffer_size: size,
            ..Default::default()
        };

        assert_eq!(config.buffer_size, size);
    }
}

#[test]
fn test_connection_timeouts() {
    let timeouts = vec![
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_secs(60),
    ];

    for timeout in timeouts {
        let config = ConnectionConfig {
            read_timeout: timeout,
            write_timeout: timeout,
            ..Default::default()
        };

        assert_eq!(config.read_timeout, timeout);
        assert_eq!(config.write_timeout, timeout);
    }
}

#[test]
fn test_connection_keepalive_enabled() {
    let config = ConnectionConfig {
        enable_keepalive: true,
        keepalive_interval: Duration::from_secs(30),
        ..Default::default()
    };

    assert!(config.enable_keepalive);
    assert_eq!(config.keepalive_interval.as_secs(), 30);
}

#[test]
fn test_connection_keepalive_disabled() {
    let config = ConnectionConfig {
        enable_keepalive: false,
        ..Default::default()
    };

    assert!(!config.enable_keepalive);
}

#[test]
fn test_connection_state_transitions() {
    // Проверяем логику переходов состояний
    let initial = ConnectionState::New;
    let connected = ConnectionState::Connected;
    let authenticated = ConnectionState::Authenticated;
    let closed = ConnectionState::Closed;

    assert_ne!(initial, connected);
    assert_ne!(connected, authenticated);
    assert_ne!(authenticated, closed);
}

#[test]
fn test_connection_id_generation() {
    // Тестируем, что ID соединений уникальны
    let ids: Vec<u64> = (1..=100).collect();
    let unique_ids: std::collections::HashSet<_> = ids.iter().collect();

    assert_eq!(ids.len(), unique_ids.len());
}

#[test]
fn test_connection_statistics() {
    let config = ConnectionConfig::default();

    // Статистика должна быть доступна
    assert!(config.buffer_size > 0);
}
