//! Global `tracing` subscriber for the RustDB server ([tokio-rs/tracing](https://github.com/tokio-rs/tracing)).
//!
//! Configures [`tracing-subscriber`](https://docs.rs/tracing-subscriber) with:
//! - [`EnvFilter`] from **`RUST_LOG`** (default `info`);
//! - optional [`tracing-chrome`](https://docs.rs/tracing-chrome) JSON when **`RUSTDB_TRACE_CHROME_PATH`** is set
//!   (open in Chrome’s **chrome://tracing**);
//! - [`fmt`](mod@tracing_subscriber::fmt) for human-readable stderr logs.
//!
//! After the subscriber is installed, [`tracing-log`](https://docs.rs/tracing-log) bridges the legacy
//! [`log`](https://docs.rs/log) crate so `log::info!` etc. are recorded by the same pipeline.

use std::path::PathBuf;

use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Holds resources that must stay alive for the duration of the process (e.g. Chrome trace flush on drop).
pub struct ServerTracing {
    chrome_flush_guard: Option<tracing_chrome::FlushGuard>,
}

impl ServerTracing {
    /// `true` when **`RUSTDB_TRACE_CHROME_PATH`** was set and a Chrome JSON trace is being written.
    pub fn chrome_trace_enabled(&self) -> bool {
        self.chrome_flush_guard.is_some()
    }
}

fn default_filter() -> EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

fn chrome_path_from_env() -> Option<PathBuf> {
    let chrome_path = std::env::var("RUSTDB_TRACE_CHROME_PATH").ok()?;
    if chrome_path.trim().is_empty() {
        return None;
    }
    Some(PathBuf::from(chrome_path))
}

fn build_chrome_layer(
    pb: &PathBuf,
) -> Result<
    (
        tracing_chrome::ChromeLayer<tracing_subscriber::Registry>,
        tracing_chrome::FlushGuard,
    ),
    String,
> {
    if let Some(parent) = pb.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    Ok(tracing_chrome::ChromeLayerBuilder::new()
        .file(pb)
        .include_args(true)
        .build())
}

/// Install the global default subscriber. Call **once** at process startup (e.g. `rustdb server`).
pub fn init_server_tracing() -> Result<ServerTracing, String> {
    let filter = default_filter();
    let chrome_path = chrome_path_from_env();
    let mut chrome_guard = None;

    match chrome_path {
        Some(ref pb) => {
            let (chrome_layer, guard) = build_chrome_layer(pb)?;
            chrome_guard = Some(guard);
            tracing_subscriber::registry()
                .with(chrome_layer)
                .with(filter)
                .with(tracing_subscriber::fmt::layer())
                .try_init()
                .map_err(|e| e.to_string())?;
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .try_init()
                .map_err(|e| e.to_string())?;
        }
    }

    // Route `log::` records through tracing (storage WAL, etc.).
    let _ = tracing_log::LogTracer::init();

    Ok(ServerTracing {
        chrome_flush_guard: chrome_guard,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use tempfile::TempDir;

    #[test]
    fn chrome_path_from_env_empty_is_none() {
        std::env::remove_var("RUSTDB_TRACE_CHROME_PATH");
        assert!(chrome_path_from_env().is_none());
        std::env::set_var("RUSTDB_TRACE_CHROME_PATH", "   ");
        assert!(chrome_path_from_env().is_none());
    }

    #[test]
    fn build_chrome_layer_creates_parent_dir() {
        let dir = TempDir::new().expect("tempdir");
        let trace_path = dir.path().join("nested").join("trace.json");
        assert!(
            trace_path.parent().unwrap().exists() == false,
            "parent dir should not exist yet"
        );

        let _ = build_chrome_layer(&trace_path).expect("build chrome layer");
        assert!(trace_path.parent().unwrap().exists(), "parent dir created");
    }

    #[test]
    fn default_filter_is_constructible() {
        let _f = default_filter();
    }

    #[test]
    fn init_server_tracing_installs_subscriber_once() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            std::env::remove_var("RUSTDB_TRACE_CHROME_PATH");
            let t = init_server_tracing().expect("init tracing");
            assert!(!t.chrome_trace_enabled());
        });
    }
}
