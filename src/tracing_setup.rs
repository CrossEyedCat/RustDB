//! Global `tracing` subscriber for the RustDB server ([tokio-rs/tracing](https://github.com/tokio-rs/tracing)).
//!
//! Configures [`tracing-subscriber`](https://docs.rs/tracing-subscriber) with:
//! - [`EnvFilter`](tracing_subscriber::EnvFilter) from **`RUST_LOG`** (default `info`);
//! - optional [`tracing-chrome`](https://docs.rs/tracing-chrome) JSON when **`RUSTDB_TRACE_CHROME_PATH`** is set
//!   (open in Chrome’s **chrome://tracing**);
//! - [`fmt`](mod@tracing_subscriber::fmt) for human-readable stderr logs.
//!
//! After the subscriber is installed, [`tracing-log`](https://docs.rs/tracing-log) bridges the legacy
//! [`log`](https://docs.rs/log) crate so `log::info!` etc. are recorded by the same pipeline.

use std::path::PathBuf;

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

/// Install the global default subscriber. Call **once** at process startup (e.g. `rustdb server`).
pub fn init_server_tracing() -> Result<ServerTracing, String> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let chrome_path = std::env::var("RUSTDB_TRACE_CHROME_PATH").ok();
    let mut chrome_guard = None;

    match chrome_path {
        Some(ref p) if !p.trim().is_empty() => {
            let pb = PathBuf::from(p);
            if let Some(parent) = pb.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .file(pb)
                .include_args(true)
                .build();
            chrome_guard = Some(guard);
            tracing_subscriber::registry()
                .with(filter)
                .with(chrome_layer)
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
