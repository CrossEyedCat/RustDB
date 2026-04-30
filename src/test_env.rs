use std::sync::Mutex;

/// Global lock for tests that mutate process environment variables.
///
/// Rust tests run in parallel by default, and environment variables are process-global.
/// Any test that calls `std::env::set_var` / `remove_var` should hold this lock to avoid flakiness.
pub static ENV_LOCK: Mutex<()> = Mutex::new(());

