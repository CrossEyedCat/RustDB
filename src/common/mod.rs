//! Общие типы и утилиты для RustBD

pub mod error;
pub mod types;
pub mod constants;
pub mod utils;

pub use error::{Error, Result};
pub use types::*;
pub use constants::*;
pub use utils::*;
