//! Main executable file for rustdb

#![allow(unused_imports)]

use rustdb::cli::Cli;
use rustdb::common::{set_language, t, Language, MessageKey};
use rustdb::{Database, VERSION};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize CLI with internationalization support
    let cli = Cli::init();

    // Load configuration
    let config = cli.load_config()?;

    // Set language from configuration
    set_language(config.language)?;

    // Execute command
    cli.execute().await?;

    Ok(())
}
