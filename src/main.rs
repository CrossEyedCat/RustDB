//! Main executable file for rustdb

#![allow(unused_imports)]

use rustdb::cli::{Cli, Commands};
use rustdb::common::{set_language, t, Language, MessageKey};
use rustdb::{Database, VERSION};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize CLI with internationalization support
    let cli = Cli::init();

    // Load configuration
    let config = cli.load_config()?;

    // Set language from configuration
    set_language(config.language)?;

    if let Some(Commands::Query {
        query,
        batch_file,
        database,
    }) = &cli.command
    {
        return cli.execute_query_sync(query.as_deref(), database.as_ref(), batch_file.as_deref());
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(cli.execute())?;

    Ok(())
}
