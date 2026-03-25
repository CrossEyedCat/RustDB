//! CLI interface for rustdb
//!
//! Provides command-line interface for database management and language settings

use crate::common::{set_language, t, DatabaseConfig, I18nManager, Language, MessageKey, I18N};
use crate::network::engine::{EngineHandle, EngineOutput, StubEngine};
use crate::network::server::QuicServer;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// RustDB - Relational database implementation in Rust
#[derive(Parser)]
#[command(name = "rustdb")]
#[command(about = "RustDB - A relational database implementation in Rust")]
#[command(version)]
pub struct Cli {
    /// Interface language (en)
    #[arg(short, long, value_name = "LANGUAGE")]
    pub language: Option<String>,

    /// Configuration file
    #[arg(short, long, value_name = "CONFIG")]
    pub config: Option<PathBuf>,

    /// Logging verbosity level
    #[arg(long, value_name = "LEVEL")]
    pub log_level: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the QUIC database server (ALPN `rustdb-v1`; see `docs/network/`)
    Server {
        /// Listen address host (overrides `network.host` in config when set)
        #[arg(long, value_name = "HOST")]
        host: Option<String>,

        /// Listen UDP port (overrides `network.port` in config when set)
        #[arg(short, long, value_name = "PORT")]
        port: Option<u16>,

        /// Write the dev TLS leaf certificate (DER) to this path for `rustdb_quic_client --cert`
        #[arg(long, value_name = "PATH")]
        cert_out: Option<PathBuf>,
    },

    /// Interface language management
    Language {
        #[command(subcommand)]
        action: LanguageCommands,
    },

    /// Show system information
    Info,

    /// Create a new database
    Create {
        /// Database name
        name: String,

        /// Data storage directory
        #[arg(short, long)]
        data_dir: Option<PathBuf>,
    },

    /// Execute SQL query
    Query {
        /// SQL query
        query: String,

        /// Database
        #[arg(short, long)]
        database: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum LanguageCommands {
    /// Show current language
    Show,

    /// Set language
    Set {
        /// Language (en)
        language: String,
    },

    /// Show supported languages
    List,
}

impl Cli {
    /// Initializes CLI with language settings
    pub fn init() -> Self {
        let cli = Self::parse();

        // Set language from command line arguments
        if let Some(lang_str) = &cli.language {
            if let Ok(language) = lang_str.parse::<Language>() {
                let _ = set_language(language);
            }
        }

        cli
    }

    /// Loads configuration
    pub fn load_config(&self) -> Result<DatabaseConfig, Box<dyn std::error::Error>> {
        let mut config = if let Some(config_path) = &self.config {
            DatabaseConfig::from_file(config_path)?
        } else {
            // Try to load from config.toml, if not found - use default
            DatabaseConfig::from_file(&std::path::PathBuf::from("config.toml"))
                .unwrap_or_else(|_| DatabaseConfig::default())
        };

        // Apply settings from command line
        if let Some(lang_str) = &self.language {
            if let Ok(language) = lang_str.parse::<Language>() {
                config.language = language;
                let _ = set_language(language);
            }
        }

        // if let Some(log_level) = &self.log_level {
        //     config.logging.level = log_level.clone();
        // }

        Ok(config)
    }

    /// Executes a command
    pub async fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.command {
            Some(Commands::Server {
                port,
                host,
                cert_out,
            }) => self.run_server(host.clone(), *port, cert_out.clone()).await,
            Some(Commands::Language { action }) => self.handle_language_command(action).await,
            Some(Commands::Info) => self.show_info().await,
            Some(Commands::Create { name, data_dir }) => {
                self.create_database(name, data_dir.as_ref()).await
            }
            Some(Commands::Query { query, database }) => {
                self.execute_query(query, database.as_ref()).await
            }
            None => self.show_help().await,
        }
    }

    /// Starts the QUIC listener with [`StubEngine`] until Ctrl+C (production wiring uses [`EngineHandle`] for `Database` later).
    async fn run_server(
        &self,
        host_arg: Option<String>,
        port_arg: Option<u16>,
        cert_out: Option<PathBuf>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init();

        println!("{}", t(MessageKey::Welcome));

        let db = self.load_config()?;
        db.validate().map_err(|e| e.to_string())?;

        let host = host_arg.unwrap_or_else(|| db.network.host.clone());
        let port = port_arg.unwrap_or(db.network.port);

        let server_config = crate::network::server::ServerConfig {
            host,
            port,
            max_connections: db.network.max_connections,
            connection_timeout: Duration::from_secs(db.connection_timeout),
            query_timeout: Duration::from_secs(db.query_timeout),
            ..Default::default()
        };

        let srv = Arc::new(
            QuicServer::bind(server_config)
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?,
        );

        if let Some(ref path) = cert_out {
            std::fs::write(path, srv.pinned_certificate().as_ref())?;
            info!(path = %path.display(), "wrote TLS leaf certificate (DER) for QUIC clients");
        }

        let listen = srv.local_addr()?;
        println!(
            "QUIC listening on {} (ALPN rustdb-v1). Press Ctrl+C to stop.",
            listen
        );
        if let Some(ref p) = cert_out {
            println!(
                "TLS leaf written to {} — use: rustdb_quic_client --addr {} --cert {} --server-name <SAN>",
                p.display(),
                listen,
                p.display(),
            );
        }

        let engine: Arc<dyn EngineHandle> =
            Arc::new(StubEngine::fixed_ok(EngineOutput::ResultSet {
                columns: vec![],
                rows: vec![],
            }));

        let endpoint = srv.endpoint().clone();
        let run_task = tokio::spawn({
            let srv = srv.clone();
            async move {
                if let Err(e) = srv.run(engine).await {
                    warn!(error = %e, "QUIC accept loop ended with error");
                }
            }
        });

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown requested");
                QuicServer::initiate_shutdown(&endpoint);
                QuicServer::wait_idle(&endpoint).await;
            }
            r = run_task => {
                match r {
                    Ok(()) => {}
                    Err(e) => return Err(format!("server task join: {}", e).into()),
                }
            }
        }

        Ok(())
    }

    /// Handles language management commands
    async fn handle_language_command(
        &self,
        action: &LanguageCommands,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Load configuration before executing commands
        let config = self.load_config()?;
        println!("DEBUG: Loaded config language: {:?}", config.language);
        set_language(config.language)?;

        match action {
            LanguageCommands::Show => {
                let current_lang = I18N.get_language()?;
                println!("{}: {}", t(MessageKey::Info), current_lang);
            }
            LanguageCommands::Set { language } => {
                let lang: Language = language.parse()?;
                set_language(lang)?;
                println!("{}: {}", t(MessageKey::Success), lang);
            }
            LanguageCommands::List => {
                println!("{}:", t(MessageKey::Info));
                for lang in I18nManager::supported_languages() {
                    println!("  {} - {}", lang, I18N.get_language_name(lang));
                }
            }
        }
        Ok(())
    }

    /// Shows system information
    async fn show_info(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("RustDB {}", env!("CARGO_PKG_VERSION"));
        println!("{}: {}", t(MessageKey::Info), I18N.get_language()?);
        println!("{}: {}", t(MessageKey::Info), std::env::consts::OS);
        println!("{}: {}", t(MessageKey::Info), std::env::consts::ARCH);

        Ok(())
    }

    /// Creates a new database
    async fn create_database(
        &self,
        name: &str,
        data_dir: Option<&PathBuf>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}: {}", t(MessageKey::Info), name);

        let data_path = data_dir.cloned().unwrap_or_else(|| PathBuf::from("./data"));
        println!("{}: {:?}", t(MessageKey::Info), data_path);

        // TODO: Implement database creation
        println!("{}", t(MessageKey::Success));

        Ok(())
    }

    /// Executes an SQL query
    async fn execute_query(
        &self,
        query: &str,
        database: Option<&String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}: {}", t(MessageKey::Info), query);

        if let Some(db_name) = database {
            println!("{}: {}", t(MessageKey::Info), db_name);
        }

        // TODO: Implement query execution
        println!("{}", t(MessageKey::Success));

        Ok(())
    }

    /// Shows help
    async fn show_help(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", t(MessageKey::Welcome));
        println!("{}", t(MessageKey::Info));

        // TODO: Show detailed help

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        let args = vec!["rustdb", "--language", "en", "language", "show"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.language, Some("en".to_string()));
        assert!(matches!(cli.command, Some(Commands::Language { .. })));
    }

    #[test]
    fn test_language_commands() {
        let args = vec!["rustdb", "language", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        if let Some(Commands::Language { action }) = cli.command {
            assert!(matches!(action, LanguageCommands::List));
        } else {
            panic!("Expected language command");
        }
    }

    #[test]
    fn test_cli_server_command() {
        let cli = Cli::try_parse_from(vec![
            "rustdb",
            "server",
            "--port",
            "9000",
            "--host",
            "127.0.0.1",
        ])
        .unwrap();
        if let Some(Commands::Server {
            port,
            host,
            cert_out,
        }) = cli.command
        {
            assert_eq!(port, Some(9000));
            assert_eq!(host, Some("127.0.0.1".to_string()));
            assert!(cert_out.is_none());
        } else {
            panic!();
        }
    }

    #[test]
    fn test_cli_server_defaults_from_config() {
        let cli = Cli::try_parse_from(vec!["rustdb", "server"]).unwrap();
        if let Some(Commands::Server {
            port,
            host,
            cert_out,
        }) = cli.command
        {
            assert!(port.is_none());
            assert!(host.is_none());
            assert!(cert_out.is_none());
        } else {
            panic!();
        }
    }

    #[test]
    fn test_cli_info() {
        let cli = Cli::try_parse_from(vec!["rustdb", "info"]).unwrap();
        assert!(matches!(cli.command, Some(Commands::Info)));
    }

    #[test]
    fn test_cli_create() {
        let cli =
            Cli::try_parse_from(vec!["rustdb", "create", "mydb", "--data-dir", "./d"]).unwrap();
        if let Some(Commands::Create { name, data_dir }) = cli.command {
            assert_eq!(name, "mydb");
            assert_eq!(data_dir, Some(std::path::PathBuf::from("./d")));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_cli_query() {
        let cli = Cli::try_parse_from(vec!["rustdb", "query", "SELECT 1", "-d", "db1"]).unwrap();
        if let Some(Commands::Query { query, database }) = cli.command {
            assert!(query.contains("SELECT"));
            assert_eq!(database, Some("db1".into()));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_cli_language_set() {
        let cli = Cli::try_parse_from(vec!["rustdb", "language", "set", "en"]).unwrap();
        if let Some(Commands::Language { action }) = cli.command {
            assert!(matches!(action, LanguageCommands::Set { .. }));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_cli_init_parses_language() {
        let _ = Cli::try_parse_from(vec!["rustdb", "--language", "en"]);
    }
}
