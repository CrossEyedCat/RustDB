//! CLI interface for rustdb
//!
//! Provides command-line interface for database management and language settings

use crate::common::{set_language, t, DatabaseConfig, I18nManager, Language, MessageKey, I18N};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    /// Start the database server
    Server {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,

        /// Host to listen on
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
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
            Some(Commands::Server { port, host }) => self.run_server(host.clone(), *port).await,
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

    /// Starts the database server
    async fn run_server(&self, host: String, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}", t(MessageKey::Welcome));
        println!("{}: {}:{}", t(MessageKey::Info), host, port);

        // TODO: Implement server startup
        println!("{}", t(MessageKey::Info));

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
}
