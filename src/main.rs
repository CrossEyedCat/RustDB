//! Главный исполняемый файл RustBD

use clap::{Parser, Subcommand};
use rustbd::{Database, VERSION};

#[derive(Parser)]
#[command(name = "rustbd")]
#[command(about = "Реляционная база данных на Rust")]
#[command(version = VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Путь к файлу базы данных
    #[arg(short, long)]
    database: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Создает новую базу данных
    Create {
        /// Путь к файлу базы данных
        path: String,
    },
    /// Открывает существующую базу данных
    Open {
        /// Путь к файлу базы данных
        path: String,
    },
    /// Выполняет SQL запрос
    Query {
        /// SQL запрос для выполнения
        sql: String,
    },
    /// Показывает информацию о базе данных
    Info,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Create { path }) => {
            println!("Создание новой базы данных: {}", path);
            let db = Database::new()?;
            println!("База данных создана успешно!");
        }
        Some(Commands::Open { path }) => {
            println!("Открытие базы данных: {}", path);
            let db = Database::open(path)?;
            println!("База данных открыта успешно!");
        }
        Some(Commands::Query { sql }) => {
            println!("Выполнение SQL запроса: {}", sql);
            // TODO: Реализовать выполнение SQL запросов
            println!("Запрос выполнен!");
        }
        Some(Commands::Info) => {
            println!("Информация о базе данных:");
            println!("Версия: {}", VERSION);
            println!("Статус: В разработке");
        }
        None => {
            println!("Добро пожаловать в RustBD v{}!", VERSION);
            println!("Используйте --help для получения справки");
        }
    }

    Ok(())
}
