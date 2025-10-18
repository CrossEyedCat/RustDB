//! Главный исполняемый файл rustdb

use rustdb::{Database, VERSION};
use rustdb::cli::Cli;
use rustdb::common::{set_language, Language, MessageKey, t};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Инициализируем CLI с поддержкой интернационализации
    let cli = Cli::init();
    
    // Загружаем конфигурацию
    let config = cli.load_config()?;
    
    // Устанавливаем язык из конфигурации
    set_language(config.language)?;
    
    // Выполняем команду
    cli.execute().await?;
    
    Ok(())
}
