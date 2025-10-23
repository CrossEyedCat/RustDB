//! Пример использования системы интернационализации RustDB

use rustdb::common::{set_language, t, t_with_params, I18nManager, Language, MessageKey, I18N};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Пример системы интернационализации RustDB ===\n");

    // Демонстрация работы с английским языком
    println!("--- Английский язык ---");
    set_language(Language::English)?;

    println!("Приветствие: {}", t(MessageKey::Welcome));
    println!("Ошибка: {}", t(MessageKey::Error));
    println!("Успех: {}", t(MessageKey::Success));
    println!("Предупреждение: {}", t(MessageKey::Warning));
    println!("Информация: {}", t(MessageKey::Info));

    println!("\nСообщения об ошибках:");
    println!("Ошибка базы данных: {}", t(MessageKey::DatabaseError));
    println!("Ошибка подключения: {}", t(MessageKey::ConnectionError));
    println!("Ошибка транзакции: {}", t(MessageKey::TransactionError));
    println!("Таймаут блокировки: {}", t(MessageKey::LockTimeout));
    println!(
        "Обнаружена взаимоблокировка: {}",
        t(MessageKey::DeadlockDetected)
    );

    println!("\nСообщения транзакций:");
    println!("Транзакция начата: {}", t(MessageKey::TransactionStarted));
    println!(
        "Транзакция подтверждена: {}",
        t(MessageKey::TransactionCommitted)
    );
    println!(
        "Транзакция откачена: {}",
        t(MessageKey::TransactionRolledBack)
    );

    println!("\nСообщения блокировок:");
    println!("Блокировка получена: {}", t(MessageKey::LockAcquired));
    println!("Блокировка освобождена: {}", t(MessageKey::LockReleased));
    println!("Ожидание блокировки: {}", t(MessageKey::LockWaiting));

    // Демонстрация работы с русским языком
    println!("\n--- Русский язык ---");
    set_language(Language::Russian)?;

    println!("Приветствие: {}", t(MessageKey::Welcome));
    println!("Ошибка: {}", t(MessageKey::Error));
    println!("Успех: {}", t(MessageKey::Success));
    println!("Предупреждение: {}", t(MessageKey::Warning));
    println!("Информация: {}", t(MessageKey::Info));

    println!("\nСообщения об ошибках:");
    println!("Ошибка базы данных: {}", t(MessageKey::DatabaseError));
    println!("Ошибка подключения: {}", t(MessageKey::ConnectionError));
    println!("Ошибка транзакции: {}", t(MessageKey::TransactionError));
    println!("Таймаут блокировки: {}", t(MessageKey::LockTimeout));
    println!(
        "Обнаружена взаимоблокировка: {}",
        t(MessageKey::DeadlockDetected)
    );

    println!("\nСообщения транзакций:");
    println!("Транзакция начата: {}", t(MessageKey::TransactionStarted));
    println!(
        "Транзакция подтверждена: {}",
        t(MessageKey::TransactionCommitted)
    );
    println!(
        "Транзакция откачена: {}",
        t(MessageKey::TransactionRolledBack)
    );

    println!("\nСообщения блокировок:");
    println!("Блокировка получена: {}", t(MessageKey::LockAcquired));
    println!("Блокировка освобождена: {}", t(MessageKey::LockReleased));
    println!("Ожидание блокировки: {}", t(MessageKey::LockWaiting));

    // Демонстрация сообщений с параметрами
    println!("\n--- Сообщения с параметрами ---");

    // Пример с английским языком
    set_language(Language::English)?;
    println!(
        "Английский: {}",
        t_with_params(MessageKey::Welcome, &["RustDB"])
    );

    // Пример с русским языком
    set_language(Language::Russian)?;
    println!(
        "Русский: {}",
        t_with_params(MessageKey::Welcome, &["RustDB"])
    );

    // Демонстрация получения текущего языка
    println!("\n--- Информация о языке ---");
    let current_lang = I18N.get_language()?;
    println!("Текущий язык: {}", current_lang);
    println!("Название языка: {}", I18N.get_language_name(current_lang));

    // Демонстрация поддерживаемых языков
    println!("\nПоддерживаемые языки:");
    for lang in I18nManager::supported_languages() {
        println!("  {} - {}", lang, I18N.get_language_name(lang));
    }

    // Демонстрация переключения языка
    println!("\n--- Переключение языка ---");

    println!("Переключение на английский...");
    set_language(Language::English)?;
    println!("Текущий язык: {}", I18N.get_language()?);
    println!("Приветствие: {}", t(MessageKey::Welcome));

    println!("\nПереключение на русский...");
    set_language(Language::Russian)?;
    println!("Текущий язык: {}", I18N.get_language()?);
    println!("Приветствие: {}", t(MessageKey::Welcome));

    println!("\n=== Пример завершен ===");

    Ok(())
}
