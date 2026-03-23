//! An example of using the RustDB internationalization system

use rustdb::common::{set_language, t, t_with_params, I18nManager, Language, MessageKey, I18N};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Example of the RustDB internationalization system ===\n");

    // Demonstration of working with English
    println!("--- English language ---");
    set_language(Language::English)?;

    println!("Greetings: {}", t(MessageKey::Welcome));
    println!("Error: {}", t(MessageKey::Error));
    println!("Success: {}", t(MessageKey::Success));
    println!("Warning: {}", t(MessageKey::Warning));
    println!("Information: {}", t(MessageKey::Info));

    println!("\nError messages:");
    println!("Database Error: {}", t(MessageKey::DatabaseError));
    println!("Connection error: {}", t(MessageKey::ConnectionError));
    println!("Transaction error: {}", t(MessageKey::TransactionError));
    println!("Lock timeout: {}", t(MessageKey::LockTimeout));
    println!(
        "Deadlock detected: {}",
        t(MessageKey::DeadlockDetected)
    );

    println!("\nTransaction messages:");
    println!("Transaction started: {}", t(MessageKey::TransactionStarted));
    println!(
        "Transaction confirmed: {}",
        t(MessageKey::TransactionCommitted)
    );
    println!(
        "Transaction rolled back: {}",
        t(MessageKey::TransactionRolledBack)
    );

    println!("\nBlocking messages:");
    println!("Lock received: {}", t(MessageKey::LockAcquired));
    println!("Lock released: {}", t(MessageKey::LockReleased));
    println!("Waiting for lock: {}", t(MessageKey::LockWaiting));

    // Demonstration of messages with parameters
    println!("\n--- Messages with parameters ---");

    // English example
    set_language(Language::English)?;
    println!(
        "English: {}",
        t_with_params(MessageKey::Welcome, &["RustDB"])
    );

    // Demonstration of getting the current language
    println!("\n--- Language information ---");
    let current_lang = I18N.get_language()?;
    println!("Current language: {}", current_lang);
    println!("Language name: {}", I18N.get_language_name(current_lang));

    // Demonstration of supported languages
    println!("\nSupported languages:");
    for lang in I18nManager::supported_languages() {
        println!("  {} - {}", lang, I18N.get_language_name(lang));
    }

    // Demonstration of language switching
    println!("\n--- Switch language ---");

    println!("Switch to English...");
    set_language(Language::English)?;
    println!("Current language: {}", I18N.get_language()?);
    println!("Greetings: {}", t(MessageKey::Welcome));

    println!("\n=== Example completed ===");

    Ok(())
}
