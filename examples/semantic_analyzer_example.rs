//! Пример использования семантического анализатора rustdb

use rustdb::analyzer::{AnalysisContext, SemanticAnalyzer, SemanticAnalyzerSettings};
use rustdb::common::Result;
use rustdb::parser::SqlParser;

fn main() -> Result<()> {
    println!("=== Пример использования семантического анализатора rustdb ===\n");

    // 1. Создание анализатора с настройками по умолчанию
    println!("1. Создание семантического анализатора:");
    let mut analyzer = SemanticAnalyzer::default();
    let settings = analyzer.settings();
    println!(
        "   Проверка существования объектов: {}",
        settings.check_object_existence
    );
    println!("   Проверка типов: {}", settings.check_types);
    println!("   Проверка прав доступа: {}", settings.check_access_rights);
    println!(
        "   Кэширование метаданных: {}",
        settings.enable_metadata_cache
    );

    // 2. Анализ простого SELECT запроса
    println!("\n2. Анализ простого SELECT запроса:");
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;

    let context = AnalysisContext::default();
    let result = analyzer.analyze(&statement, &context)?;

    println!("   Запрос валиден: {}", result.is_valid);
    println!("   Количество ошибок: {}", result.errors.len());
    println!("   Количество предупреждений: {}", result.warnings.len());
    println!(
        "   Проверено объектов: {}",
        result.statistics.objects_checked
    );
    println!("   Проверок типов: {}", result.statistics.type_checks);
    println!(
        "   Время анализа: {} мс",
        result.statistics.analysis_time_ms
    );

    // 3. Анализ SELECT с колонками и WHERE
    println!("\n3. Анализ SELECT с колонками и WHERE:");
    let mut parser = SqlParser::new("SELECT name, email, age FROM users WHERE active = true")?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("   Запрос валиден: {}", result.is_valid);
    println!(
        "   Типы результата: {} колонок",
        result.type_info.result_types.len()
    );
    for (i, data_type) in result.type_info.result_types.iter().enumerate() {
        println!("     Колонка {}: {:?}", i + 1, data_type);
    }

    // 4. Анализ INSERT запроса
    println!("\n4. Анализ INSERT запроса:");
    let mut parser = SqlParser::new(
        "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25)",
    )?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("   Запрос валиден: {}", result.is_valid);
    println!(
        "   Проверено объектов: {}",
        result.statistics.objects_checked
    );
    println!("   Проверок типов: {}", result.statistics.type_checks);

    // 5. Анализ UPDATE запроса
    println!("\n5. Анализ UPDATE запроса:");
    let mut parser = SqlParser::new("UPDATE users SET name = 'Bob', age = age + 1 WHERE id = 1")?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("   Запрос валиден: {}", result.is_valid);
    if !result.warnings.is_empty() {
        println!("   Предупреждения:");
        for warning in &result.warnings {
            println!("     - {:?}: {}", warning.warning_type, warning.message);
        }
    }

    // 6. Анализ CREATE TABLE запроса
    println!("\n6. Анализ CREATE TABLE запроса:");
    let mut parser = SqlParser::new("CREATE TABLE products (id INTEGER, name TEXT)")?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("   Запрос валиден: {}", result.is_valid);
    println!(
        "   Проверено объектов: {}",
        result.statistics.objects_checked
    );

    // 7. Анализ множественных запросов
    println!("\n7. Анализ множественных запросов:");
    let sql_statements = vec![
        "SELECT * FROM users",
        "INSERT INTO users (name) VALUES ('Charlie')",
        "UPDATE users SET active = true WHERE name = 'Charlie'",
        "DELETE FROM users WHERE age > 65",
    ];

    let mut statements = Vec::new();
    for sql in &sql_statements {
        let mut parser = SqlParser::new(sql)?;
        statements.push(parser.parse()?);
    }

    let results = analyzer.analyze_multiple(&statements, &context)?;
    println!("   Проанализировано запросов: {}", results.len());
    for (i, result) in results.iter().enumerate() {
        println!(
            "     Запрос {}: валиден={}, ошибок={}, предупреждений={}",
            i + 1,
            result.is_valid,
            result.errors.len(),
            result.warnings.len()
        );
    }

    // 8. Создание анализатора с кастомными настройками
    println!("\n8. Анализатор с кастомными настройками:");
    let custom_settings = SemanticAnalyzerSettings {
        check_object_existence: true,
        check_types: true,
        check_access_rights: true, // Включаем проверку прав доступа
        enable_metadata_cache: true,
        strict_validation: false,
        max_warnings: 50,
    };

    let mut custom_analyzer = SemanticAnalyzer::new(custom_settings);

    // Создаем контекст с пользователем
    let custom_context = AnalysisContext {
        current_user: Some("test_user".to_string()),
        ..Default::default()
    };

    let mut parser = SqlParser::new("SELECT * FROM sensitive_data")?;
    let statement = parser.parse()?;

    let result = custom_analyzer.analyze(&statement, &custom_context)?;
    println!("   Запрос валиден: {}", result.is_valid);
    println!("   Проверок доступа: {}", result.statistics.access_checks);

    if !result.errors.is_empty() {
        println!("   Ошибки:");
        for error in &result.errors {
            println!("     - {:?}: {}", error.error_type, error.message);
            if let Some(fix) = &error.suggested_fix {
                println!("       Предложение: {}", fix);
            }
        }
    }

    // 9. Статистика кэша
    println!("\n9. Статистика кэша анализатора:");
    let (hits, misses) = analyzer.cache_statistics();
    println!("   Попадания в кэш: {}", hits);
    println!("   Промахи кэша: {}", misses);
    if hits + misses > 0 {
        let hit_rate = hits as f64 / (hits + misses) as f64 * 100.0;
        println!("   Процент попаданий: {:.1}%", hit_rate);
    }

    // 10. Обновление настроек анализатора
    println!("\n10. Обновление настроек анализатора:");
    let new_settings = SemanticAnalyzerSettings {
        check_object_existence: false, // Отключаем проверку объектов
        check_types: true,
        check_access_rights: false,
        enable_metadata_cache: false, // Отключаем кэш
        strict_validation: true,      // Включаем строгую валидацию
        max_warnings: 10,
    };

    analyzer.update_settings(new_settings);
    let updated_settings = analyzer.settings();
    println!(
        "   Проверка объектов: {}",
        updated_settings.check_object_existence
    );
    println!("   Кэширование: {}", updated_settings.enable_metadata_cache);
    println!(
        "   Строгая валидация: {}",
        updated_settings.strict_validation
    );

    // 11. Очистка кэша
    println!("\n11. Очистка кэша анализатора:");
    analyzer.clear_cache();
    let (hits_after, misses_after) = analyzer.cache_statistics();
    println!(
        "   Кэш очищен. Попадания: {}, промахи: {}",
        hits_after, misses_after
    );

    println!("\n=== Пример завершен ===");
    Ok(())
}
