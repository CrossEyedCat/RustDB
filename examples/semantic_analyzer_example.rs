//! An example of using the rustdb semantic analyzer

use rustdb::analyzer::{AnalysisContext, SemanticAnalyzer, SemanticAnalyzerSettings};
use rustdb::common::Result;
use rustdb::parser::SqlParser;

fn main() -> Result<()> {
    println!("=== Example of using rustdb semantic analyzer ===\n");

    // 1. Creating an analyzer with default settings
    println!("1. Creating a semantic analyzer:");
    let mut analyzer = SemanticAnalyzer::default();
    let settings = analyzer.settings();
    println!(
        "Checking the existence of objects: {}",
        settings.check_object_existence
    );
    println!("Type checking: {}", settings.check_types);
    println!("Permission check: {}", settings.check_access_rights);
    println!(
        "Metadata caching: {}",
        settings.enable_metadata_cache
    );

    // 2. Analysis of a simple SELECT query
    println!("\n2. Analysis of a simple SELECT query:");
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;

    let context = AnalysisContext::default();
    let result = analyzer.analyze(&statement, &context)?;

    println!("The request is valid: {}", result.is_valid);
    println!("Number of errors: {}", result.errors.len());
    println!("Number of warnings: {}", result.warnings.len());
    println!(
        "Checked objects: {}",
        result.statistics.objects_checked
    );
    println!("Type checks: {}", result.statistics.type_checks);
    println!(
        "Analysis time: {} ms",
        result.statistics.analysis_time_ms
    );

    // 3. SELECT analysis with columns and WHERE
    println!("\n3. SELECT analysis with columns and WHERE:");
    let mut parser = SqlParser::new("SELECT name, email, age FROM users WHERE active = true")?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("The request is valid: {}", result.is_valid);
    println!(
        "Result types: {} columns",
        result.type_info.result_types.len()
    );
    for (i, data_type) in result.type_info.result_types.iter().enumerate() {
        println!("Column {}: {:?}", i + 1, data_type);
    }

    // 4. Analysis of the INSERT request
    println!("\n4. Analysis of the INSERT query:");
    let mut parser = SqlParser::new(
        "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25)",
    )?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("The request is valid: {}", result.is_valid);
    println!(
        "Checked objects: {}",
        result.statistics.objects_checked
    );
    println!("Type checks: {}", result.statistics.type_checks);

    // 5. Analysis of the UPDATE request
    println!("\n5. Analysis of the UPDATE request:");
    let mut parser = SqlParser::new("UPDATE users SET name = 'Bob', age = age + 1 WHERE id = 1")?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("The request is valid: {}", result.is_valid);
    if !result.warnings.is_empty() {
        println!("Warnings:");
        for warning in &result.warnings {
            println!("     - {:?}: {}", warning.warning_type, warning.message);
        }
    }

    // 6. Analysis of the CREATE TABLE query
    println!("\n6. Analysis of the CREATE TABLE query:");
    let mut parser = SqlParser::new("CREATE TABLE products (id INTEGER, name TEXT)")?;
    let statement = parser.parse()?;

    let result = analyzer.analyze(&statement, &context)?;
    println!("The request is valid: {}", result.is_valid);
    println!(
        "Checked objects: {}",
        result.statistics.objects_checked
    );

    // 7. Analysis of multiple requests
    println!("\n7. Analysis of multiple requests:");
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
    println!("Queries analyzed: {}", results.len());
    for (i, result) in results.iter().enumerate() {
        println!(
            "Request {}: valid={}, errors={}, warnings={}",
            i + 1,
            result.is_valid,
            result.errors.len(),
            result.warnings.len()
        );
    }

    // 8. Creating an analyzer with custom settings
    println!("\n8. Analyzer with custom settings:");
    let custom_settings = SemanticAnalyzerSettings {
        check_object_existence: true,
        check_types: true,
        check_access_rights: true,
        enable_metadata_cache: true,
        strict_validation: false,
        max_warnings: 50,
    };

    let mut custom_analyzer = SemanticAnalyzer::new(custom_settings);

    // Create a context with the user
    let custom_context = AnalysisContext {
        current_user: Some("test_user".to_string()),
        ..Default::default()
    };

    let mut parser = SqlParser::new("SELECT * FROM sensitive_data")?;
    let statement = parser.parse()?;

    let result = custom_analyzer.analyze(&statement, &custom_context)?;
    println!("The request is valid: {}", result.is_valid);
    println!("Access checks: {}", result.statistics.access_checks);

    if !result.errors.is_empty() {
        println!("Errors:");
        for error in &result.errors {
            println!("     - {:?}: {}", error.error_type, error.message);
            if let Some(fix) = &error.suggested_fix {
                println!("Offer: {}", fix);
            }
        }
    }

    // 9. Cache statistics
    println!("\n9. Analyzer cache statistics:");
    let (hits, misses) = analyzer.cache_statistics();
    println!("Cache hits: {}", hits);
    println!("Cache misses: {}", misses);
    if hits + misses > 0 {
        let hit_rate = hits as f64 / (hits + misses) as f64 * 100.0;
        println!("Hit percentage: {:.1}%", hit_rate);
    }

    // 10. Updating analyzer settings
    println!("\n10. Updating analyzer settings:");
    let new_settings = SemanticAnalyzerSettings {
        check_object_existence: false,
        check_types: true,
        check_access_rights: false,
        enable_metadata_cache: false,
        strict_validation: true,
        max_warnings: 10,
    };

    analyzer.update_settings(new_settings);
    let updated_settings = analyzer.settings();
    println!(
        "Checking objects: {}",
        updated_settings.check_object_existence
    );
    println!("Caching: {}", updated_settings.enable_metadata_cache);
    println!(
        "Strong validation: {}",
        updated_settings.strict_validation
    );

    // 11. Clear cache
    println!("\n11. Clearing the analyzer cache:");
    analyzer.clear_cache();
    let (hits_after, misses_after) = analyzer.cache_statistics();
    println!(
        "The cache has been cleared. Hits: {}, Misses: {}",
        hits_after, misses_after
    );

    println!("\n=== Example completed ===");
    Ok(())
}
