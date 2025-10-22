//! Пример использования планировщика запросов

use rustdb::planner::{QueryPlanner, QueryOptimizer};
use rustdb::parser::SqlParser;
use rustdb::common::Result;

fn main() -> Result<()> {
    println!("=== Пример использования планировщика запросов ===\n");

    // Создаем планировщик и оптимизатор
    let mut planner = QueryPlanner::new()?;
    let mut optimizer = QueryOptimizer::new()?;

    // Пример 1: Простой SELECT запрос
    println!("1. Создание плана для простого SELECT запроса:");
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    println!("   План создан:");
    println!("   - Оценка стоимости: {:.2}", plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", plan.metadata.estimated_rows);
    println!("   - Количество операторов: {}", plan.metadata.statistics.operator_count);
    println!("   - Количество таблиц: {}", plan.metadata.statistics.table_count);
    println!();

    // Пример 2: SELECT с WHERE условием
    println!("2. Создание плана для SELECT с WHERE:");
    let mut parser = SqlParser::new("SELECT name, age FROM users WHERE age > 18")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    println!("   План создан:");
    println!("   - Оценка стоимости: {:.2}", plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", plan.metadata.estimated_rows);
    println!("   - Количество операторов: {}", plan.metadata.statistics.operator_count);
    println!();

    // Пример 3: INSERT запрос
    println!("3. Создание плана для INSERT:");
    let mut parser = SqlParser::new("INSERT INTO users (name, age) VALUES ('John', 25)")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    println!("   План создан:");
    println!("   - Оценка стоимости: {:.2}", plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", plan.metadata.estimated_rows);
    println!();

    // Пример 4: UPDATE запрос
    println!("4. Создание плана для UPDATE:");
    let mut parser = SqlParser::new("UPDATE users SET age = 26 WHERE name = 'John'")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    println!("   План создан:");
    println!("   - Оценка стоимости: {:.2}", plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", plan.metadata.estimated_rows);
    println!();

    // Пример 5: DELETE запрос
    println!("5. Создание плана для DELETE:");
    let mut parser = SqlParser::new("DELETE FROM users WHERE age < 18")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    println!("   План создан:");
    println!("   - Оценка стоимости: {:.2}", plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", plan.metadata.estimated_rows);
    println!();

    // Пример 6: Оптимизация плана
    println!("6. Оптимизация плана:");
    let mut parser = SqlParser::new("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > 18")?;
    let statement = parser.parse()?;
    let original_plan = planner.create_plan(&statement)?;
    
    println!("   Исходный план:");
    println!("   - Оценка стоимости: {:.2}", original_plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", original_plan.metadata.estimated_rows);
    println!("   - Количество JOIN: {}", original_plan.metadata.statistics.join_count);
    
    let optimization_result = optimizer.optimize(original_plan)?;
    
    println!("   Оптимизированный план:");
    println!("   - Оценка стоимости: {:.2}", optimization_result.optimized_plan.metadata.estimated_cost);
    println!("   - Оценка строк: {}", optimization_result.optimized_plan.metadata.estimated_rows);
    println!("   - Время оптимизации: {} мс", optimization_result.statistics.optimization_time_ms);
    println!("   - Улучшение стоимости: {:.2}%", optimization_result.statistics.cost_improvement_percent);
    println!("   - Применено оптимизаций: {}", optimization_result.statistics.optimizations_applied);
    
    if !optimization_result.messages.is_empty() {
        println!("   Сообщения об оптимизации:");
        for msg in &optimization_result.messages {
            println!("   - {}", msg);
        }
    }
    println!();

    // Пример 7: Настройки планировщика
    println!("7. Настройки планировщика:");
    let settings = planner.settings();
    println!("   - Кэширование планов: {}", settings.enable_plan_cache);
    println!("   - Максимальный размер кэша: {}", settings.max_cache_size);
    println!("   - Оптимизация: {}", settings.enable_optimization);
    println!("   - Максимальная глубина рекурсии: {}", settings.max_recursion_depth);
    println!();

    // Пример 8: Статистика кэша
    println!("8. Статистика кэша:");
    let cache_stats = planner.cache_stats();
    println!("   - Текущий размер: {}", cache_stats.size);
    println!("   - Максимальный размер: {}", cache_stats.max_size);
    println!();

    // Пример 9: Настройки оптимизатора
    println!("9. Настройки оптимизатора:");
    let opt_settings = optimizer.settings();
    println!("   - Перестановка JOIN: {}", opt_settings.enable_join_reordering);
    println!("   - Выбор индексов: {}", opt_settings.enable_index_selection);
    println!("   - Упрощение выражений: {}", opt_settings.enable_expression_simplification);
    println!("   - Выталкивание предикатов: {}", opt_settings.enable_predicate_pushdown);
    println!("   - Максимальные итерации: {}", opt_settings.max_optimization_iterations);
    println!("   - Порог стоимости: {:.2}", opt_settings.cost_threshold);
    println!();

    // Пример 10: Статистика оптимизатора
    println!("10. Статистика оптимизатора:");
    let opt_stats = optimizer.statistics();
    println!("   - Применено оптимизаций: {}", opt_stats.optimizations_applied);
    println!("   - Время оптимизации: {} мс", opt_stats.optimization_time_ms);
    println!("   - Улучшение стоимости: {:.2}%", opt_stats.cost_improvement_percent);
    println!("   - Перестановки JOIN: {}", opt_stats.join_reorders);
    println!("   - Применено индексов: {}", opt_stats.indexes_applied);
    println!("   - Упрощения выражений: {}", opt_stats.expression_simplifications);
    println!();

    println!("=== Пример завершен ===");
    Ok(())
}
