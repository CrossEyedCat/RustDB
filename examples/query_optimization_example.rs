//! Пример использования расширенной оптимизации запросов для RustDB

use rustdb::catalog::{StatisticsManager, ValueDistribution};
use rustdb::planner::planner::{
    ExecutionPlan, FilterNode, PlanMetadata, PlanNode, PlanStatistics, TableScanNode,
};
use rustdb::planner::{AdvancedOptimizerSettings, AdvancedQueryOptimizer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Пример расширенной оптимизации запросов RustDB ===\n");

    // Демонстрация менеджера статистики
    demo_statistics_manager()?;

    // Демонстрация расширенного оптимизатора
    demo_advanced_optimizer()?;

    // Демонстрация оптимизации с использованием статистики
    demo_optimization_with_statistics()?;

    println!("=== Демонстрация завершена успешно! ===");
    Ok(())
}

/// Демонстрация работы менеджера статистики
fn demo_statistics_manager() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Демонстрация менеджера статистики:");
    println!("   Создание менеджера статистики...");

    let mut stats_manager = StatisticsManager::new()?;

    // Собираем статистику для таблицы users
    println!("   Сбор статистики для таблицы 'users'...");
    let table_stats = stats_manager.collect_table_statistics("users")?;

    println!("   Статистика таблицы 'users':");
    println!("     - Общее количество строк: {}", table_stats.total_rows);
    println!(
        "     - Размер таблицы: {} байт",
        table_stats.total_size_bytes
    );
    println!(
        "     - Количество колонок: {}",
        table_stats.column_statistics.len()
    );

    // Показываем статистику по колонкам
    for (col_name, col_stats) in &table_stats.column_statistics {
        println!("     - Колонка '{}':", col_name);
        println!(
            "       * Уникальных значений: {}",
            col_stats.distinct_values
        );
        println!("       * NULL значений: {}", col_stats.null_count);
        println!("       * Минимальное значение: {:?}", col_stats.min_value);
        println!("       * Максимальное значение: {:?}", col_stats.max_value);

        match &col_stats.value_distribution {
            ValueDistribution::Uniform { step } => {
                println!("       * Распределение: равномерное (шаг: {})", step);
            }
            ValueDistribution::Normal { mean, std_dev } => {
                println!(
                    "       * Распределение: нормальное (среднее: {}, ст.откл.: {})",
                    mean, std_dev
                );
            }
            ValueDistribution::Histogram { buckets } => {
                println!(
                    "       * Распределение: гистограмма ({} корзин)",
                    buckets.len()
                );
            }
            ValueDistribution::Unknown => {
                println!("       * Распределение: неизвестно");
            }
        }
    }

    // Демонстрируем оценку селективности
    println!("\n   Оценка селективности:");
    let selectivity_eq = stats_manager.estimate_selectivity("users", "id", "=")?;
    let selectivity_range = stats_manager.estimate_selectivity("users", "age", ">")?;
    let selectivity_like = stats_manager.estimate_selectivity("users", "name", "LIKE")?;

    println!("     - id = ?: {:.4}", selectivity_eq);
    println!("     - age > ?: {:.4}", selectivity_range);
    println!("     - name LIKE ?: {:.4}", selectivity_like);

    // Демонстрируем оценку количества строк результата
    println!("\n   Оценка количества строк результата:");
    let rows_eq = stats_manager.estimate_result_rows("users", "id", "=")?;
    let rows_range = stats_manager.estimate_result_rows("users", "age", ">")?;

    println!("     - id = ?: {} строк", rows_eq);
    println!("     - age > ?: {} строк", rows_range);

    println!();
    Ok(())
}

/// Демонстрация работы расширенного оптимизатора
fn demo_advanced_optimizer() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Демонстрация расширенного оптимизатора:");
    println!("   Создание расширенного оптимизатора...");

    let settings = AdvancedOptimizerSettings {
        enable_statistics_usage: true,
        enable_query_rewriting: true,
        enable_expression_simplification: true,
        enable_subquery_extraction: true,
        enable_debug_logging: true,
        cost_threshold: 500.0,
    };

    let optimizer = AdvancedQueryOptimizer::with_settings(settings)?;

    println!("   Настройки оптимизатора:");
    println!(
        "     - Использование статистики: {}",
        optimizer.settings().enable_statistics_usage
    );
    println!(
        "     - Перезапись запросов: {}",
        optimizer.settings().enable_query_rewriting
    );
    println!(
        "     - Упрощение выражений: {}",
        optimizer.settings().enable_expression_simplification
    );
    println!(
        "     - Вынесение подзапросов: {}",
        optimizer.settings().enable_subquery_extraction
    );
    println!(
        "     - Порог стоимости: {}",
        optimizer.settings().cost_threshold
    );

    println!();
    Ok(())
}

/// Демонстрация оптимизации с использованием статистики
fn demo_optimization_with_statistics() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Демонстрация оптимизации с использованием статистики:");

    // Создаем простой план выполнения
    println!("   Создание тестового плана выполнения...");
    let test_plan = create_test_execution_plan()?;

    println!("   Исходный план:");
    println!(
        "     - Оценка стоимости: {:.2}",
        test_plan.metadata.estimated_cost
    );
    println!(
        "     - Оценка количества строк: {}",
        test_plan.metadata.estimated_rows
    );
    println!(
        "     - Количество операторов: {}",
        test_plan.metadata.statistics.operator_count
    );

    // Создаем оптимизатор и применяем оптимизацию
    println!("\n   Применение расширенной оптимизации...");
    let mut optimizer = AdvancedQueryOptimizer::new()?;

    // Собираем статистику для таблиц в плане
    let stats_manager = optimizer.statistics_manager_mut();
    stats_manager.collect_table_statistics("users")?;
    stats_manager.collect_table_statistics("orders")?;

    // Применяем оптимизацию
    let optimization_result = optimizer.optimize_with_statistics(test_plan)?;

    println!("   Результат оптимизации:");
    println!(
        "     - Количество примененных оптимизаций: {}",
        optimization_result.statistics.optimizations_applied
    );
    println!(
        "     - Время оптимизации: {} мс",
        optimization_result.statistics.optimization_time_ms
    );
    println!(
        "     - Улучшение стоимости: {:.2}%",
        optimization_result.statistics.cost_improvement_percent
    );
    println!(
        "     - Перезаписи запросов: {}",
        optimization_result.statistics.query_rewrites
    );
    println!(
        "     - Упрощения выражений: {}",
        optimization_result.statistics.expression_simplifications
    );
    println!(
        "     - Вынесения подзапросов: {}",
        optimization_result.statistics.subquery_extractions
    );
    println!(
        "     - Использования статистики: {}",
        optimization_result.statistics.statistics_usage_count
    );

    // Показываем сообщения об оптимизациях
    if !optimization_result.messages.is_empty() {
        println!("\n   Примененные оптимизации:");
        for (i, message) in optimization_result.messages.iter().enumerate() {
            println!("     {}. {}", i + 1, message);
        }
    }

    // Показываем использованную статистику
    if !optimization_result.used_statistics.is_empty() {
        println!("\n   Использованная статистика:");
        for (i, stat_info) in optimization_result.used_statistics.iter().enumerate() {
            println!("     {}. {}", i + 1, stat_info);
        }
    }

    // Показываем оптимизированный план
    println!("\n   Оптимизированный план:");
    println!(
        "     - Оценка стоимости: {:.2}",
        optimization_result.optimized_plan.metadata.estimated_cost
    );
    println!(
        "     - Оценка количества строк: {}",
        optimization_result.optimized_plan.metadata.estimated_rows
    );

    println!();
    Ok(())
}

/// Создание тестового плана выполнения
fn create_test_execution_plan() -> Result<ExecutionPlan, Box<dyn std::error::Error>> {
    // Создаем узлы плана
    let users_scan = PlanNode::TableScan(TableScanNode {
        table_name: "users".to_string(),
        alias: Some("u".to_string()),
        columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
        filter: None,
        cost: 1000.0,
        estimated_rows: 10000,
    });

    let _orders_scan = PlanNode::TableScan(TableScanNode {
        table_name: "orders".to_string(),
        alias: Some("o".to_string()),
        columns: vec![
            "id".to_string(),
            "user_id".to_string(),
            "amount".to_string(),
        ],
        filter: None,
        cost: 800.0,
        estimated_rows: 50000,
    });

    let filter_node = PlanNode::Filter(FilterNode {
        condition: "u.age > 18 AND o.amount > 100".to_string(),
        input: Box::new(users_scan),
        selectivity: 0.3,
        cost: 300.0,
    });

    // Создаем метаданные плана
    let metadata = PlanMetadata {
        estimated_cost: 2100.0,
        estimated_rows: 1500,
        created_at: std::time::SystemTime::now(),
        statistics: PlanStatistics {
            operator_count: 3,
            max_depth: 2,
            table_count: 2,
            join_count: 0,
        },
    };

    Ok(ExecutionPlan {
        root: filter_node,
        metadata,
    })
}
