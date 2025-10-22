//! Тесты для менеджера статистики

use crate::catalog::statistics::{
    ColumnStatistics, ColumnValue, HistogramBucket, StatisticsManager, StatisticsSettings,
    TableStatistics, ValueDistribution,
};

#[test]
fn test_statistics_manager_creation() {
    let manager = StatisticsManager::new();
    assert!(manager.is_ok());
}

#[test]
fn test_statistics_manager_with_settings() {
    let settings = StatisticsSettings {
        auto_update: false,
        update_interval_seconds: 7200,
        max_histogram_buckets: 200,
        enable_debug_logging: true,
    };

    let manager = StatisticsManager::with_settings(settings);
    assert!(manager.is_ok());

    let manager = manager.unwrap();
    assert_eq!(manager.settings().auto_update, false);
    assert_eq!(manager.settings().update_interval_seconds, 7200);
    assert_eq!(manager.settings().max_histogram_buckets, 200);
    assert_eq!(manager.settings().enable_debug_logging, true);
}

#[test]
fn test_table_statistics_collection() {
    let mut manager = StatisticsManager::new().unwrap();
    let stats = manager.collect_table_statistics("users");
    assert!(stats.is_ok());

    let stats = stats.unwrap();
    assert_eq!(stats.table_name, "users");
    assert_eq!(stats.total_rows, 10000);
    assert_eq!(stats.total_size_bytes, 1024 * 1024);
    assert!(stats.column_statistics.contains_key("id"));
    assert!(stats.column_statistics.contains_key("name"));
    assert!(stats.column_statistics.contains_key("age"));
    assert!(stats.column_statistics.contains_key("salary"));
}

#[test]
fn test_column_statistics_creation() {
    let manager = StatisticsManager::new().unwrap();

    // Тестируем создание статистики для колонки id
    let id_stats = manager.create_column_statistics("id", 10000, 0, 1, 10000);
    assert_eq!(id_stats.column_name, "id");
    assert_eq!(id_stats.distinct_values, 10000);
    assert_eq!(id_stats.null_count, 0);
    assert_eq!(id_stats.min_value, Some(ColumnValue::Integer(1)));
    assert_eq!(id_stats.max_value, Some(ColumnValue::Integer(10000)));

    match id_stats.value_distribution {
        ValueDistribution::Uniform { step } => assert_eq!(step, 1.0),
        _ => panic!("Ожидалось равномерное распределение для id"),
    }

    // Тестируем создание статистики для колонки age
    let age_stats = manager.create_column_statistics("age", 100, 0, 18, 65);
    assert_eq!(age_stats.column_name, "age");
    assert_eq!(age_stats.distinct_values, 100);
    assert_eq!(age_stats.min_value, Some(ColumnValue::Integer(18)));
    assert_eq!(age_stats.max_value, Some(ColumnValue::Integer(65)));

    match age_stats.value_distribution {
        ValueDistribution::Normal { mean, std_dev } => {
            assert_eq!(mean, 35.0);
            assert_eq!(std_dev, 10.0);
        }
        _ => panic!("Ожидалось нормальное распределение для age"),
    }
}

#[test]
fn test_selectivity_estimation() {
    let mut manager = StatisticsManager::new().unwrap();
    manager.collect_table_statistics("users").unwrap();

    // Тестируем оценку селективности для различных операторов
    let selectivity_eq = manager.estimate_selectivity("users", "id", "=").unwrap();
    assert!(selectivity_eq > 0.0);
    assert!(selectivity_eq <= 1.0);

    let selectivity_neq = manager.estimate_selectivity("users", "id", "!=").unwrap();
    assert!(selectivity_neq > 0.0);
    assert!(selectivity_neq <= 1.0);

    let selectivity_range = manager.estimate_selectivity("users", "age", ">").unwrap();
    assert_eq!(selectivity_range, 0.5);

    let selectivity_like = manager
        .estimate_selectivity("users", "name", "LIKE")
        .unwrap();
    assert_eq!(selectivity_like, 0.1);

    let selectivity_in = manager.estimate_selectivity("users", "id", "IN").unwrap();
    assert_eq!(selectivity_in, 0.2);

    let selectivity_unknown = manager
        .estimate_selectivity("users", "id", "UNKNOWN")
        .unwrap();
    assert_eq!(selectivity_unknown, 0.1);
}

#[test]
fn test_result_rows_estimation() {
    let mut manager = StatisticsManager::new().unwrap();
    manager.collect_table_statistics("users").unwrap();

    // Тестируем оценку количества строк результата
    let rows_eq = manager.estimate_result_rows("users", "id", "=").unwrap();
    assert!(rows_eq > 0);
    assert!(rows_eq <= 10000);

    let rows_range = manager.estimate_result_rows("users", "age", ">").unwrap();
    assert!(rows_range > 0);
    assert!(rows_range <= 10000);

    // Тестируем случай с несуществующей таблицей
    let rows_unknown = manager
        .estimate_result_rows("unknown_table", "id", "=")
        .unwrap();
    assert_eq!(rows_unknown, "unknown_table".len());
}

#[test]
fn test_statistics_update() {
    let mut manager = StatisticsManager::new().unwrap();

    // Собираем статистику
    let stats1 = manager.collect_table_statistics("users").unwrap();
    let last_updated1 = stats1.last_updated;

    // Обновляем статистику
    manager.update_table_statistics("users").unwrap();
    let stats2 = manager.get_table_statistics("users").unwrap();
    let last_updated2 = stats2.last_updated;

    // Время обновления должно измениться
    assert!(last_updated2 > last_updated1);
}

#[test]
fn test_statistics_cache() {
    let mut manager = StatisticsManager::new().unwrap();

    // Собираем статистику для нескольких таблиц
    manager.collect_table_statistics("users").unwrap();
    manager.collect_table_statistics("orders").unwrap();

    // Проверяем, что статистика сохранена в кэше
    assert!(manager.get_table_statistics("users").is_some());
    assert!(manager.get_table_statistics("orders").is_some());

    // Очищаем кэш
    manager.clear_cache();

    // Проверяем, что кэш очищен
    assert!(manager.get_table_statistics("users").is_none());
    assert!(manager.get_table_statistics("orders").is_none());
}

#[test]
fn test_statistics_settings_update() {
    let mut manager = StatisticsManager::new().unwrap();
    let original_settings = manager.settings().clone();

    // Создаем новые настройки
    let new_settings = StatisticsSettings {
        auto_update: false,
        update_interval_seconds: 1800,
        max_histogram_buckets: 150,
        enable_debug_logging: true,
    };

    // Обновляем настройки
    manager.update_settings(new_settings.clone());

    // Проверяем, что настройки обновились
    assert_eq!(manager.settings().auto_update, new_settings.auto_update);
    assert_eq!(
        manager.settings().update_interval_seconds,
        new_settings.update_interval_seconds
    );
    assert_eq!(
        manager.settings().max_histogram_buckets,
        new_settings.max_histogram_buckets
    );
    assert_eq!(
        manager.settings().enable_debug_logging,
        new_settings.enable_debug_logging
    );

    // Проверяем, что настройки отличаются от исходных
    assert_ne!(
        manager.settings().auto_update,
        original_settings.auto_update
    );
    assert_ne!(
        manager.settings().update_interval_seconds,
        original_settings.update_interval_seconds
    );
}

#[test]
fn test_should_update_statistics() {
    let mut manager = StatisticsManager::new().unwrap();

    // По умолчанию auto_update = true
    assert!(manager.settings().auto_update);

    // Сразу после создания не должно требовать обновления
    assert!(!manager.should_update_statistics());

    // Отключаем автообновление
    let settings = StatisticsSettings {
        auto_update: false,
        ..Default::default()
    };
    manager.update_settings(settings);

    // При отключенном автообновлении не должно требовать обновления
    assert!(!manager.should_update_statistics());
}

#[test]
fn test_value_distribution_variants() {
    // Тестируем все варианты распределения значений

    let uniform = ValueDistribution::Uniform { step: 2.5 };
    match uniform {
        ValueDistribution::Uniform { step } => assert_eq!(step, 2.5),
        _ => panic!("Ожидалось равномерное распределение"),
    }

    let normal = ValueDistribution::Normal {
        mean: 42.0,
        std_dev: 5.0,
    };
    match normal {
        ValueDistribution::Normal { mean, std_dev } => {
            assert_eq!(mean, 42.0);
            assert_eq!(std_dev, 5.0);
        }
        _ => panic!("Ожидалось нормальное распределение"),
    }

    let histogram = ValueDistribution::Histogram {
        buckets: vec![HistogramBucket {
            lower_bound: 0.0,
            upper_bound: 10.0,
            count: 100,
            distinct_count: 50,
        }],
    };
    match histogram {
        ValueDistribution::Histogram { buckets } => {
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].lower_bound, 0.0);
            assert_eq!(buckets[0].upper_bound, 10.0);
            assert_eq!(buckets[0].count, 100);
            assert_eq!(buckets[0].distinct_count, 50);
        }
        _ => panic!("Ожидалась гистограмма"),
    }

    let unknown = ValueDistribution::Unknown;
    match unknown {
        ValueDistribution::Unknown => (), // OK
        _ => panic!("Ожидалось неизвестное распределение"),
    }
}

#[test]
fn test_column_value_variants() {
    // Тестируем все варианты значений колонок

    let int_val = ColumnValue::Integer(42);
    match int_val {
        ColumnValue::Integer(val) => assert_eq!(val, 42),
        _ => panic!("Ожидалось целое число"),
    }

    let float_val = ColumnValue::Float(3.14);
    match float_val {
        ColumnValue::Float(val) => assert_eq!(val, 3.14),
        _ => panic!("Ожидалось число с плавающей точкой"),
    }

    let string_val = ColumnValue::String("test".to_string());
    match string_val {
        ColumnValue::String(val) => assert_eq!(val, "test"),
        _ => panic!("Ожидалась строка"),
    }

    let bool_val = ColumnValue::Boolean(true);
    match bool_val {
        ColumnValue::Boolean(val) => assert_eq!(val, true),
        _ => panic!("Ожидалось булево значение"),
    }

    let null_val = ColumnValue::Null;
    match null_val {
        ColumnValue::Null => (), // OK
        _ => panic!("Ожидалось NULL значение"),
    }
}
