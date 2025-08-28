//! Пример использования оптимизации I/O в RustBD
//!
//! Этот пример демонстрирует:
//! - Буферизованные операции записи
//! - Асинхронные операции чтения/записи
//! - Кэширование страниц с LRU политикой
//! - Предвыборку данных
//! - Мониторинг производительности

use rustbd::storage::{
    io_optimization::{BufferedIoManager, IoBufferConfig},
    optimized_file_manager::OptimizedFileManager,
    database_file::{DatabaseFileType, ExtensionStrategy, BLOCK_SIZE},
};
use rustbd::common::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use rand::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Пример оптимизации I/O операций RustBD ===\n");

    // Демонстрация буферизованного I/O менеджера
    demonstrate_buffered_io().await?;
    
    // Демонстрация оптимизированного менеджера файлов
    demonstrate_optimized_file_manager().await?;
    
    // Демонстрация производительности кэша (упрощенная версия)
    demonstrate_cache_performance_simple().await?;
    
    // Демонстрация мониторинга и статистики
    demonstrate_monitoring().await?;

    println!("\n🎉 Пример успешно завершен!");
    Ok(())
}

async fn demonstrate_buffered_io() -> Result<()> {
    println!("💾 === Демонстрация буферизованного I/O ===");
    
    // Создаем конфигурацию с настройками производительности
    let mut config = IoBufferConfig::default();
    config.max_write_buffer_size = 500;
    config.max_buffer_time = Duration::from_millis(50);
    config.page_cache_size = 1000;
    config.enable_prefetch = true;
    config.prefetch_window_size = 5;
    
    println!("📋 Конфигурация I/O:");
    println!("   - Размер буфера записи: {} операций", config.max_write_buffer_size);
    println!("   - Время буферизации: {:?}", config.max_buffer_time);
    println!("   - Размер кэша страниц: {} страниц", config.page_cache_size);
    println!("   - Предвыборка: {} (окно: {})", config.enable_prefetch, config.prefetch_window_size);
    
    let manager = BufferedIoManager::new(config);
    
    println!("\n📝 Выполняем операции записи...");
    let start_time = Instant::now();
    
    // Выполняем пакет операций записи
    for i in 0..100 {
        let data = vec![(i % 256) as u8; BLOCK_SIZE];
        manager.write_page_async(1, i, data).await?;
        
        if i % 20 == 0 {
            print!(".");
        }
    }
    
    let write_time = start_time.elapsed();
    println!("\n   ✅ Записано 100 страниц за {:?}", write_time);
    
    println!("\n📖 Выполняем операции чтения...");
    let start_time = Instant::now();
    
    // Читаем данные (должны попадать в кэш)
    for i in 0..100 {
        let _data = manager.read_page_async(1, i).await?;
        
        if i % 20 == 0 {
            print!(".");
        }
    }
    
    let read_time = start_time.elapsed();
    println!("\n   ✅ Прочитано 100 страниц за {:?}", read_time);
    
    // Получаем статистику
    let stats = manager.get_statistics();
    println!("\n📊 Статистика I/O:");
    println!("   - Всего операций: {}", stats.total_operations);
    println!("   - Операций записи: {}", stats.write_operations);
    println!("   - Операций чтения: {}", stats.read_operations);
    println!("   - Попаданий в кэш: {}", stats.cache_hits);
    println!("   - Промахов кэша: {}", stats.cache_misses);
    println!("   - Коэффициент попаданий: {:.2}%", stats.cache_hit_ratio * 100.0);
    
    let (buffer_used, buffer_max, cache_size) = manager.get_buffer_info();
    println!("   - Использование буфера: {}/{}", buffer_used, buffer_max);
    println!("   - Размер кэша: {} страниц", cache_size);
    
    Ok(())
}

async fn demonstrate_optimized_file_manager() -> Result<()> {
    println!("\n🚀 === Демонстрация оптимизированного менеджера файлов ===");
    
    let temp_dir = tempfile::TempDir::new().unwrap();
    let manager = OptimizedFileManager::new(temp_dir.path())?;
    
    // Создаем файлы с разными стратегиями расширения
    let strategies = [
        ("Фиксированная", ExtensionStrategy::Fixed),
        ("Линейная", ExtensionStrategy::Linear),
        ("Экспоненциальная", ExtensionStrategy::Exponential),
        ("Адаптивная", ExtensionStrategy::Adaptive),
    ];
    
    for (name, strategy) in &strategies {
        println!("\n📁 Создаем файл со стратегией: {}", name);
        
        let file_id = manager.create_database_file(
            &format!("{}_test.db", name.to_lowercase()),
            DatabaseFileType::Data,
            123,
            *strategy,
        ).await?;
        
        // Выделяем страницы
        let start_page = manager.allocate_pages(file_id, 50).await?;
        println!("   ✅ Выделено 50 страниц, начиная с {}", start_page);
        
        // Записываем данные
        let test_data = vec![42u8; BLOCK_SIZE];
        let start_time = Instant::now();
        
        for i in 0..50 {
            manager.write_page(file_id, start_page + i, &test_data).await?;
        }
        
        let write_time = start_time.elapsed();
        println!("   ✅ Записано 50 страниц за {:?}", write_time);
        
        // Читаем данные
        let start_time = Instant::now();
        
        for i in 0..50 {
            let _data = manager.read_page(file_id, start_page + i).await?;
        }
        
        let read_time = start_time.elapsed();
        println!("   ✅ Прочитано 50 страниц за {:?}", read_time);
        
        // Получаем информацию о файле
        if let Some(file_info) = manager.get_file_info(file_id).await {
            println!("   📊 Информация о файле:");
            println!("      - Всего страниц: {}", file_info.total_pages);
            println!("      - Используемых страниц: {}", file_info.used_pages);
            println!("      - Свободных страниц: {}", file_info.free_pages);
            println!("      - Коэффициент использования: {:.1}%", file_info.utilization_ratio * 100.0);
            println!("      - Коэффициент фрагментации: {:.1}%", file_info.fragmentation_ratio * 100.0);
        }
    }
    
    Ok(())
}

async fn demonstrate_cache_performance_simple() -> Result<()> {
    println!("\n🔄 === Демонстрация производительности кэша ===");
    
    let temp_dir = tempfile::TempDir::new().unwrap();
    let manager = OptimizedFileManager::new(temp_dir.path())?;
    
    let file_id = manager.create_database_file(
        "cache_demo.db",
        DatabaseFileType::Data,
        456,
        ExtensionStrategy::Adaptive,
    ).await?;
    
    // Подготавливаем тестовые данные
    let page_count = 200;
    let start_page = manager.allocate_pages(file_id, page_count).await?;
    
    println!("📝 Записываем {} страниц...", page_count);
    for i in 0..page_count {
        let data = vec![(i % 256) as u8; BLOCK_SIZE];
        manager.write_page(file_id, start_page + i as u64, &data).await?;
    }
    
    // Тестируем простые паттерны доступа
    let access_patterns = [
        ("Последовательный", (0..page_count).collect::<Vec<_>>()),
        ("Обратный", (0..page_count).rev().collect::<Vec<_>>()),
    ];
    
    for (pattern_name, pattern) in &access_patterns {
        println!("\n🔍 Тестируем паттерн: {}", pattern_name);
        
        // Очищаем кэш для чистого теста
        manager.clear_io_cache().await;
        
        let start_time = Instant::now();
        
        for &page_offset in pattern {
            let _data = manager.read_page(file_id, start_page + page_offset as u64).await?;
        }
        
        let access_time = start_time.elapsed();
        let stats = manager.get_io_statistics();
        
        println!("   ⏱️  Время доступа: {:?}", access_time);
        println!("   📊 Попаданий в кэш: {} ({:.1}%)", 
                 stats.cache_hits, stats.cache_hit_ratio * 100.0);
        println!("   📊 Промахов кэша: {}", stats.cache_misses);
        
        let avg_time_per_op = access_time.as_nanos() / pattern.len() as u128;
        println!("   ⚡ Среднее время на операцию: {} нс", avg_time_per_op);
    }
    
    Ok(())
}

async fn demonstrate_monitoring() -> Result<()> {
    println!("\n📈 === Демонстрация мониторинга и статистики ===");
    
    let temp_dir = tempfile::TempDir::new().unwrap();
    let manager = OptimizedFileManager::new(temp_dir.path())?;
    
    // Создаем несколько файлов
    let mut file_ids = Vec::new();
    for i in 0..3 {
        let file_id = manager.create_database_file(
            &format!("monitor_test_{}.db", i),
            DatabaseFileType::Data,
            100 + i,
            ExtensionStrategy::Adaptive,
        ).await?;
        file_ids.push(file_id);
    }
    
    println!("🏃 Выполняем интенсивную нагрузку...");
    
    // Симулируем рабочую нагрузку
    let workload_start = Instant::now();
    
    for round in 0..5 {
        println!("   Раунд {}/5", round + 1);
        
        for &file_id in &file_ids {
            // Выделяем страницы
            let start_page = manager.allocate_pages(file_id, 20).await?;
            
            // Записываем данные
            for i in 0..20 {
                let data = vec![(round * 20 + i) as u8; BLOCK_SIZE];
                manager.write_page(file_id, start_page + i as u64, &data).await?;
            }
            
            // Читаем данные (смесь новых и старых)
            for i in 0..30 {
                let page_id = if i < 20 {
                    start_page + i as u64
                } else {
                    // Читаем старые данные
                    if start_page > 0 { start_page / 2 } else { 0 }
                };
                let _data = manager.read_page(file_id, page_id).await?;
            }
        }
        
        // Периодическое обслуживание
        if round % 2 == 0 {
            let extended_files = manager.maintenance_check().await?;
            if !extended_files.is_empty() {
                println!("   🔧 Расширено файлов: {}", extended_files.len());
            }
        }
        
        sleep(Duration::from_millis(100)).await;
    }
    
    let workload_time = workload_start.elapsed();
    println!("   ✅ Нагрузка завершена за {:?}", workload_time);
    
    // Получаем комбинированную статистику
    let stats = manager.get_combined_statistics().await;
    
    println!("\n📊 Комбинированная статистика:");
    println!("   - Всего файлов: {}", stats.total_files);
    println!("   - Всего страниц: {}", stats.total_pages);
    println!("   - Операций чтения: {}", stats.total_reads);
    println!("   - Операций записи: {}", stats.total_writes);
    println!("   - Коэффициент попаданий в кэш: {:.1}%", stats.cache_hit_ratio * 100.0);
    println!("   - Средняя утилизация: {:.1}%", stats.average_utilization * 100.0);
    println!("   - Средняя фрагментация: {:.1}%", stats.average_fragmentation * 100.0);
    println!("   - Использование буфера: {:.1}%", stats.buffer_usage * 100.0);
    println!("   - Размер кэша: {} страниц", stats.cache_usage);
    
    if stats.read_throughput > 0.0 {
        println!("   - Пропускная способность чтения: {:.1} МБ/с", 
                 stats.read_throughput / 1_000_000.0);
    }
    if stats.write_throughput > 0.0 {
        println!("   - Пропускная способность записи: {:.1} МБ/с", 
                 stats.write_throughput / 1_000_000.0);
    }
    
    // Оценка производительности
    let performance_score = stats.performance_score();
    println!("\n⭐ Оценка производительности: {:.1}% ({})", 
             performance_score * 100.0,
             match performance_score {
                 s if s >= 0.9 => "Отлично",
                 s if s >= 0.8 => "Хорошо",
                 s if s >= 0.7 => "Удовлетворительно",
                 s if s >= 0.6 => "Требует внимания",
                 _ => "Требует оптимизации",
             });
    
    // Рекомендации по оптимизации
    let recommendations = stats.get_recommendations();
    println!("\n💡 Рекомендации по оптимизации:");
    for (i, recommendation) in recommendations.iter().enumerate() {
        println!("   {}. {}", i + 1, recommendation);
    }
    
    // Проверка целостности
    println!("\n🔍 Проверка целостности файлов...");
    let validation_results = manager.validate_all().await?;
    
    let mut valid_files = 0;
    let mut invalid_files = 0;
    
    for (file_id, result) in validation_results {
        match result {
            Ok(_) => {
                valid_files += 1;
                println!("   ✅ Файл {} корректен", file_id);
            }
            Err(e) => {
                invalid_files += 1;
                println!("   ❌ Файл {} поврежден: {}", file_id, e);
            }
        }
    }
    
    println!("\n📋 Результат проверки:");
    println!("   - Корректных файлов: {}", valid_files);
    println!("   - Поврежденных файлов: {}", invalid_files);
    
    if invalid_files == 0 {
        println!("   🎉 Все файлы прошли проверку целостности!");
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_io_optimization_example() -> Result<()> {
        // Запускаем основную функцию как тест
        main().await
    }

    #[tokio::test]
    async fn test_performance_comparison() -> Result<()> {
        let temp_dir = tempfile::TempDir::new().unwrap();
        
        // Тестируем обычный менеджер vs оптимизированный
        let optimized_manager = OptimizedFileManager::new(temp_dir.path())?;
        
        let file_id = optimized_manager.create_database_file(
            "perf_test.db",
            DatabaseFileType::Data,
            999,
            ExtensionStrategy::Adaptive,
        ).await?;
        
        let data = vec![123u8; BLOCK_SIZE];
        let operations = 100;
        
        // Тестируем оптимизированный менеджер
        let start_time = Instant::now();
        
        let start_page = optimized_manager.allocate_pages(file_id, operations).await?;
        
        for i in 0..operations {
            optimized_manager.write_page(file_id, start_page + i as u64, &data).await?;
        }
        
        for i in 0..operations {
            let _read_data = optimized_manager.read_page(file_id, start_page + i as u64).await?;
        }
        
        let optimized_time = start_time.elapsed();
        
        // Получаем статистику
        let stats = optimized_manager.get_combined_statistics().await;
        
        println!("Оптимизированный менеджер:");
        println!("  Время: {:?}", optimized_time);
        println!("  Коэффициент попаданий в кэш: {:.1}%", stats.cache_hit_ratio * 100.0);
        println!("  Оценка производительности: {:.1}%", stats.performance_score() * 100.0);
        
        // Проверяем, что производительность приемлемая
        assert!(stats.performance_score() > 0.5, "Производительность слишком низкая");
        
        Ok(())
    }
}
