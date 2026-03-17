//! Пример использования менеджера страниц rustdb
//!
//! Демонстрирует основные возможности PageManager:
//! - Создание и открытие менеджера страниц
//! - CRUD операции (вставка, чтение, обновление, удаление)
//! - Batch операции
//! - Дефрагментация страниц
//! - Мониторинг статистики

use rustdb::storage::page_manager::{PageManager, PageManagerConfig};
use std::path::Path;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗄️  Пример использования PageManager rustdb");
    println!("{}", "=".repeat(50));

    // Создаем временную директорию для примера
    let temp_dir = TempDir::new()?;
    let data_dir = temp_dir.path().to_path_buf();
    println!("📁 Рабочая директория: {:?}", data_dir);

    // Демонстрируем создание PageManager
    demo_create_page_manager(&data_dir)?;

    // Демонстрируем CRUD операции
    demo_crud_operations(&data_dir)?;

    // Демонстрируем batch операции
    demo_batch_operations(&data_dir)?;

    // Демонстрируем дефрагментацию
    demo_defragmentation(&data_dir)?;

    // Демонстрируем открытие существующего менеджера
    demo_open_existing_manager(&data_dir)?;

    println!("\n✅ Все демонстрации завершены успешно!");

    Ok(())
}

/// Демонстрирует создание PageManager с различными конфигурациями
fn demo_create_page_manager(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧 Демонстрация создания PageManager");
    println!("{}", "-".repeat(30));

    // Создаем менеджер с конфигурацией по умолчанию
    let default_config = PageManagerConfig::default();
    println!("📊 Конфигурация по умолчанию:");
    println!("   - max_fill_factor: {}", default_config.max_fill_factor);
    println!("   - min_fill_factor: {}", default_config.min_fill_factor);
    println!(
        "   - preallocation_buffer_size: {}",
        default_config.preallocation_buffer_size
    );
    println!(
        "   - enable_compression: {}",
        default_config.enable_compression
    );
    println!("   - batch_size: {}", default_config.batch_size);

    let manager_result = PageManager::new(data_dir.to_path_buf(), "demo_table", default_config);
    match manager_result {
        Ok(_manager) => {
            println!("✅ PageManager создан успешно");
        }
        Err(e) => {
            println!("❌ Ошибка создания PageManager: {}", e);
            return Ok(()); // Продолжаем выполнение даже при ошибке
        }
    }

    // Создаем менеджер с кастомной конфигурацией
    let custom_config = PageManagerConfig {
        max_fill_factor: 0.85,
        min_fill_factor: 0.25,
        preallocation_buffer_size: 20,
        enable_compression: true,
        batch_size: 200,
        buffer_pool_size: 1000,
    };

    println!("\n📊 Кастомная конфигурация:");
    println!("   - max_fill_factor: {}", custom_config.max_fill_factor);
    println!("   - min_fill_factor: {}", custom_config.min_fill_factor);
    println!(
        "   - preallocation_buffer_size: {}",
        custom_config.preallocation_buffer_size
    );
    println!(
        "   - enable_compression: {}",
        custom_config.enable_compression
    );
    println!("   - batch_size: {}", custom_config.batch_size);

    let custom_manager_result =
        PageManager::new(data_dir.to_path_buf(), "custom_table", custom_config);
    match custom_manager_result {
        Ok(_manager) => {
            println!("✅ PageManager с кастомной конфигурацией создан успешно");
        }
        Err(e) => {
            println!("❌ Ошибка создания кастомного PageManager: {}", e);
        }
    }

    Ok(())
}

/// Демонстрирует CRUD операции
fn demo_crud_operations(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📝 Демонстрация CRUD операций");
    println!("{}", "-".repeat(30));

    let config = PageManagerConfig::default();
    let manager_result = PageManager::new(data_dir.to_path_buf(), "crud_table", config);

    let mut manager = match manager_result {
        Ok(mgr) => mgr,
        Err(e) => {
            println!("❌ Не удалось создать PageManager: {}", e);
            return Ok(());
        }
    };

    // CREATE (INSERT) операции
    println!("📥 Вставка записей:");
    let records = [
        "Alice Johnson - Software Engineer".as_bytes(),
        "Bob Smith - Data Analyst".as_bytes(),
        "Carol Davis - Project Manager".as_bytes(),
        "David Wilson - DevOps Engineer".as_bytes(),
    ];

    let mut record_ids = Vec::new();
    for (i, record) in records.iter().enumerate() {
        match manager.insert(record) {
            Ok(insert_result) => {
                println!(
                    "   ✅ Запись {}: ID {}, Страница {}",
                    i + 1,
                    insert_result.record_id,
                    insert_result.page_id
                );
                record_ids.push(insert_result.record_id);
            }
            Err(e) => {
                println!("   ❌ Ошибка вставки записи {}: {}", i + 1, e);
            }
        }
    }

    // READ (SELECT) операции
    println!("\n📤 Чтение всех записей:");
    match manager.select(None) {
        Ok(all_records) => {
            println!("   📊 Найдено {} записей:", all_records.len());
            for (i, (record_id, data)) in all_records.iter().enumerate() {
                let data_str = String::from_utf8_lossy(data);
                println!("   {} - ID: {}, Данные: {}", i + 1, record_id, data_str);
            }
        }
        Err(e) => {
            println!("   ❌ Ошибка чтения записей: {}", e);
        }
    }

    // Чтение с условием
    println!("\n🔍 Чтение записей с фильтром (содержат 'Engineer'):");
    let condition = Box::new(|data: &[u8]| String::from_utf8_lossy(data).contains("Engineer"));

    match manager.select(Some(condition)) {
        Ok(filtered_records) => {
            println!(
                "   📊 Найдено {} записей с 'Engineer':",
                filtered_records.len()
            );
            for (i, (record_id, data)) in filtered_records.iter().enumerate() {
                let data_str = String::from_utf8_lossy(data);
                println!("   {} - ID: {}, Данные: {}", i + 1, record_id, data_str);
            }
        }
        Err(e) => {
            println!("   ❌ Ошибка фильтрованного чтения: {}", e);
        }
    }

    // UPDATE операции
    if !record_ids.is_empty() {
        println!("\n✏️  Обновление записи:");
        let record_to_update = record_ids[0];
        let new_data = "Alice Johnson - Senior Software Engineer (Updated)".as_bytes();

        match manager.update(record_to_update, new_data) {
            Ok(update_result) => {
                println!("   ✅ Запись {} обновлена", record_to_update);
                if update_result.in_place {
                    println!("   📍 Обновление выполнено на месте");
                } else {
                    println!(
                        "   🔄 Запись перемещена на страницу {:?}",
                        update_result.new_page_id
                    );
                }
            }
            Err(e) => {
                println!("   ❌ Ошибка обновления: {}", e);
            }
        }
    }

    // DELETE операции
    if record_ids.len() > 1 {
        println!("\n🗑️  Удаление записи:");
        let record_to_delete = record_ids[1];

        match manager.delete(record_to_delete) {
            Ok(delete_result) => {
                println!("   ✅ Запись {} удалена", record_to_delete);
                if delete_result.physical_delete {
                    println!("   🗑️  Физическое удаление");
                } else {
                    println!("   👻 Логическое удаление");
                }
                if delete_result.page_merge {
                    println!("   🔄 Выполнено объединение страниц");
                }
            }
            Err(e) => {
                println!("   ❌ Ошибка удаления: {}", e);
            }
        }
    }

    // Показываем статистику
    let stats = manager.get_statistics();
    println!("\n📈 Статистика операций:");
    println!("   - Вставки: {}", stats.insert_operations);
    println!("   - Чтения: {}", stats.select_operations);
    println!("   - Обновления: {}", stats.update_operations);
    println!("   - Удаления: {}", stats.delete_operations);

    Ok(())
}

/// Демонстрирует batch операции
fn demo_batch_operations(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📦 Демонстрация batch операций");
    println!("{}", "-".repeat(30));

    let config = PageManagerConfig {
        batch_size: 10, // Небольшой размер batch для демонстрации
        ..PageManagerConfig::default()
    };

    let manager_result = PageManager::new(data_dir.to_path_buf(), "batch_table", config);
    let mut manager = match manager_result {
        Ok(mgr) => mgr,
        Err(e) => {
            println!("❌ Не удалось создать PageManager: {}", e);
            return Ok(());
        }
    };

    // Подготавливаем данные для batch вставки
    let batch_data: Vec<Vec<u8>> = (1..=25)
        .map(|i| format!("Batch Record #{:03} - Generated Data", i).into_bytes())
        .collect();

    println!("📥 Batch вставка {} записей:", batch_data.len());

    match manager.batch_insert(batch_data.clone()) {
        Ok(results) => {
            println!("   ✅ Успешно обработано {} записей", results.len());

            let mut page_splits = 0;
            for result in &results {
                if result.page_split {
                    page_splits += 1;
                }
            }

            if page_splits > 0 {
                println!("   🔄 Произошло {} разделений страниц", page_splits);
            }

            // Показываем первые несколько записей
            println!("   📋 Первые записи:");
            for (i, result) in results.iter().take(5).enumerate() {
                println!(
                    "     {} - ID: {}, Страница: {}",
                    i + 1,
                    result.record_id,
                    result.page_id
                );
            }

            if results.len() > 5 {
                println!("     ... и еще {} записей", results.len() - 5);
            }
        }
        Err(e) => {
            println!("   ❌ Ошибка batch вставки: {}", e);
        }
    }

    // Проверяем результат
    match manager.select(None) {
        Ok(all_records) => {
            println!(
                "   📊 Общее количество записей в таблице: {}",
                all_records.len()
            );
        }
        Err(e) => {
            println!("   ❌ Ошибка проверки записей: {}", e);
        }
    }

    let stats = manager.get_statistics();
    println!("   📈 Статистика batch операций:");
    println!("     - Всего вставок: {}", stats.insert_operations);
    println!("     - Разделений страниц: {}", stats.page_splits);

    Ok(())
}

/// Демонстрирует дефрагментацию страниц
fn demo_defragmentation(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧 Демонстрация дефрагментации");
    println!("{}", "-".repeat(30));

    let config = PageManagerConfig::default();
    let manager_result = PageManager::new(data_dir.to_path_buf(), "defrag_table", config);
    let mut manager = match manager_result {
        Ok(mgr) => mgr,
        Err(e) => {
            println!("❌ Не удалось создать PageManager: {}", e);
            return Ok(());
        }
    };

    // Вставляем записи
    println!("📥 Создание записей для демонстрации фрагментации:");
    let mut record_ids = Vec::new();

    for i in 1..=15 {
        let data = format!("Fragmentation Test Record #{:02}", i).into_bytes();
        match manager.insert(&data) {
            Ok(result) => {
                record_ids.push(result.record_id);
            }
            Err(e) => {
                println!("   ❌ Ошибка вставки записи {}: {}", i, e);
            }
        }
    }

    println!("   ✅ Создано {} записей", record_ids.len());

    // Удаляем каждую вторую запись для создания фрагментации
    println!("\n🗑️  Удаление каждой второй записи (создание фрагментации):");
    let mut deleted_count = 0;

    for (i, &record_id) in record_ids.iter().enumerate() {
        if i % 2 == 1 {
            // Удаляем записи с нечетными индексами
            match manager.delete(record_id) {
                Ok(_) => {
                    deleted_count += 1;
                }
                Err(e) => {
                    println!("   ❌ Ошибка удаления записи {}: {}", record_id, e);
                }
            }
        }
    }

    println!("   ✅ Удалено {} записей", deleted_count);

    // Показываем статистику до дефрагментации
    let stats_before = manager.get_statistics();
    println!("\n📊 Статистика до дефрагментации:");
    println!(
        "   - Операций дефрагментации: {}",
        stats_before.defragmentation_operations
    );

    // Выполняем дефрагментацию
    println!("\n🔧 Выполнение дефрагментации:");
    match manager.defragment() {
        Ok(defragmented_count) => {
            println!("   ✅ Дефрагментировано страниц: {}", defragmented_count);
        }
        Err(e) => {
            println!("   ❌ Ошибка дефрагментации: {}", e);
        }
    }

    // Показываем статистику после дефрагментации
    let stats_after = manager.get_statistics();
    println!("\n📊 Статистика после дефрагментации:");
    println!(
        "   - Операций дефрагментации: {}",
        stats_after.defragmentation_operations
    );
    println!("   - Всего вставок: {}", stats_after.insert_operations);
    println!("   - Всего удалений: {}", stats_after.delete_operations);

    Ok(())
}

/// Демонстрирует открытие существующего менеджера
fn demo_open_existing_manager(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔓 Демонстрация открытия существующего менеджера");
    println!("{}", "-".repeat(30));

    let table_name = "persistent_table";
    let config = PageManagerConfig::default();

    // Создаем менеджер и добавляем данные
    println!("📝 Создание нового менеджера и добавление данных:");
    {
        let manager_result = PageManager::new(data_dir.to_path_buf(), table_name, config.clone());
        match manager_result {
            Ok(mut manager) => {
                // Добавляем несколько записей
                let persistent_data = [
                    "Persistent Record 1 - Will survive restart".as_bytes(),
                    "Persistent Record 2 - Stored on disk".as_bytes(),
                    "Persistent Record 3 - Available after reopen".as_bytes(),
                ];

                for (i, data) in persistent_data.iter().enumerate() {
                    match manager.insert(data) {
                        Ok(result) => {
                            println!("   ✅ Запись {}: ID {}", i + 1, result.record_id);
                        }
                        Err(e) => {
                            println!("   ❌ Ошибка записи {}: {}", i + 1, e);
                        }
                    }
                }

                let stats = manager.get_statistics();
                println!("   📊 Вставлено записей: {}", stats.insert_operations);
            }
            Err(e) => {
                println!("   ❌ Не удалось создать менеджер: {}", e);
                return Ok(());
            }
        }
    } // manager выходит из области видимости и закрывается

    // Открываем существующий менеджер
    println!("\n🔓 Открытие существующего менеджера:");
    match PageManager::open(data_dir.to_path_buf(), table_name, config) {
        Ok(mut manager) => {
            println!("   ✅ Менеджер успешно открыт");

            // Проверяем, что данные сохранились
            match manager.select(None) {
                Ok(records) => {
                    println!("   📊 Найдено {} сохраненных записей:", records.len());
                    for (i, (record_id, data)) in records.iter().enumerate() {
                        let data_str = String::from_utf8_lossy(data);
                        println!("     {} - ID: {}, Данные: {}", i + 1, record_id, data_str);
                    }
                }
                Err(e) => {
                    println!("   ❌ Ошибка чтения сохраненных данных: {}", e);
                }
            }

            // Добавляем новую запись
            match manager.insert("New Record - Added after reopen".as_bytes()) {
                Ok(result) => {
                    println!("   ✅ Новая запись добавлена: ID {}", result.record_id);
                }
                Err(e) => {
                    println!("   ❌ Ошибка добавления новой записи: {}", e);
                }
            }
        }
        Err(e) => {
            println!("   ❌ Не удалось открыть существующий менеджер: {}", e);
        }
    }

    Ok(())
}
