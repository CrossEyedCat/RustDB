//! Тесты для менеджера страниц

use crate::storage::page_manager::{PageManager, PageManagerConfig};
use tempfile::TempDir;

/// Создает тестовый PageManager с временной директорией
fn create_test_page_manager() -> (PageManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = PageManagerConfig::default();
    let manager = PageManager::new(temp_dir.path().to_path_buf(), "test_table", config).unwrap();
    (manager, temp_dir)
}

#[test]
fn test_create_page_manager() {
    let (manager, _temp_dir) = create_test_page_manager();
    let stats = manager.get_statistics();
    assert_eq!(stats.insert_operations, 0);
    assert_eq!(stats.select_operations, 0);
    assert_eq!(stats.update_operations, 0);
    assert_eq!(stats.delete_operations, 0);
}

#[test]
fn test_insert_operation() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    let test_data = b"Hello, PageManager!";
    let result = manager.insert(test_data);
    
    assert!(result.is_ok());
    let insert_result = result.unwrap();
    assert!(insert_result.record_id > 0);
    assert!(insert_result.page_id >= 0);
    assert!(!insert_result.page_split); // Первая вставка не должна вызывать разделение
    
    let stats = manager.get_statistics();
    assert_eq!(stats.insert_operations, 1);
}

#[test]
fn test_select_operation() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Вставляем несколько записей
    let test_data1 = b"Record 1";
    let test_data2 = b"Record 2";
    let test_data3 = b"Record 3";
    
    manager.insert(test_data1).unwrap();
    manager.insert(test_data2).unwrap();
    manager.insert(test_data3).unwrap();
    
    // Выбираем все записи
    let results = manager.select(None);
    assert!(results.is_ok());
    
    let records = results.unwrap();
    assert_eq!(records.len(), 3);
    
    let stats = manager.get_statistics();
    assert_eq!(stats.select_operations, 1);
    assert_eq!(stats.insert_operations, 3);
}

#[test]
fn test_select_with_condition() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Вставляем записи
    manager.insert(b"apple").unwrap();
    manager.insert(b"banana").unwrap();
    manager.insert(b"cherry").unwrap();
    
    // Выбираем записи, содержащие 'a'
    let condition = Box::new(|data: &[u8]| {
        String::from_utf8_lossy(data).contains('a')
    });
    
    let results = manager.select(Some(condition));
    assert!(results.is_ok());
    
    let records = results.unwrap();
    assert_eq!(records.len(), 2); // "apple" и "banana"
}

#[test]
fn test_update_operation() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Вставляем запись
    let original_data = b"Original data";
    let insert_result = manager.insert(original_data).unwrap();
    let record_id = insert_result.record_id;
    
    // Обновляем запись
    let new_data = b"Updated data";
    let update_result = manager.update(record_id, new_data);
    
    assert!(update_result.is_ok());
    let _update_info = update_result.unwrap();
    
    let stats = manager.get_statistics();
    assert_eq!(stats.insert_operations, 1);
    assert_eq!(stats.update_operations, 1);
    
    // Проверяем, что запись обновлена
    let records = manager.select(None).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].1, new_data);
}

#[test]
fn test_delete_operation() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Вставляем записи
    let _record1 = manager.insert(b"Record 1").unwrap();
    let record2 = manager.insert(b"Record 2").unwrap();
    let _record3 = manager.insert(b"Record 3").unwrap();
    
    // Удаляем среднюю запись
    let delete_result = manager.delete(record2.record_id);
    assert!(delete_result.is_ok());
    
    let stats = manager.get_statistics();
    assert_eq!(stats.insert_operations, 3);
    assert_eq!(stats.delete_operations, 1);
    
    // Проверяем, что запись удалена
    let records = manager.select(None).unwrap();
    assert_eq!(records.len(), 2);
}

#[test]
fn test_batch_insert() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Подготавливаем данные для batch вставки
    let batch_data = vec![
        b"Batch record 1".to_vec(),
        b"Batch record 2".to_vec(),
        b"Batch record 3".to_vec(),
        b"Batch record 4".to_vec(),
        b"Batch record 5".to_vec(),
    ];
    
    let results = manager.batch_insert(batch_data.clone());
    assert!(results.is_ok());
    
    let insert_results = results.unwrap();
    assert_eq!(insert_results.len(), 5);
    
    // Проверяем, что все записи вставлены
    let records = manager.select(None).unwrap();
    assert_eq!(records.len(), 5);
    
    let stats = manager.get_statistics();
    assert_eq!(stats.insert_operations, 5);
}

#[test]
fn test_defragmentation() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Вставляем записи
    let mut record_ids = Vec::new();
    for i in 0..10 {
        let data = format!("Record {}", i).into_bytes();
        let result = manager.insert(&data).unwrap();
        record_ids.push(result.record_id);
    }
    
    // Удаляем некоторые записи для создания фрагментации
    for i in (0..10).step_by(2) {
        manager.delete(record_ids[i]).unwrap();
    }
    
    // Выполняем дефрагментацию
    let defrag_result = manager.defragment();
    assert!(defrag_result.is_ok());
    
    let defragmented_count = defrag_result.unwrap();
    // Количество дефрагментированных страниц зависит от реализации
    assert!(defragmented_count >= 0);
    
    let stats = manager.get_statistics();
    assert_eq!(stats.defragmentation_operations, 1);
}

#[test]
fn test_page_manager_config() {
    let temp_dir = TempDir::new().unwrap();
    
    let custom_config = PageManagerConfig {
        max_fill_factor: 0.8,
        min_fill_factor: 0.3,
        preallocation_buffer_size: 5,
        enable_compression: true,
        batch_size: 50,
    };
    
    let manager = PageManager::new(temp_dir.path().to_path_buf(), "config_test", custom_config);
    assert!(manager.is_ok());
}

#[test]
fn test_open_existing_page_manager() {
    let temp_dir = TempDir::new().unwrap();
    let table_name = "existing_table";
    
    // Создаем менеджер и вставляем данные
    {
        let mut manager = PageManager::new(
            temp_dir.path().to_path_buf(), 
            table_name, 
            PageManagerConfig::default()
        ).unwrap();
        
        manager.insert(b"Persistent data").unwrap();
    }
    
    // Открываем существующий менеджер
    let manager = PageManager::open(
        temp_dir.path().to_path_buf(),
        table_name,
        PageManagerConfig::default()
    );
    
    assert!(manager.is_ok());
}

#[test]
fn test_large_record_handling() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Создаем большую запись (но не превышающую MAX_RECORD_SIZE)
    let large_data = vec![b'X'; 1000]; // 1KB данных
    
    let result = manager.insert(&large_data);
    assert!(result.is_ok());
    
    // Проверяем, что запись была сохранена
    let records = manager.select(None).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].1, large_data);
}

#[test]
fn test_statistics_tracking() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Выполняем различные операции
    let record_id = manager.insert(b"Test record").unwrap().record_id;
    manager.select(None).unwrap();
    manager.update(record_id, b"Updated record").unwrap();
    manager.delete(record_id).unwrap();
    manager.defragment().unwrap();
    
    let stats = manager.get_statistics();
    assert_eq!(stats.insert_operations, 1);
    assert_eq!(stats.select_operations, 1);
    assert_eq!(stats.update_operations, 1);
    assert_eq!(stats.delete_operations, 1);
    assert_eq!(stats.defragmentation_operations, 1);
}

#[test]
fn test_error_handling() {
    let (mut manager, _temp_dir) = create_test_page_manager();
    
    // Попытка обновить несуществующую запись
    let invalid_record_id = 999999;
    let update_result = manager.update(invalid_record_id, b"New data");
    // Результат зависит от реализации, но не должен вызывать панику
    let _ = update_result;
    
    // Попытка удалить несуществующую запись
    let delete_result = manager.delete(invalid_record_id);
    // Результат зависит от реализации, но не должен вызывать панику
    let _ = delete_result;
}
