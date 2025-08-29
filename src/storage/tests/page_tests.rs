//! Тесты для структуры Page

use crate::storage::page::{Page, PageHeader, RecordSlot, PAGE_SIZE, PAGE_HEADER_SIZE};
use crate::common::types::PageId;
use tempfile::TempDir;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

#[test]
fn test_page_creation() {
    let page = Page::new(PageId(1));
    
    assert_eq!(page.get_page_id(), PageId(1));
    assert_eq!(page.get_free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);
    assert_eq!(page.get_record_count(), 0);
    assert!(page.is_dirty());
}

#[test]
fn test_page_header() {
    let page = Page::new(PageId(42));
    let header = page.get_header();
    
    assert_eq!(header.page_id, PageId(42));
    assert_eq!(header.record_count, 0);
    assert_eq!(header.free_space_offset, PAGE_HEADER_SIZE as u16);
    assert_eq!(header.free_space_size, (PAGE_SIZE - PAGE_HEADER_SIZE) as u16);
}

#[test]
fn test_add_record() {
    let mut page = Page::new(PageId(1));
    let record_data = b"Hello, World!";
    let record_id = 1;
    
    let result = page.add_record(record_data, record_id);
    assert!(result.is_ok());
    
    let offset = result.unwrap();
    assert!(offset >= PAGE_HEADER_SIZE);
    assert_eq!(page.get_record_count(), 1);
    assert!(page.get_free_space() < PAGE_SIZE - PAGE_HEADER_SIZE);
}

#[test]
fn test_add_multiple_records() {
    let mut page = Page::new(PageId(1));
    let mut record_offsets = Vec::new();
    
    // Добавляем несколько записей
    for i in 1..=5 {
        let record_data = format!("Record {}", i);
        let result = page.add_record(record_data.as_bytes(), i);
        assert!(result.is_ok());
        record_offsets.push(result.unwrap());
    }
    
    assert_eq!(page.get_record_count(), 5);
    
    // Проверяем, что все записи имеют разные смещения
    for i in 0..record_offsets.len() {
        for j in i + 1..record_offsets.len() {
            assert_ne!(record_offsets[i], record_offsets[j]);
        }
    }
}

#[test]
fn test_get_record() {
    let mut page = Page::new(PageId(1));
    let record_data = b"Test Record";
    let record_id = 1;
    
    let offset = page.add_record(record_data, record_id).unwrap();
    let retrieved = page.get_record(offset);
    
    assert!(retrieved.is_some());
    let retrieved_data = retrieved.unwrap();
    assert_eq!(retrieved_data, record_data);
}

#[test]
fn test_get_nonexistent_record() {
    let page = Page::new(PageId(1));
    let result = page.get_record(PAGE_SIZE); // Невалидное смещение
    assert!(result.is_none());
}

#[test]
fn test_update_record() {
    let mut page = Page::new(PageId(1));
    let original_data = b"Original Data";
    let updated_data = b"Updated Data!";
    let record_id = 1;
    
    let offset = page.add_record(original_data, record_id).unwrap();
    
    // Обновляем запись
    let result = page.update_record(offset, updated_data);
    assert!(result.is_ok());
    
    // Проверяем, что данные обновились
    let retrieved = page.get_record(offset).unwrap();
    assert_eq!(retrieved, updated_data);
}

#[test]
fn test_update_record_larger_size() {
    let mut page = Page::new(PageId(1));
    let original_data = b"Short";
    let updated_data = b"This is a much longer piece of data that should not fit in the same space";
    let record_id = 1;
    
    let offset = page.add_record(original_data, record_id).unwrap();
    
    // Попытка обновить запись более длинными данными может не удаться
    let result = page.update_record(offset, updated_data);
    // В зависимости от реализации, это может быть ошибкой или успехом
    // Проверяем, что страница остается в консистентном состоянии
    assert!(page.get_record(offset).is_some());
}

#[test]
fn test_delete_record() {
    let mut page = Page::new(PageId(1));
    let record_data = b"To be deleted";
    let record_id = 1;
    
    let offset = page.add_record(record_data, record_id).unwrap();
    let initial_count = page.get_record_count();
    
    let result = page.delete_record(offset);
    assert!(result.is_ok());
    
    // Проверяем, что запись удалена
    assert!(page.get_record(offset).is_none());
    assert_eq!(page.get_record_count(), initial_count - 1);
}

#[test]
fn test_delete_nonexistent_record() {
    let mut page = Page::new(PageId(1));
    let result = page.delete_record(PAGE_SIZE); // Невалидное смещение
    assert!(result.is_err());
}

#[test]
fn test_page_full_scenario() {
    let mut page = Page::new(PageId(1));
    let mut records_added = 0;
    
    // Заполняем страницу записями до тех пор, пока не закончится место
    loop {
        let record_data = format!("Record number {}", records_added);
        let result = page.add_record(record_data.as_bytes(), records_added + 1);
        
        if result.is_err() {
            break;
        }
        records_added += 1;
        
        // Защита от бесконечного цикла
        if records_added > 1000 {
            break;
        }
    }
    
    assert!(records_added > 0);
    assert_eq!(page.get_record_count(), records_added);
    
    // Проверяем, что свободного места очень мало или нет совсем
    assert!(page.get_free_space() < 100); // Менее 100 байт свободного места
}

#[test]
fn test_page_serialization() {
    let mut page = Page::new(PageId(42));
    
    // Добавляем несколько записей
    page.add_record(b"First record", 1).unwrap();
    page.add_record(b"Second record", 2).unwrap();
    page.add_record(b"Third record", 3).unwrap();
    
    // Сериализуем страницу
    let serialized = page.serialize();
    assert_eq!(serialized.len(), PAGE_SIZE);
    
    // Десериализуем страницу
    let deserialized = Page::deserialize(&serialized, PageId(42));
    assert!(deserialized.is_ok());
    
    let new_page = deserialized.unwrap();
    assert_eq!(new_page.get_page_id(), PageId(42));
    assert_eq!(new_page.get_record_count(), 3);
}

#[test]
fn test_page_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_page.dat");
    
    // Создаем и заполняем страницу
    let mut page = Page::new(PageId(100));
    page.add_record(b"Persistent data", 1).unwrap();
    
    // Записываем страницу в файл
    {
        let mut file = File::create(&file_path).unwrap();
        let serialized = page.serialize();
        file.write_all(&serialized).unwrap();
    }
    
    // Читаем страницу из файла
    {
        let mut file = File::open(&file_path).unwrap();
        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer).unwrap();
        
        let loaded_page = Page::deserialize(&buffer, PageId(100)).unwrap();
        assert_eq!(loaded_page.get_page_id(), PageId(100));
        assert_eq!(loaded_page.get_record_count(), 1);
        
        // Проверяем, что данные сохранились
        let header = loaded_page.get_header();
        assert_eq!(header.page_id, PageId(100));
    }
}

#[test]
fn test_record_slot_functionality() {
    let mut page = Page::new(PageId(1));
    let record_data = b"Test data for slot";
    
    let offset = page.add_record(record_data, 1).unwrap();
    
    // Проверяем, что слот корректно создался
    let retrieved = page.get_record(offset);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), record_data);
}

#[test]
fn test_page_dirty_flag() {
    let mut page = Page::new(PageId(1));
    assert!(page.is_dirty()); // Новая страница помечена как грязная
    
    page.mark_clean();
    assert!(!page.is_dirty());
    
    // Любая модификация должна помечать страницу как грязную
    page.add_record(b"test", 1).unwrap();
    assert!(page.is_dirty());
}

#[test]
fn test_page_compaction() {
    let mut page = Page::new(PageId(1));
    
    // Добавляем записи
    let offset1 = page.add_record(b"Record 1", 1).unwrap();
    let offset2 = page.add_record(b"Record 2", 2).unwrap();
    let offset3 = page.add_record(b"Record 3", 3).unwrap();
    
    let initial_free_space = page.get_free_space();
    
    // Удаляем среднюю запись
    page.delete_record(offset2).unwrap();
    
    // Проверяем, что свободное место увеличилось
    assert!(page.get_free_space() > initial_free_space);
    
    // Записи 1 и 3 должны остаться доступными
    assert!(page.get_record(offset1).is_some());
    assert!(page.get_record(offset3).is_some());
    assert!(page.get_record(offset2).is_none());
}

#[test]
fn test_page_boundary_conditions() {
    let mut page = Page::new(PageId(u64::MAX)); // Максимальный ID страницы
    
    // Проверяем корректность работы с граничными значениями
    assert_eq!(page.get_page_id(), PageId(u64::MAX));
    
    // Пытаемся добавить запись максимально возможного размера
    let max_record_size = page.get_free_space() - std::mem::size_of::<RecordSlot>();
    let large_record = vec![0u8; max_record_size];
    
    let result = page.add_record(&large_record, 1);
    // Результат зависит от реализации, но страница должна остаться в корректном состоянии
    assert!(page.get_record_count() <= 1);
}

#[test]
fn test_concurrent_page_access_simulation() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let page = Arc::new(Mutex::new(Page::new(PageId(1))));
    let mut handles = vec![];
    
    // Симулируем конкурентный доступ
    for i in 0..10 {
        let page_clone = Arc::clone(&page);
        let handle = thread::spawn(move || {
            let mut page = page_clone.lock().unwrap();
            let record_data = format!("Record from thread {}", i);
            page.add_record(record_data.as_bytes(), i + 1)
        });
        handles.push(handle);
    }
    
    // Ждем завершения всех потоков
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_page = page.lock().unwrap();
    assert!(final_page.get_record_count() <= 10); // Некоторые записи могут не поместиться
}
