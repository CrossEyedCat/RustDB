//! Тесты для структуры Block

use crate::storage::block::{Block, BlockHeader, BLOCK_SIZE};
use crate::common::types::BlockId;
use std::io::{Read, Write, Cursor};

#[test]
fn test_block_creation() {
    let block = Block::new(BlockId(1));
    
    assert_eq!(block.get_id(), BlockId(1));
    assert_eq!(block.get_data().len(), BLOCK_SIZE);
    assert!(block.is_dirty());
}

#[test]
fn test_block_header() {
    let block = Block::new(BlockId(42));
    let header = block.get_header();
    
    assert_eq!(header.block_id, BlockId(42));
    assert_eq!(header.checksum, 0); // Изначально 0
}

#[test]
fn test_block_data_operations() {
    let mut block = Block::new(BlockId(1));
    let test_data = b"Hello, Block World!";
    
    // Записываем данные в блок
    let result = block.write_data(0, test_data);
    assert!(result.is_ok());
    
    // Читаем данные из блока
    let mut buffer = vec![0u8; test_data.len()];
    let result = block.read_data(0, &mut buffer);
    assert!(result.is_ok());
    assert_eq!(buffer, test_data);
}

#[test]
fn test_block_write_read_at_offset() {
    let mut block = Block::new(BlockId(1));
    let data1 = b"First part";
    let data2 = b"Second part";
    let offset1 = 0;
    let offset2 = 100;
    
    // Записываем данные в разные части блока
    block.write_data(offset1, data1).unwrap();
    block.write_data(offset2, data2).unwrap();
    
    // Читаем данные из разных частей
    let mut buffer1 = vec![0u8; data1.len()];
    let mut buffer2 = vec![0u8; data2.len()];
    
    block.read_data(offset1, &mut buffer1).unwrap();
    block.read_data(offset2, &mut buffer2).unwrap();
    
    assert_eq!(buffer1, data1);
    assert_eq!(buffer2, data2);
}

#[test]
fn test_block_boundary_write() {
    let mut block = Block::new(BlockId(1));
    let test_data = vec![0xAB; 100];
    
    // Записываем в конец блока
    let offset = BLOCK_SIZE - test_data.len();
    let result = block.write_data(offset, &test_data);
    assert!(result.is_ok());
    
    // Читаем обратно
    let mut buffer = vec![0u8; test_data.len()];
    block.read_data(offset, &mut buffer).unwrap();
    assert_eq!(buffer, test_data);
}

#[test]
fn test_block_write_beyond_boundary() {
    let mut block = Block::new(BlockId(1));
    let test_data = b"This data is too large for the remaining space";
    
    // Пытаемся записать за границы блока
    let offset = BLOCK_SIZE - 10; // Оставляем только 10 байт
    let result = block.write_data(offset, test_data);
    
    // Должна быть ошибка
    assert!(result.is_err());
}

#[test]
fn test_block_read_beyond_boundary() {
    let block = Block::new(BlockId(1));
    let mut buffer = vec![0u8; 100];
    
    // Пытаемся читать за границами блока
    let offset = BLOCK_SIZE - 50;
    let result = block.read_data(offset, &mut buffer);
    
    // Должна быть ошибка
    assert!(result.is_err());
}

#[test]
fn test_block_fill_completely() {
    let mut block = Block::new(BlockId(1));
    let fill_byte = 0x42;
    let fill_data = vec![fill_byte; BLOCK_SIZE];
    
    // Заполняем весь блок
    let result = block.write_data(0, &fill_data);
    assert!(result.is_ok());
    
    // Проверяем, что весь блок заполнен
    let mut buffer = vec![0u8; BLOCK_SIZE];
    block.read_data(0, &mut buffer).unwrap();
    assert_eq!(buffer, fill_data);
}

#[test]
fn test_block_partial_overwrite() {
    let mut block = Block::new(BlockId(1));
    let initial_data = b"Initial data that will be partially overwritten";
    let overwrite_data = b"OVERWRITE";
    let offset = 8; // Начинаем перезапись с позиции 8
    
    // Записываем исходные данные
    block.write_data(0, initial_data).unwrap();
    
    // Частично перезаписываем
    block.write_data(offset, overwrite_data).unwrap();
    
    // Проверяем результат
    let mut buffer = vec![0u8; initial_data.len()];
    block.read_data(0, &mut buffer).unwrap();
    
    // Первые 8 байт должны остаться неизменными
    assert_eq!(&buffer[0..offset], &initial_data[0..offset]);
    
    // Следующие байты должны быть перезаписаны
    assert_eq!(&buffer[offset..offset + overwrite_data.len()], overwrite_data);
}

#[test]
fn test_block_zero_data() {
    let mut block = Block::new(BlockId(1));
    
    // Записываем пустые данные
    let result = block.write_data(0, &[]);
    assert!(result.is_ok());
    
    // Читаем пустые данные
    let mut empty_buffer = vec![];
    let result = block.read_data(0, &mut empty_buffer);
    assert!(result.is_ok());
}

#[test]
fn test_block_serialization() {
    let mut block = Block::new(BlockId(42));
    let test_data = b"Serialization test data";
    
    // Записываем тестовые данные
    block.write_data(100, test_data).unwrap();
    
    // Сериализуем блок
    let serialized = block.serialize();
    assert_eq!(serialized.len(), BLOCK_SIZE);
    
    // Десериализуем блок
    let deserialized = Block::deserialize(&serialized, BlockId(42));
    assert!(deserialized.is_ok());
    
    let new_block = deserialized.unwrap();
    assert_eq!(new_block.get_id(), BlockId(42));
    
    // Проверяем, что данные сохранились
    let mut buffer = vec![0u8; test_data.len()];
    new_block.read_data(100, &mut buffer).unwrap();
    assert_eq!(buffer, test_data);
}

#[test]
fn test_block_checksum() {
    let mut block = Block::new(BlockId(1));
    let test_data = b"Data for checksum calculation";
    
    block.write_data(0, test_data).unwrap();
    
    // Вычисляем контрольную сумму
    let checksum = block.calculate_checksum();
    assert_ne!(checksum, 0); // Контрольная сумма не должна быть нулевой для непустых данных
    
    // Изменяем данные
    block.write_data(0, b"Different data").unwrap();
    
    // Контрольная сумма должна измениться
    let new_checksum = block.calculate_checksum();
    assert_ne!(checksum, new_checksum);
}

#[test]
fn test_block_dirty_flag() {
    let mut block = Block::new(BlockId(1));
    assert!(block.is_dirty()); // Новый блок помечен как грязный
    
    block.mark_clean();
    assert!(!block.is_dirty());
    
    // Любая запись должна помечать блок как грязный
    block.write_data(0, b"test").unwrap();
    assert!(block.is_dirty());
}

#[test]
fn test_block_stream_io() {
    let mut block = Block::new(BlockId(1));
    let test_data = b"Stream I/O test data";
    
    // Записываем данные через поток
    {
        let mut cursor = Cursor::new(block.get_data_mut());
        cursor.write_all(test_data).unwrap();
    }
    
    // Читаем данные через поток
    {
        let mut cursor = Cursor::new(block.get_data());
        let mut buffer = vec![0u8; test_data.len()];
        cursor.read_exact(&mut buffer).unwrap();
        assert_eq!(buffer, test_data);
    }
}

#[test]
fn test_multiple_blocks_independence() {
    let mut block1 = Block::new(BlockId(1));
    let mut block2 = Block::new(BlockId(2));
    
    let data1 = b"Data for block 1";
    let data2 = b"Data for block 2";
    
    // Записываем разные данные в разные блоки
    block1.write_data(0, data1).unwrap();
    block2.write_data(0, data2).unwrap();
    
    // Проверяем, что блоки независимы
    let mut buffer1 = vec![0u8; data1.len()];
    let mut buffer2 = vec![0u8; data2.len()];
    
    block1.read_data(0, &mut buffer1).unwrap();
    block2.read_data(0, &mut buffer2).unwrap();
    
    assert_eq!(buffer1, data1);
    assert_eq!(buffer2, data2);
    assert_ne!(buffer1, buffer2);
}

#[test]
fn test_block_pattern_write_read() {
    let mut block = Block::new(BlockId(1));
    
    // Записываем паттерн данных
    for i in 0..256 {
        let data = vec![i as u8; 10];
        let offset = i * 10;
        
        if offset + data.len() <= BLOCK_SIZE {
            block.write_data(offset, &data).unwrap();
        }
    }
    
    // Проверяем паттерн
    for i in 0..256 {
        let offset = i * 10;
        if offset + 10 <= BLOCK_SIZE {
            let mut buffer = vec![0u8; 10];
            block.read_data(offset, &mut buffer).unwrap();
            
            let expected = vec![i as u8; 10];
            assert_eq!(buffer, expected);
        }
    }
}

#[test]
fn test_block_concurrent_access_simulation() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let block = Arc::new(Mutex::new(Block::new(BlockId(1))));
    let mut handles = vec![];
    
    // Симулируем конкурентный доступ
    for i in 0..10 {
        let block_clone = Arc::clone(&block);
        let handle = thread::spawn(move || {
            let mut block = block_clone.lock().unwrap();
            let data = vec![i as u8; 100];
            let offset = i * 100;
            
            if offset + data.len() <= BLOCK_SIZE {
                block.write_data(offset, &data)
            } else {
                Ok(())
            }
        });
        handles.push(handle);
    }
    
    // Ждем завершения всех потоков
    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_ok());
    }
    
    // Проверяем результат
    let final_block = block.lock().unwrap();
    for i in 0..10 {
        let offset = i * 100;
        if offset + 100 <= BLOCK_SIZE {
            let mut buffer = vec![0u8; 100];
            final_block.read_data(offset, &mut buffer).unwrap();
            
            let expected = vec![i as u8; 100];
            assert_eq!(buffer, expected);
        }
    }
}

#[test]
fn test_block_persistence_integrity() {
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::{Write, Read};
    
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_block.dat");
    
    let original_data = b"Persistence integrity test data";
    
    // Создаем и записываем блок
    {
        let mut block = Block::new(BlockId(100));
        block.write_data(0, original_data).unwrap();
        
        let mut file = File::create(&file_path).unwrap();
        let serialized = block.serialize();
        file.write_all(&serialized).unwrap();
    }
    
    // Читаем и проверяем блок
    {
        let mut file = File::open(&file_path).unwrap();
        let mut buffer = vec![0u8; BLOCK_SIZE];
        file.read_exact(&mut buffer).unwrap();
        
        let loaded_block = Block::deserialize(&buffer, BlockId(100)).unwrap();
        assert_eq!(loaded_block.get_id(), BlockId(100));
        
        let mut data_buffer = vec![0u8; original_data.len()];
        loaded_block.read_data(0, &mut data_buffer).unwrap();
        assert_eq!(data_buffer, original_data);
    }
}
