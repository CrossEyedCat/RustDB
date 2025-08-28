//! Пример использования структуры файлов базы данных RustBD
//! 
//! Этот пример демонстрирует:
//! - Работу с расширенными заголовками файлов БД
//! - Управление картой свободных страниц
//! - Различные стратегии расширения файлов
//! - Мониторинг и статистику использования

use rustbd::storage::database_file::{
    DatabaseFileHeader, DatabaseFileType, DatabaseFileState, FreePageMap,
    FileExtensionManager, ExtensionStrategy, ExtensionReason
};
use rustbd::common::Result;

fn main() -> Result<()> {
    println!("=== Пример структуры файлов базы данных RustBD ===\n");

    // Демонстрация работы с заголовком файла БД
    demonstrate_database_header()?;
    
    // Демонстрация карты свободных страниц
    demonstrate_free_page_map()?;
    
    // Демонстрация менеджера расширения файлов
    demonstrate_extension_manager()?;

    println!("\n🎉 Пример успешно завершен!");
    Ok(())
}

fn demonstrate_database_header() -> Result<()> {
    println!("📋 === Демонстрация заголовка файла БД ===");
    
    // Создаем заголовок для файла данных
    let mut data_header = DatabaseFileHeader::new(DatabaseFileType::Data, 12345);
    data_header.file_sequence = 1;
    data_header.max_pages = 1000000; // Ограничение в 1 миллион страниц
    data_header.extension_size = 512; // Расширяем по 512 страниц (2MB)
    
    println!("🗄️ Создан заголовок файла данных:");
    println!("   - Тип файла: {}", data_header.type_description());
    println!("   - Состояние: {}", data_header.state_description());
    println!("   - ID базы данных: {}", data_header.database_id);
    println!("   - Версия формата: {}.{}", data_header.version, data_header.subversion);
    println!("   - Размер страницы: {} байт", data_header.page_size);
    println!("   - Максимум страниц: {}", data_header.max_pages);
    println!("   - Размер расширения: {} страниц", data_header.extension_size);
    
    // Демонстрируем работу с флагами
    data_header.set_flag(DatabaseFileHeader::FLAG_COMPRESSED);
    data_header.set_flag(DatabaseFileHeader::FLAG_DEBUG_MODE);
    
    println!("   - Флаги:");
    println!("     * Сжатие: {}", data_header.has_flag(DatabaseFileHeader::FLAG_COMPRESSED));
    println!("     * Шифрование: {}", data_header.has_flag(DatabaseFileHeader::FLAG_ENCRYPTED));
    println!("     * Контрольные суммы: {}", data_header.has_flag(DatabaseFileHeader::FLAG_CHECKSUM_ENABLED));
    println!("     * Режим отладки: {}", data_header.has_flag(DatabaseFileHeader::FLAG_DEBUG_MODE));
    
    // Обновляем статистику
    data_header.total_pages = 1000;
    data_header.used_pages = 750;
    data_header.free_pages = 250;
    data_header.increment_write_count();
    data_header.increment_read_count();
    
    // Проверяем контрольную сумму
    data_header.update_checksum();
    println!("   - Контрольная сумма: 0x{:08X}", data_header.checksum);
    println!("   - Заголовок корректен: {}", data_header.is_valid());
    
    // Создаем заголовок для файла индексов
    let mut index_header = DatabaseFileHeader::new(DatabaseFileType::Index, 12345);
    index_header.file_sequence = 2;
    index_header.catalog_root_page = Some(1); // Корневая страница каталога
    index_header.update_checksum();
    
    println!("\n📊 Создан заголовок файла индексов:");
    println!("   - Тип файла: {}", index_header.type_description());
    println!("   - Корневая страница каталога: {:?}", index_header.catalog_root_page);
    println!("   - Последовательность файла: {}", index_header.file_sequence);
    
    Ok(())
}

fn demonstrate_free_page_map() -> Result<()> {
    println!("\n🗺️ === Демонстрация карты свободных страниц ===");
    
    let mut free_map = FreePageMap::new();
    
    println!("📍 Добавляем свободные блоки:");
    
    // Добавляем различные блоки свободных страниц
    free_map.add_free_block(100, 50)?; // 50 страниц начиная с 100
    println!("   ✅ Добавлен блок: страницы 100-149 (50 страниц)");
    
    free_map.add_free_block(200, 25)?; // 25 страниц начиная с 200
    println!("   ✅ Добавлен блок: страницы 200-224 (25 страниц)");
    
    free_map.add_free_block(300, 75)?; // 75 страниц начиная с 300
    println!("   ✅ Добавлен блок: страницы 300-374 (75 страниц)");
    
    // Пытаемся добавить соседний блок (должен объединиться)
    free_map.add_free_block(150, 20)?; // Соседний с первым блоком
    println!("   ✅ Добавлен соседний блок: страницы 150-169 (20 страниц) - объединен с предыдущим");
    
    println!("\n📊 Статистика карты свободных страниц:");
    println!("   - Общее количество записей: {}", free_map.header.total_entries);
    println!("   - Активных записей: {}", free_map.header.active_entries);
    println!("   - Всего свободных страниц: {}", free_map.total_free_pages());
    println!("   - Наибольший свободный блок: {} страниц", free_map.find_largest_free_block());
    
    println!("\n💾 Выделяем страницы:");
    
    // Выделяем страницы различного размера
    if let Some(allocated) = free_map.allocate_pages(30) {
        println!("   ✅ Выделено 30 страниц, начиная с страницы {}", allocated);
    }
    
    if let Some(allocated) = free_map.allocate_pages(10) {
        println!("   ✅ Выделено 10 страниц, начиная с страницы {}", allocated);
    }
    
    if let Some(allocated) = free_map.allocate_pages(100) {
        println!("   ✅ Выделено 100 страниц, начиная с страницы {}", allocated);
    } else {
        println!("   ❌ Не удалось выделить 100 страниц (недостаточно места)");
    }
    
    println!("\n📊 Обновленная статистика:");
    println!("   - Всего свободных страниц: {}", free_map.total_free_pages());
    println!("   - Наибольший свободный блок: {} страниц", free_map.find_largest_free_block());
    
    // Освобождаем некоторые страницы
    println!("\n🔄 Освобождаем страницы:");
    free_map.free_pages(50, 15)?; // Освобождаем 15 страниц начиная с 50
    println!("   ✅ Освобождено 15 страниц, начиная с страницы 50");
    
    // Дефрагментируем карту
    println!("\n🔧 Дефрагментируем карту...");
    free_map.defragment();
    println!("   ✅ Дефрагментация завершена");
    println!("   - Записей после дефрагментации: {}", free_map.entries.len());
    
    // Проверяем целостность
    match free_map.validate() {
        Ok(_) => println!("   ✅ Карта свободных страниц корректна"),
        Err(e) => println!("   ❌ Ошибка валидации карты: {}", e),
    }
    
    Ok(())
}

fn demonstrate_extension_manager() -> Result<()> {
    println!("\n📈 === Демонстрация менеджера расширения файлов ===");
    
    // Демонстрируем различные стратегии расширения
    let strategies = vec![
        ("Фиксированная", ExtensionStrategy::Fixed),
        ("Линейная", ExtensionStrategy::Linear),
        ("Экспоненциальная", ExtensionStrategy::Exponential),
        ("Адаптивная", ExtensionStrategy::Adaptive),
    ];
    
    for (name, strategy) in strategies {
        println!("\n🔧 Стратегия расширения: {}", name);
        
        let mut manager = FileExtensionManager::new(strategy);
        manager.min_extension_size = 32;  // 128KB
        manager.max_extension_size = 1024; // 4MB
        manager.growth_factor = 1.5;
        
        let current_size = 1000u64; // Текущий размер файла
        
        // Вычисляем размеры расширения для различных требований
        let sizes = vec![10, 50, 100, 500];
        
        for required_size in sizes {
            let extension_size = manager.calculate_extension_size(current_size, required_size);
            println!("   - Требуется {} страниц → расширение на {} страниц", 
                     required_size, extension_size);
        }
        
        // Симулируем несколько расширений
        let mut file_size = current_size;
        for i in 1..=3 {
            let old_size = file_size;
            let extension = manager.calculate_extension_size(file_size, 50);
            file_size += extension as u64;
            
            manager.record_extension(old_size, file_size, ExtensionReason::OutOfSpace);
            
            println!("   - Расширение #{}: {} → {} страниц (+{})", 
                     i, old_size, file_size, extension);
        }
        
        // Получаем статистику
        let stats = manager.get_statistics();
        println!("   - Всего расширений: {}", stats.total_extensions);
        println!("   - Средний размер расширения: {:.1} страниц", stats.average_extension_size);
        
        // Проверяем рекомендации по предварительному расширению
        let should_preextend = manager.should_preextend(file_size, 100, file_size);
        println!("   - Рекомендуется предварительное расширение: {}", should_preextend);
    }
    
    // Демонстрируем адаптивную стратегию с историей
    println!("\n🧠 Демонстрация адаптивной стратегии:");
    let mut adaptive_manager = FileExtensionManager::new(ExtensionStrategy::Adaptive);
    
    // Симулируем активное использование (много расширений)
    let mut file_size = 500u64;
    for i in 1..=8 {
        let old_size = file_size;
        let extension = adaptive_manager.calculate_extension_size(file_size, 20);
        file_size += extension as u64;
        
        let reason = if i % 3 == 0 {
            ExtensionReason::Preallocation
        } else {
            ExtensionReason::OutOfSpace
        };
        
        adaptive_manager.record_extension(old_size, file_size, reason);
        
        println!("   - Адаптивное расширение #{}: +{} страниц (причина: {:?})", 
                 i, extension, reason);
    }
    
    let final_stats = adaptive_manager.get_statistics();
    println!("   - Итоговая статистика:");
    println!("     * Всего расширений: {}", final_stats.total_extensions);
    println!("     * Средний размер: {:.1} страниц", final_stats.average_extension_size);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_structure_example() -> Result<()> {
        // Запускаем основную функцию как тест
        main()
    }

    #[test]
    fn test_header_operations() -> Result<()> {
        let mut header = DatabaseFileHeader::new(DatabaseFileType::Data, 999);
        
        // Тестируем базовые операции
        assert_eq!(header.database_id, 999);
        assert_eq!(header.file_type, DatabaseFileType::Data);
        assert_eq!(header.file_state, DatabaseFileState::Creating);
        
        // Тестируем флаги
        header.set_flag(DatabaseFileHeader::FLAG_COMPRESSED);
        assert!(header.has_flag(DatabaseFileHeader::FLAG_COMPRESSED));
        
        header.clear_flag(DatabaseFileHeader::FLAG_COMPRESSED);
        assert!(!header.has_flag(DatabaseFileHeader::FLAG_COMPRESSED));
        
        // Тестируем валидацию
        header.update_checksum();
        assert!(header.is_valid());
        
        Ok(())
    }

    #[test]
    fn test_free_page_map_operations() -> Result<()> {
        let mut map = FreePageMap::new();
        
        // Добавляем блоки
        map.add_free_block(10, 5)?;
        map.add_free_block(20, 10)?;
        
        assert_eq!(map.total_free_pages(), 15);
        assert_eq!(map.find_largest_free_block(), 10);
        
        // Выделяем страницы
        let allocated = map.allocate_pages(3);
        assert_eq!(allocated, Some(10)); // Должен выделить из первого подходящего блока (first-fit)
        
        assert_eq!(map.total_free_pages(), 12);
        
        // Освобождаем страницы
        map.free_pages(50, 5)?;
        assert_eq!(map.total_free_pages(), 17);
        
        Ok(())
    }

    #[test]
    fn test_extension_strategies() {
        let fixed = FileExtensionManager::new(ExtensionStrategy::Fixed);
        let linear = FileExtensionManager::new(ExtensionStrategy::Linear);
        let exponential = FileExtensionManager::new(ExtensionStrategy::Exponential);
        
        let current_size = 1000;
        let required = 10;
        
        let fixed_ext = fixed.calculate_extension_size(current_size, required);
        let linear_ext = linear.calculate_extension_size(current_size, required);
        let exp_ext = exponential.calculate_extension_size(current_size, required);
        
        // Фиксированная стратегия должна давать минимальный размер
        assert!(fixed_ext >= required as u32);
        
        // Другие стратегии должны учитывать размер файла
        assert!(linear_ext >= fixed_ext);
        assert!(exp_ext >= fixed_ext);
    }
}
