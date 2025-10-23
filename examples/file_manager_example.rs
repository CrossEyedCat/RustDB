//! Пример использования менеджера файлов rustdb
//!
//! Этот пример демонстрирует:
//! - Создание файлов базы данных
//! - Запись и чтение блоков данных
//! - Управление размерами файлов
//! - Работу с заголовками файлов

use rustdb::common::Result;
use rustdb::storage::file_manager::{FileManager, BLOCK_SIZE};
use tempfile::TempDir;

fn main() -> Result<()> {
    println!("=== Пример использования менеджера файлов rustdb ===\n");

    // Создаем временную директорию для примера
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    println!(
        "📁 Создаем менеджер файлов в директории: {}",
        db_path.display()
    );
    let mut file_manager = FileManager::new(db_path)?;

    // Создаем новый файл базы данных
    println!("\n🗄️ Создаем новый файл базы данных 'example.db'");
    let file_id = file_manager.create_file("example.db")?;
    println!("✅ Файл создан с ID: {}", file_id);

    // Получаем информацию о файле
    if let Some(file_info) = file_manager.get_file_info(file_id) {
        println!("📊 Информация о файле:");
        println!("   - Путь: {}", file_info.path.display());
        println!("   - Размер в блоках: {}", file_info.size_in_blocks());
        println!("   - Используемых блоков: {}", file_info.used_blocks());
        println!("   - Свободных блоков: {}", file_info.free_blocks());
    }

    // Создаем тестовые данные для записи
    println!("\n📝 Записываем тестовые данные в блоки:");

    // Блок 0: Строковые данные
    let mut block0_data = vec![0u8; BLOCK_SIZE];
    let text = "Привет, мир! Это тест менеджера файлов rustdb.";
    let text_bytes = text.as_bytes();
    block0_data[..text_bytes.len()].copy_from_slice(text_bytes);
    file_manager.write_block(file_id, 0, &block0_data)?;
    println!("   ✅ Блок 0: записан текст '{}'", text);

    // Блок 1: Числовые данные
    let mut block1_data = vec![0u8; BLOCK_SIZE];
    for (i, byte) in block1_data.iter_mut().enumerate().take(256) {
        *byte = (i % 256) as u8;
    }
    file_manager.write_block(file_id, 1, &block1_data)?;
    println!("   ✅ Блок 1: записана последовательность байтов 0-255");

    // Блок 2: Случайные данные
    let block2_data: Vec<u8> = (0..BLOCK_SIZE)
        .map(|i| ((i * 7 + 13) % 256) as u8)
        .collect();
    file_manager.write_block(file_id, 2, &block2_data)?;
    println!("   ✅ Блок 2: записаны псевдослучайные данные");

    // Синхронизируем данные на диск
    println!("\n💾 Синхронизируем данные на диск");
    file_manager.sync_file(file_id)?;
    println!("   ✅ Данные синхронизированы");

    // Получаем обновленную информацию о файле
    if let Some(file_info) = file_manager.get_file_info(file_id) {
        println!("\n📊 Обновленная информация о файле:");
        println!("   - Размер в блоках: {}", file_info.size_in_blocks());
        println!("   - Используемых блоков: {}", file_info.used_blocks());
        println!("   - Свободных блоков: {}", file_info.free_blocks());
    }

    // Читаем данные обратно
    println!("\n📖 Читаем данные из блоков:");

    // Читаем блок 0
    let read_block0 = file_manager.read_block(file_id, 0)?;
    let text_end = read_block0
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(text_bytes.len());
    let read_text = String::from_utf8_lossy(&read_block0[..text_end]);
    println!("   📄 Блок 0: '{}'", read_text);

    // Читаем блок 1
    let read_block1 = file_manager.read_block(file_id, 1)?;
    let first_10_bytes: Vec<u8> = read_block1[..10].to_vec();
    println!("   🔢 Блок 1: первые 10 байтов: {:?}", first_10_bytes);

    // Читаем блок 2
    let read_block2 = file_manager.read_block(file_id, 2)?;
    let checksum: u32 = read_block2.iter().map(|&x| x as u32).sum();
    println!("   🎲 Блок 2: контрольная сумма: {}", checksum);

    // Проверяем целостность данных
    println!("\n🔍 Проверяем целостность данных:");
    let block1_valid = read_block1[..256]
        .iter()
        .enumerate()
        .all(|(i, &byte)| byte == (i % 256) as u8);
    println!("   ✅ Блок 1 корректен: {}", block1_valid);

    let block2_valid = read_block2
        .iter()
        .enumerate()
        .all(|(i, &byte)| byte == ((i * 7 + 13) % 256) as u8);
    println!("   ✅ Блок 2 корректен: {}", block2_valid);

    // Закрываем файл
    println!("\n🚪 Закрываем файл");
    file_manager.close_file(file_id)?;
    println!("   ✅ Файл закрыт");

    // Повторно открываем файл для проверки персистентности
    println!("\n🔄 Повторно открываем файл для проверки персистентности");
    let reopened_file_id = file_manager.open_file("example.db", false)?;
    println!("   ✅ Файл открыт с ID: {}", reopened_file_id);

    // Проверяем, что данные сохранились
    let persistent_block0 = file_manager.read_block(reopened_file_id, 0)?;
    let persistent_text_end = persistent_block0
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(text_bytes.len());
    let persistent_text = String::from_utf8_lossy(&persistent_block0[..persistent_text_end]);
    println!("   📄 Персистентные данные блока 0: '{}'", persistent_text);

    // Демонстрируем работу с несколькими файлами
    println!("\n📚 Создаем второй файл для демонстрации мультифайловой работы");
    let file2_id = file_manager.create_file("second.db")?;

    let file2_data = "Это данные во втором файле базы данных!".as_bytes();
    let mut block_data = vec![0u8; BLOCK_SIZE];
    block_data[..file2_data.len()].copy_from_slice(file2_data);
    file_manager.write_block(file2_id, 0, &block_data)?;

    let read_file2_data = file_manager.read_block(file2_id, 0)?;
    let file2_text_end = read_file2_data
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(file2_data.len());
    let file2_text = String::from_utf8_lossy(&read_file2_data[..file2_text_end]);
    println!("   📄 Данные второго файла: '{}'", file2_text);

    // Показываем список открытых файлов
    let open_files = file_manager.list_open_files();
    println!("\n📋 Список открытых файлов: {:?}", open_files);

    // Закрываем все файлы
    println!("\n🚪 Закрываем все файлы");
    file_manager.close_all()?;
    println!("   ✅ Все файлы закрыты");

    println!("\n🎉 Пример успешно завершен!");
    println!("\nЭтот пример продемонстрировал:");
    println!("• Создание и управление файлами базы данных");
    println!("• Запись и чтение блоков данных различных типов");
    println!("• Проверку целостности данных");
    println!("• Персистентность данных между сессиями");
    println!("• Работу с несколькими файлами одновременно");
    println!("• Синхронизацию данных на диск");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_manager_example() -> Result<()> {
        // Запускаем основную функцию как тест
        main()
    }

    #[test]
    fn test_multiple_files_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut file_manager = FileManager::new(temp_dir.path())?;

        // Создаем несколько файлов
        let file1_id = file_manager.create_file("test1.db")?;
        let file2_id = file_manager.create_file("test2.db")?;
        let file3_id = file_manager.create_file("test3.db")?;

        // Записываем уникальные данные в каждый файл
        for (i, &file_id) in [file1_id, file2_id, file3_id].iter().enumerate() {
            let data = vec![(i + 1) as u8; BLOCK_SIZE];
            file_manager.write_block(file_id, 0, &data)?;
        }

        // Проверяем, что данные в каждом файле уникальны
        for (i, &file_id) in [file1_id, file2_id, file3_id].iter().enumerate() {
            let read_data = file_manager.read_block(file_id, 0)?;
            assert_eq!(read_data[0], (i + 1) as u8);
            assert!(read_data.iter().all(|&x| x == (i + 1) as u8));
        }

        file_manager.close_all()?;
        Ok(())
    }

    #[test]
    fn test_large_file_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut file_manager = FileManager::new(temp_dir.path())?;

        let file_id = file_manager.create_file("large.db")?;

        // Записываем данные в блоки с большими индексами
        let block_indices = [0, 10, 100, 1000];

        for &block_id in &block_indices {
            let data = vec![(block_id % 256) as u8; BLOCK_SIZE];
            file_manager.write_block(file_id, block_id, &data)?;
        }

        // Проверяем, что файл автоматически расширился
        if let Some(file_info) = file_manager.get_file_info(file_id) {
            assert!(file_info.size_in_blocks() > 1000);
        }

        // Проверяем данные
        for &block_id in &block_indices {
            let read_data = file_manager.read_block(file_id, block_id)?;
            assert!(read_data.iter().all(|&x| x == (block_id % 256) as u8));
        }

        file_manager.close_file(file_id)?;
        Ok(())
    }
}
