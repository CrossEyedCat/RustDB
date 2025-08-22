# Руководство по тестированию RustBD

## 🧪 Обзор

Это руководство описывает стратегии и практики тестирования для проекта RustBD. Мы используем многоуровневый подход к тестированию для обеспечения качества и надежности системы.

## 🏗️ Архитектура тестирования

### Уровни тестирования

```
┌─────────────────────────────────────────────────────────────┐
│                    End-to-End Tests                        │
│              (Интеграционные тесты системы)                │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                Integration Tests                           │
│           (Тесты взаимодействия компонентов)               │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                   Unit Tests                              │
│              (Тесты отдельных функций)                     │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                Property Tests                              │
│           (Тесты свойств и инвариантов)                   │
└─────────────────────────────────────────────────────────────┘
```

## 🔧 Unit тестирование

### Структура unit тестов

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_function_name() {
        // Arrange - подготовка данных
        let input = "test_data";
        
        // Act - выполнение тестируемого кода
        let result = function_under_test(input);
        
        // Assert - проверка результата
        assert_eq!(result, expected_value);
    }

    #[test]
    fn test_function_with_error() {
        // Arrange
        let invalid_input = "invalid";
        
        // Act & Assert
        let result = function_under_test(invalid_input);
        assert!(result.is_err());
        
        match result.unwrap_err() {
            ExpectedError::InvalidInput { message } => {
                assert!(message.contains("invalid"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }
}
```

### Тестирование структур данных

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        // Arrange
        let page_id = PageId::new(1);
        let page_size = 4096;
        
        // Act
        let page = Page::new(page_id, page_size);
        
        // Assert
        assert_eq!(page.id(), page_id);
        assert_eq!(page.size(), page_size);
        assert_eq!(page.free_space(), page_size - Page::header_size());
        assert!(page.is_empty());
    }

    #[test]
    fn test_page_insert_record() {
        // Arrange
        let mut page = Page::new(PageId::new(1), 4096);
        let record = Record::new("test_data".as_bytes().to_vec());
        
        // Act
        let result = page.insert_record(&record);
        
        // Assert
        assert!(result.is_ok());
        assert!(!page.is_empty());
        assert_eq!(page.record_count(), 1);
        assert!(page.free_space() < 4096 - Page::header_size());
    }

    #[test]
    fn test_page_full_insertion() {
        // Arrange
        let mut page = Page::new(PageId::new(1), 100);
        let large_record = Record::new(vec![0u8; 200]); // Слишком большой
        
        // Act
        let result = page.insert_record(&large_record);
        
        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            PageError::RecordTooLarge { record_size, available_space } => {
                assert_eq!(record_size, 200);
                assert!(available_space < 200);
            }
            _ => panic!("Expected RecordTooLarge error"),
        }
    }
}
```

### Тестирование асинхронного кода

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[tokio::test]
    async fn test_async_connection() {
        // Arrange
        let config = ConnectionConfig::new()
            .host("localhost")
            .port(5432)
            .build()
            .unwrap();
        
        // Act
        let connection = DatabaseConnection::connect(&config).await;
        
        // Assert
        assert!(connection.is_ok());
        let conn = connection.unwrap();
        assert_eq!(conn.host(), "localhost");
        assert_eq!(conn.port(), 5432);
    }

    #[tokio::test]
    async fn test_connection_timeout() {
        // Arrange
        let config = ConnectionConfig::new()
            .host("invalid-host")
            .port(5432)
            .connection_timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        
        // Act
        let start = Instant::now();
        let connection = DatabaseConnection::connect(&config).await;
        let duration = start.elapsed();
        
        // Assert
        assert!(connection.is_err());
        assert!(duration >= Duration::from_millis(100));
        assert!(duration < Duration::from_millis(200));
    }
}
```

### Тестирование с моками

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;
    use mockall::*;

    mock! {
        Storage {}
        
        impl Storage for Storage {
            async fn read_page(&self, page_id: PageId) -> Result<Page, StorageError>;
            async fn write_page(&self, page: &Page) -> Result<(), StorageError>;
        }
    }

    #[tokio::test]
    async fn test_buffer_manager_with_mock_storage() {
        // Arrange
        let mut mock_storage = MockStorage::new();
        let page_id = PageId::new(1);
        let page = Page::new(page_id, 4096);
        
        mock_storage
            .expect_read_page()
            .with(eq(page_id))
            .times(1)
            .returning(move |_| Ok(page.clone()));
        
        let buffer_manager = BufferManager::new(Arc::new(mock_storage));
        
        // Act
        let result = buffer_manager.get_page(page_id).await;
        
        // Assert
        assert!(result.is_ok());
        let retrieved_page = result.unwrap();
        assert_eq!(retrieved_page.id(), page_id);
    }
}
```

## 🔗 Интеграционное тестирование

### Тестирование взаимодействия компонентов

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_utils::TestDatabase;

    #[tokio::test]
    async fn test_full_query_execution() {
        // Arrange
        let db = TestDatabase::new().await;
        
        // Создание таблицы
        let schema = TableSchema::new("users")
            .add_column("id", ColumnType::Integer, vec![ColumnConstraint::PrimaryKey])
            .add_column("name", ColumnType::Varchar(50), vec![ColumnConstraint::NotNull])
            .build()
            .unwrap();
        
        db.create_table(&schema).await.unwrap();
        
        // Вставка данных
        let insert_result = db.execute(
            "INSERT INTO users (name) VALUES (?)",
            &["John Doe"]
        ).await;
        assert!(insert_result.is_ok());
        
        // Выборка данных
        let select_result = db.query("SELECT * FROM users", &[]).await;
        assert!(select_result.is_ok());
        
        let rows = select_result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").unwrap(), "John Doe");
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        // Arrange
        let db = TestDatabase::new().await;
        
        // Создание таблицы
        let schema = TableSchema::new("accounts")
            .add_column("id", ColumnType::Integer, vec![ColumnConstraint::PrimaryKey])
            .add_column("balance", ColumnType::Integer, vec![ColumnConstraint::NotNull])
            .build()
            .unwrap();
        
        db.create_table(&schema).await.unwrap();
        
        // Начало транзакции
        let transaction = db.begin_transaction().await.unwrap();
        
        // Вставка данных
        transaction.execute("INSERT INTO accounts (balance) VALUES (?)", &[&100]).await.unwrap();
        
        // Проверка данных в транзакции
        let rows = transaction.query("SELECT * FROM accounts", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        
        // Откат транзакции
        transaction.rollback().await.unwrap();
        
        // Проверка, что данные не сохранились
        let rows = db.query("SELECT * FROM accounts", &[]).await.unwrap();
        assert_eq!(rows.len(), 0);
    }
}
```

### Тестирование сетевого слоя

```rust
#[cfg(test)]
mod network_tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_client_server_communication() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        
        // Запуск сервера в отдельной задаче
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut server = DatabaseServer::new(socket);
            server.handle_connection().await.unwrap();
        });
        
        // Подключение клиента
        let client = DatabaseClient::connect(&server_addr).await.unwrap();
        
        // Выполнение запроса
        let result = client.execute("SELECT 1").await;
        assert!(result.is_ok());
        
        // Ожидание завершения сервера
        server_handle.await.unwrap().unwrap();
    }
}
```

## 📊 Property-based тестирование

### Использование proptest

```rust
#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_page_size_invariants(
            page_id in 1..1000u32,
            page_size in 1024..65536u32
        ) {
            // Arrange
            let page_id = PageId::new(page_id);
            let page_size = page_size;
            
            // Act
            let page = Page::new(page_id, page_size);
            
            // Assert - проверка инвариантов
            prop_assert!(page.size() >= 1024);
            prop_assert!(page.size() <= 65536);
            prop_assert!(page.free_space() <= page.size());
            prop_assert!(page.free_space() >= page.size() - Page::header_size());
        }

        #[test]
        fn test_record_insertion_properties(
            record_data in prop::collection::vec(any::<u8>(), 1..1000)
        ) {
            // Arrange
            let mut page = Page::new(PageId::new(1), 4096);
            let record = Record::new(record_data);
            
            // Act
            let result = page.insert_record(&record);
            
            // Assert - проверка свойств
            if record.size() <= page.free_space() {
                prop_assert!(result.is_ok());
                prop_assert!(!page.is_empty());
                prop_assert!(page.record_count() > 0);
            } else {
                prop_assert!(result.is_err());
            }
        }
    }
}
```

### Тестирование сериализации

```rust
#[cfg(test)]
mod serialization_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_page_serialization_roundtrip(
            page_id in 1..1000u32,
            page_size in 1024..65536u32
        ) {
            // Arrange
            let original_page = Page::new(PageId::new(page_id), page_size);
            
            // Act
            let serialized = bincode::serialize(&original_page).unwrap();
            let deserialized: Page = bincode::deserialize(&serialized).unwrap();
            
            // Assert - проверка сохранения свойств
            prop_assert_eq!(original_page.id(), deserialized.id());
            prop_assert_eq!(original_page.size(), deserialized.size());
            prop_assert_eq!(original_page.free_space(), deserialized.free_space());
        }
    }
}
```

## 🚀 Performance тестирование

### Бенчмарки

```rust
#[cfg(test)]
mod benchmarks {
    use super::*;
    use criterion::{black_box, criterion_group, criterion_main, Criterion};

    fn benchmark_page_insertion(c: &mut Criterion) {
        let mut group = c.benchmark_group("page_operations");
        
        group.bench_function("insert_small_record", |b| {
            b.iter(|| {
                let mut page = Page::new(PageId::new(1), 4096);
                let record = Record::new(black_box(vec![0u8; 100]));
                page.insert_record(&record).unwrap();
                page
            });
        });
        
        group.bench_function("insert_large_record", |b| {
            b.iter(|| {
                let mut page = Page::new(PageId::new(1), 4096);
                let record = Record::new(black_box(vec![0u8; 1000]));
                page.insert_record(&record).unwrap();
                page
            });
        });
        
        group.finish();
    }

    fn benchmark_buffer_operations(c: &mut Criterion) {
        let mut group = c.benchmark_group("buffer_operations");
        
        group.bench_function("lru_cache_hit", |b| {
            let buffer_manager = BufferManager::new(Arc::new(MockStorage::new()));
            let page_id = PageId::new(1);
            
            b.iter(|| {
                buffer_manager.get_page(page_id).unwrap();
            });
        });
        
        group.finish();
    }

    criterion_group!(benches, benchmark_page_insertion, benchmark_buffer_operations);
    criterion_main!(benches);
}
```

### Нагрузочное тестирование

```rust
#[cfg(test)]
mod stress_tests {
    use super::*;
    use tokio::task;

    #[tokio::test]
    async fn test_concurrent_connections() {
        // Arrange
        let db = TestDatabase::new().await;
        let connection_count = 100;
        
        // Act - создание множественных соединений
        let handles: Vec<_> = (0..connection_count)
            .map(|i| {
                let db_clone = db.clone();
                task::spawn(async move {
                    let result = db_clone.execute("SELECT ?", &[&i]).await;
                    (i, result)
                })
            })
            .collect();
        
        // Assert - проверка всех соединений
        for handle in handles {
            let (id, result) = handle.await.unwrap();
            assert!(result.is_ok(), "Connection {} failed: {:?}", id, result);
        }
    }

    #[tokio::test]
    async fn test_high_concurrency_queries() {
        // Arrange
        let db = TestDatabase::new().await;
        let query_count = 1000;
        let concurrent_tasks = 10;
        
        // Act - выполнение множественных запросов
        let handles: Vec<_> = (0..concurrent_tasks)
            .map(|_| {
                let db_clone = db.clone();
                task::spawn(async move {
                    for i in 0..(query_count / concurrent_tasks) {
                        let result = db_clone.execute("SELECT ?", &[&i]).await;
                        assert!(result.is_ok());
                    }
                })
            })
            .collect();
        
        // Assert - ожидание завершения всех задач
        for handle in handles {
            handle.await.unwrap();
        }
    }
}
```

## 🧹 Тестовые утилиты

### TestDatabase

```rust
pub mod test_utils {
    use super::*;
    use tempfile::TempDir;
    use std::sync::Arc;

    /// Тестовая база данных для интеграционных тестов
    pub struct TestDatabase {
        data_dir: TempDir,
        database: Arc<Database>,
    }

    impl TestDatabase {
        /// Создает новую тестовую базу данных
        pub async fn new() -> Self {
            let data_dir = TempDir::new().unwrap();
            let config = DatabaseConfig::new()
                .data_directory(data_dir.path())
                .build()
                .unwrap();
            
            let database = Database::new(config).await.unwrap();
            
            Self {
                data_dir,
                database: Arc::new(database),
            }
        }

        /// Создает тестовую базу данных с предустановленной схемой
        pub async fn with_schema(schema_sql: &str) -> Self {
            let db = Self::new().await;
            
            // Выполнение SQL для создания схемы
            db.execute(schema_sql, &[]).await.unwrap();
            
            db
        }

        /// Очищает все данные в тестовой базе
        pub async fn clear(&self) -> Result<(), DatabaseError> {
            // Получение списка всех таблиц
            let tables = self.query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
                &[]
            ).await?;
            
            // Удаление всех таблиц
            for table in tables {
                let table_name: String = table.get("table_name")?.try_into()?;
                self.execute(&format!("DROP TABLE IF EXISTS {}", table_name), &[]).await?;
            }
            
            Ok(())
        }

        /// Уничтожает тестовую базу данных
        pub async fn destroy(self) -> Result<(), DatabaseError> {
            // Закрытие соединений
            self.database.shutdown().await?;
            
            // Удаление временной директории
            self.data_dir.close().unwrap();
            
            Ok(())
        }
    }

    impl std::ops::Deref for TestDatabase {
        type Target = Database;

        fn deref(&self) -> &Self::Target {
            &self.database
        }
    }

    impl std::ops::DerefMut for TestDatabase {
        fn deref_mut(&mut self) -> &mut Self::Target {
            Arc::get_mut(&mut self.database).unwrap()
        }
    }
}
```

### Тестовые данные

```rust
pub mod test_data {
    use super::*;

    /// Генерирует тестовые пользователи
    pub fn generate_test_users(count: usize) -> Vec<User> {
        (0..count)
            .map(|i| User {
                id: i as u32,
                username: format!("user_{}", i),
                email: format!("user_{}@example.com", i),
                created_at: Utc::now(),
            })
            .collect()
    }

    /// Генерирует тестовые записи для таблицы
    pub fn generate_test_records(count: usize) -> Vec<Record> {
        (0..count)
            .map(|i| Record::new(format!("record_{}", i).as_bytes().to_vec()))
            .collect()
    }

    /// Создает тестовую схему таблицы
    pub fn create_test_table_schema() -> TableSchema {
        TableSchema::new("test_table")
            .add_column("id", ColumnType::Integer, vec![ColumnConstraint::PrimaryKey])
            .add_column("name", ColumnType::Varchar(100), vec![ColumnConstraint::NotNull])
            .add_column("value", ColumnType::Integer, vec![])
            .build()
            .unwrap()
    }
}
```

## 🔍 Отладка тестов

### Логирование в тестах

```rust
#[cfg(test)]
mod debug_tests {
    use super::*;
    use tracing::{info, warn, error};

    #[tokio::test]
    async fn test_with_logging() {
        // Настройка логирования для тестов
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .init();

        info!("Начинаем тест подключения к базе данных");
        
        let db = TestDatabase::new().await;
        info!("Тестовая база данных создана");
        
        // Выполнение теста
        let result = db.execute("SELECT 1", &[]).await;
        
        match result {
            Ok(_) => info!("Тест выполнен успешно"),
            Err(e) => {
                error!("Тест завершился с ошибкой: {:?}", e);
                panic!("Тест не должен завершаться с ошибкой");
            }
        }
    }
}
```

### Визуализация тестов

```rust
#[cfg(test)]
mod visualization_tests {
    use super::*;

    #[test]
    fn test_page_structure_visualization() {
        let page = Page::new(PageId::new(1), 4096);
        
        // Визуализация структуры страницы
        println!("Page Structure:");
        println!("  ID: {}", page.id());
        println!("  Size: {} bytes", page.size());
        println!("  Free Space: {} bytes", page.free_space());
        println!("  Record Count: {}", page.record_count());
        
        // Визуализация макета страницы
        println!("Page Layout:");
        println!("  [Header: {} bytes]", Page::header_size());
        println!("  [Records: {} bytes]", page.size() - page.free_space() - Page::header_size());
        println!("  [Free Space: {} bytes]", page.free_space());
        
        assert!(page.is_empty());
    }
}
```

## 📊 Метрики тестирования

### Покрытие кода

```bash
# Установка cargo-tarpaulin
cargo install cargo-tarpaulin

# Проверка покрытия
cargo tarpaulin --out Html

# Проверка покрытия с исключениями
cargo tarpaulin --exclude-files "tests/*" --out Html
```

### Анализ производительности

```bash
# Установка cargo-flamegraph
cargo install flamegraph

# Создание flamegraph для тестов
cargo flamegraph --test test_name

# Анализ памяти
cargo install cargo-valgrind
cargo valgrind test test_name
```

## 🚨 Обработка ошибок в тестах

### Тестирование ошибок

```rust
#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn test_database_errors() {
        // Тестирование различных типов ошибок
        let connection_error = DatabaseError::ConnectionError {
            message: "Connection failed".to_string(),
        };
        
        let sql_error = DatabaseError::SqlError {
            sql: "SELECT * FROM invalid_table".to_string(),
            details: "Table does not exist".to_string(),
        };
        
        // Проверка сообщений об ошибках
        assert!(connection_error.to_string().contains("Connection failed"));
        assert!(sql_error.to_string().contains("SELECT * FROM invalid_table"));
        assert!(sql_error.to_string().contains("Table does not exist"));
    }

    #[test]
    fn test_error_conversion() {
        // Тестирование преобразования ошибок
        let io_error = std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "Connection refused"
        );
        
        let db_error: DatabaseError = io_error.into();
        
        match db_error {
            DatabaseError::IoError(_) => (), // Ожидаемый тип
            _ => panic!("Expected IoError"),
        }
    }
}
```

## 📚 Лучшие практики

### Общие принципы

1. **Изоляция**: Каждый тест должен быть независимым
2. **Детерминированность**: Тесты должны давать одинаковый результат при каждом запуске
3. **Быстрота**: Тесты должны выполняться быстро
4. **Читаемость**: Тесты должны быть понятными и самодокументируемыми
5. **Поддержка**: Тесты должны быть легко поддерживаемыми

### Организация тестов

1. **Группировка**: Группируйте связанные тесты в модули
2. **Именование**: Используйте описательные имена для тестов
3. **Структура**: Следуйте паттерну Arrange-Act-Assert
4. **Документация**: Комментируйте сложные тесты

### Управление тестовыми данными

1. **Фикстуры**: Используйте фикстуры для повторяющихся данных
2. **Фабрики**: Создавайте фабрики для генерации тестовых объектов
3. **Очистка**: Всегда очищайте тестовые данные после тестов
4. **Изоляция**: Используйте отдельные базы данных для каждого теста

## 🔗 Дополнительные ресурсы

- [Rust Book - Testing](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [Rust Testing Guide](https://rust-lang.github.io/rustc-guide/tests.html)
- [Criterion.rs](https://github.com/bheisler/criterion.rs)
- [proptest](https://github.com/AltSysrq/proptest)
- [mockall](https://github.com/asomers/mockall)

Следуя этим рекомендациям, вы создадите надежную и поддерживаемую систему тестирования для RustBD.
