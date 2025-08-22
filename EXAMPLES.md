# Примеры использования RustBD

## 🚀 Быстрый старт

### Подключение к базе данных

```rust
use rustbd::Database;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Создание подключения к базе данных
    let db = Database::connect("localhost:5432")
        .timeout(Duration::from_secs(30))
        .await?;
    
    println!("Подключение к базе данных установлено!");
    
    // Выполнение простого запроса
    let result = db.execute("SELECT version()").await?;
    println!("Версия: {:?}", result);
    
    Ok(())
}
```

### Создание таблицы

```rust
use rustbd::{Database, TableSchema, ColumnType, ColumnConstraint};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Определение схемы таблицы
    let schema = TableSchema::new("users")
        .add_column("id", ColumnType::Integer, vec![ColumnConstraint::PrimaryKey])
        .add_column("username", ColumnType::Varchar(50), vec![ColumnConstraint::NotNull, ColumnConstraint::Unique])
        .add_column("email", ColumnType::Varchar(100), vec![ColumnConstraint::NotNull])
        .add_column("created_at", ColumnType::Timestamp, vec![ColumnConstraint::NotNull])
        .build()?;
    
    // Создание таблицы
    db.create_table(&schema).await?;
    println!("Таблица 'users' создана успешно!");
    
    Ok(())
}
```

## 📊 Работа с данными

### Вставка данных

```rust
use rustbd::{Database, Row, Value};
use chrono::{DateTime, Utc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Создание строки данных
    let row = Row::new()
        .set("username", Value::String("john_doe".to_string()))
        .set("email", Value::String("john@example.com".to_string()))
        .set("created_at", Value::Timestamp(Utc::now()))
        .build()?;
    
    // Вставка данных
    let result = db.insert("users", row).await?;
    println!("Вставлено {} строк", result.affected_rows);
    
    Ok(())
}
```

### Выборка данных

```rust
use rustbd::{Database, QueryBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Простой SELECT запрос
    let users = db.query("SELECT * FROM users WHERE username = ?", &["john_doe"]).await?;
    
    for row in users {
        println!("Пользователь: {:?}", row);
    }
    
    // Использование QueryBuilder
    let query = QueryBuilder::select()
        .from("users")
        .where_("username = ?")
        .and_("email LIKE ?")
        .order_by("created_at DESC")
        .limit(10)
        .build()?;
    
    let results = db.execute_query(&query, &["john_doe", "%@example.com"]).await?;
    
    Ok(())
}
```

### Обновление данных

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Обновление данных
    let result = db.execute(
        "UPDATE users SET email = ? WHERE username = ?",
        &["new_email@example.com", "john_doe"]
    ).await?;
    
    println!("Обновлено {} строк", result.affected_rows);
    
    Ok(())
}
```

### Удаление данных

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Удаление данных
    let result = db.execute(
        "DELETE FROM users WHERE username = ?",
        &["john_doe"]
    ).await?;
    
    println!("Удалено {} строк", result.affected_rows);
    
    Ok(())
}
```

## 🔄 Транзакции

### Простая транзакция

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Начало транзакции
    let transaction = db.begin_transaction().await?;
    
    // Выполнение операций в транзакции
    transaction.execute("INSERT INTO users (username, email) VALUES (?, ?)", &["user1", "user1@example.com"]).await?;
    transaction.execute("INSERT INTO users (username, email) VALUES (?, ?)", &["user2", "user2@example.com"]).await?;
    
    // Подтверждение транзакции
    transaction.commit().await?;
    
    println!("Транзакция выполнена успешно!");
    
    Ok(())
}
```

### Транзакция с откатом

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Начало транзакции
    let transaction = db.begin_transaction().await?;
    
    // Выполнение операций
    transaction.execute("INSERT INTO users (username, email) VALUES (?, ?)", &["user3", "user3@example.com"]).await?;
    
    // Проверка условия
    let result = transaction.query("SELECT COUNT(*) FROM users WHERE username = ?", &["user3"]).await?;
    let count: i64 = result[0].get("COUNT(*)")?.try_into()?;
    
    if count > 1 {
        // Откат транзакции при определенном условии
        transaction.rollback().await?;
        println!("Транзакция отменена!");
    } else {
        // Подтверждение транзакции
        transaction.commit().await?;
        println!("Транзакция выполнена успешно!");
    }
    
    Ok(())
}
```

## 🗂️ Индексы

### Создание индекса

```rust
use rustbd::{Database, IndexType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Создание B+ дерева индекса
    db.create_index("users", "idx_username", &["username"], IndexType::BTree).await?;
    
    // Создание хеш-индекса
    db.create_index("users", "idx_email", &["email"], IndexType::Hash).await?;
    
    println!("Индексы созданы успешно!");
    
    Ok(())
}
```

### Анализ использования индексов

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Получение статистики по индексам
    let stats = db.get_index_statistics("users").await?;
    
    for (index_name, stat) in stats {
        println!("Индекс {}: использований = {}, селективность = {:.2}%", 
                index_name, stat.usage_count, stat.selectivity * 100.0);
    }
    
    Ok(())
}
```

## 📈 Оптимизация запросов

### Анализ плана выполнения

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Получение плана выполнения запроса
    let plan = db.explain("SELECT * FROM users WHERE username = ? AND email LIKE ?").await?;
    
    println!("План выполнения:");
    println!("{:?}", plan);
    
    // Получение статистики по таблицам
    let table_stats = db.get_table_statistics("users").await?;
    println!("Статистика таблицы users:");
    println!("Количество строк: {}", table_stats.row_count);
    println!("Размер таблицы: {} байт", table_stats.size_bytes);
    
    Ok(())
}
```

### Параллельное выполнение

```rust
use rustbd::Database;
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Параллельное выполнение нескольких запросов
    let handles: Vec<_> = (1..=5).map(|i| {
        let db_clone = db.clone();
        task::spawn(async move {
            let result = db_clone.query("SELECT * FROM users WHERE id = ?", &[i]).await;
            (i, result)
        })
    }).collect();
    
    // Ожидание завершения всех задач
    for handle in handles {
        let (id, result) = handle.await?;
        match result {
            Ok(rows) => println!("Запрос {}: найдено {} строк", id, rows.len()),
            Err(e) => println!("Ошибка в запросе {}: {:?}", id, e),
        }
    }
    
    Ok(())
}
```

## 🔐 Управление пользователями и правами

### Создание пользователя

```rust
use rustbd::{Database, User, UserRole};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Создание нового пользователя
    let user = User::new("new_user")
        .with_password("secure_password")
        .with_role(UserRole::Regular)
        .build()?;
    
    db.create_user(&user).await?;
    println!("Пользователь '{}' создан успешно!", user.username);
    
    Ok(())
}
```

### Назначение прав доступа

```rust
use rustbd::{Database, Permission, PermissionLevel};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Предоставление прав на чтение таблицы
    db.grant_permission(
        "new_user",
        "users",
        Permission::Select,
        PermissionLevel::Table
    ).await?;
    
    // Предоставление прав на вставку
    db.grant_permission(
        "new_user",
        "users",
        Permission::Insert,
        PermissionLevel::Table
    ).await?;
    
    println!("Права доступа назначены успешно!");
    
    Ok(())
}
```

## 📊 Мониторинг и метрики

### Получение метрик производительности

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Получение метрик производительности
    let metrics = db.get_performance_metrics().await?;
    
    println!("Метрики производительности:");
    println!("Активные соединения: {}", metrics.active_connections);
    println!("Запросов в секунду: {:.2}", metrics.queries_per_second);
    println!("Среднее время выполнения: {:?}", metrics.avg_query_time);
    println!("Hit ratio буфера: {:.2}%", metrics.buffer_hit_ratio * 100.0);
    
    Ok(())
}
```

### Мониторинг блокировок

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Получение информации о блокировках
    let locks = db.get_active_locks().await?;
    
    if locks.is_empty() {
        println!("Активных блокировок нет");
    } else {
        println!("Активные блокировки:");
        for lock in locks {
            println!("  - Таблица: {}, Тип: {:?}, Пользователь: {}", 
                    lock.table_name, lock.lock_type, lock.username);
        }
    }
    
    Ok(())
}
```

## 🧪 Тестирование

### Unit тесты

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rustbd::test_utils::TestDatabase;

    #[tokio::test]
    async fn test_user_creation() {
        let db = TestDatabase::new().await;
        
        // Создание пользователя
        let user = User::new("test_user")
            .with_password("test_password")
            .build()
            .unwrap();
        
        let result = db.create_user(&user).await;
        assert!(result.is_ok());
        
        // Проверка создания
        let created_user = db.get_user("test_user").await.unwrap();
        assert_eq!(created_user.username, "test_user");
    }
}
```

### Интеграционные тесты

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use rustbd::test_utils::TestDatabase;

    #[tokio::test]
    async fn test_full_user_workflow() {
        let db = TestDatabase::new().await;
        
        // Создание таблицы
        let schema = TableSchema::new("test_users")
            .add_column("id", ColumnType::Integer, vec![ColumnConstraint::PrimaryKey])
            .add_column("username", ColumnType::Varchar(50), vec![ColumnConstraint::NotNull])
            .build()
            .unwrap();
        
        db.create_table(&schema).await.unwrap();
        
        // Вставка данных
        let row = Row::new()
            .set("username", Value::String("test_user".to_string()))
            .build()
            .unwrap();
        
        db.insert("test_users", row).await.unwrap();
        
        // Проверка данных
        let users = db.query("SELECT * FROM test_users", &[]).await.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("username").unwrap(), "test_user");
    }
}
```

## 🔧 Конфигурация

### Настройка подключения

```rust
use rustbd::{Database, ConnectionConfig, ConnectionPool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Конфигурация подключения
    let config = ConnectionConfig::new()
        .host("localhost")
        .port(5432)
        .database("mydb")
        .username("myuser")
        .password("mypassword")
        .max_connections(20)
        .connection_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(300))
        .build()?;
    
    // Создание пула соединений
    let pool = ConnectionPool::new(config).await?;
    
    // Получение соединения из пула
    let db = pool.get_connection().await?;
    
    // Использование соединения
    let result = db.execute("SELECT 1").await?;
    
    Ok(())
}
```

### Настройка логирования

```rust
use rustbd::{Database, LogLevel, LogConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Конфигурация логирования
    let log_config = LogConfig::new()
        .level(LogLevel::Info)
        .file("rustbd.log")
        .max_size(100 * 1024 * 1024) // 100 MB
        .max_files(5)
        .build()?;
    
    // Инициализация логирования
    rustbd::init_logging(log_config)?;
    
    let db = Database::connect("localhost:5432").await?;
    
    // Теперь все операции будут логироваться
    db.execute("SELECT 1").await?;
    
    Ok(())
}
```

## 📚 Дополнительные примеры

### Работа с JSON данными

```rust
use rustbd::{Database, Value};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Вставка JSON данных
    let json_data = json!({
        "name": "John Doe",
        "age": 30,
        "skills": ["Rust", "SQL", "Database Design"]
    });
    
    let row = Row::new()
        .set("user_data", Value::Json(json_data))
        .build()?;
    
    db.insert("user_profiles", row).await?;
    
    // Запрос JSON данных
    let results = db.query("SELECT user_data->>'name' as name FROM user_profiles", &[]).await?;
    
    for row in results {
        let name: String = row.get("name")?.try_into()?;
        println!("Имя пользователя: {}", name);
    }
    
    Ok(())
}
```

### Работа с массивами

```rust
use rustbd::{Database, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Вставка массива
    let tags = vec!["rust", "database", "performance"];
    let row = Row::new()
        .set("tags", Value::Array(tags.into_iter().map(Value::String).collect()))
        .build()?;
    
    db.insert("articles", row).await?;
    
    // Поиск по элементам массива
    let results = db.query("SELECT * FROM articles WHERE 'rust' = ANY(tags)", &[]).await?;
    
    println!("Найдено {} статей с тегом 'rust'", results.len());
    
    Ok(())
}
```

Эти примеры демонстрируют основные возможности RustBD и помогут разработчикам быстро начать работу с системой. Для получения дополнительной информации обратитесь к полной документации API.
