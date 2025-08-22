# Базовые структуры данных RustBD

Этот документ описывает реализованные базовые структуры данных для системы управления базами данных RustBD.

## 📋 Обзор

Реализованы следующие основные компоненты:

1. **Страницы и блоки** - управление физическим хранением данных
2. **Менеджер буферов** - LRU кэш с различными стратегиями вытеснения
3. **Структуры таблиц** - Tuple, Schema, Row, Table
4. **Управление схемой** - ALTER TABLE операции и валидация

## 🗂️ Структура файлов

```
src/
├── storage/
│   ├── page.rs          # Структуры страниц и менеджер страниц
│   ├── block.rs         # Структуры блоков и менеджер блоков
│   ├── tuple.rs         # Кортежи и схемы таблиц
│   ├── row.rs           # Строки и таблицы
│   └── schema_manager.rs # Управление схемой таблиц
├── core/
│   └── buffer.rs        # Менеджер буферов с LRU кэшем
└── common/
    └── types.rs         # Базовые типы данных
examples/
└── basic_structures.rs  # Примеры использования
```

## 🔧 Основные компоненты

### 1. Страницы (Page)

**Файл:** `src/storage/page.rs`

Страница - это основная единица хранения данных размером 4KB.

#### Ключевые структуры:

- **`PageHeader`** - заголовок страницы с метаданными
- **`Page`** - основная структура страницы с данными и слотами
- **`RecordSlot`** - слот для записи на странице
- **`PageManager`** - менеджер страниц с кэшированием

#### Возможности:

- ✅ Управление свободным местом
- ✅ Слоты для записей
- ✅ CRUD операции с записями
- ✅ Карта свободного места
- ✅ Сериализация/десериализация

#### Пример использования:

```rust
use rustbd::storage::page::{Page, PageType, PageManager};

// Создание страницы
let mut page = Page::new(1, PageType::Data);

// Добавление записи
let record_data = b"Hello, World!";
let offset = page.add_record(record_data, 1)?;

// Получение записи
let retrieved = page.get_record(1).unwrap();

// Удаление записи
page.delete_record(1)?;
```

### 2. Блоки (Block)

**Файл:** `src/storage/block.rs`

Блок - это контейнер для страниц с дополнительными метаданными.

#### Ключевые структуры:

- **`BlockHeader`** - заголовок блока
- **`Block`** - структура блока с страницами
- **`BlockLinks`** - связи между блоками
- **`BlockManager`** - менеджер блоков

#### Возможности:

- ✅ Управление страницами в блоке
- ✅ Связи между блоками
- ✅ Метаданные блока
- ✅ Сериализация/десериализация

#### Пример использования:

```rust
use rustbd::storage::block::{Block, BlockType, BlockManager};

// Создание блока
let mut block = Block::new(1, BlockType::Data, 1024);

// Добавление страницы
let page_data = vec![1, 2, 3, 4, 5];
block.add_page(1, page_data)?;

// Установка связей
block.links.set_next(2);
block.links.set_prev(0);
```

### 3. Менеджер буферов (Buffer Manager)

**Файл:** `src/core/buffer.rs`

Менеджер буферов реализует LRU кэш с различными стратегиями вытеснения.

#### Ключевые структуры:

- **`BufferManager`** - основной менеджер буферов
- **`BufferStats`** - статистика производительности
- **`EvictionStrategy`** - стратегии вытеснения
- **`LRUEntry`** - элемент LRU кэша

#### Стратегии вытеснения:

- **LRU** - Least Recently Used
- **Clock** - Clock алгоритм
- **Adaptive** - Адаптивная стратегия

#### Возможности:

- ✅ LRU кэш страниц
- ✅ Различные стратегии вытеснения
- ✅ Статистика hit/miss ratio
- ✅ Фиксация страниц в памяти
- ✅ Управление "грязными" страницами

#### Пример использования:

```rust
use rustbd::core::buffer::{BufferManager, EvictionStrategy};

// Создание менеджера буферов
let mut buffer_manager = BufferManager::new(100, EvictionStrategy::LRU);

// Добавление страницы
let page = Page::new(1, PageType::Data);
buffer_manager.add_page(page)?;

// Получение страницы
let page_ref = buffer_manager.get_page(1);

// Получение статистики
let stats = buffer_manager.get_stats();
println!("Hit ratio: {:.2}", stats.hit_ratio());
```

### 4. Кортежи и схемы (Tuple & Schema)

**Файл:** `src/storage/tuple.rs`

Кортежи представляют строки данных, схемы определяют структуру таблиц.

#### Ключевые структуры:

- **`Tuple`** - кортеж с версионированием (MVCC)
- **`Schema`** - схема таблицы с ограничениями
- **`Constraint`** - ограничения таблицы
- **`Trigger`** - триггеры таблицы
- **`TableOptions`** - опции таблицы

#### Возможности:

- ✅ Версионирование кортежей (MVCC)
- ✅ Валидация данных по схеме
- ✅ Ограничения и триггеры
- ✅ Гибкие опции таблицы
- ✅ Сериализация/десериализация

#### Пример использования:

```rust
use rustbd::storage::tuple::{Tuple, Schema, Column, DataType};

// Создание схемы
let mut schema = Schema::new("users".to_string())
    .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
    .add_column(Column::new("name".to_string(), DataType::Varchar("".to_string())))
    .primary_key(vec!["id".to_string()]);

// Создание кортежа
let mut tuple = Tuple::new(1);
tuple.set_value("id", ColumnValue::new(DataType::Integer(1)));
tuple.set_value("name", ColumnValue::new(DataType::Varchar("John".to_string())));

// Валидация
schema.validate_tuple(&tuple)?;
```

### 5. Строки и таблицы (Row & Table)

**Файл:** `src/storage/row.rs`

Строки и таблицы обеспечивают логическое представление данных.

#### Ключевые структуры:

- **`Row`** - строка таблицы с версионированием
- **`Table`** - таблица с данными
- **`TableMetadata`** - метаданные таблицы
- **`TableStats`** - статистика таблицы

#### Возможности:

- ✅ Управление версиями строк
- ✅ CRUD операции с таблицами
- ✅ Статистика операций
- ✅ Связи с индексами
- ✅ Метаданные таблицы

#### Пример использования:

```rust
use rustbd::storage::row::{Row, Table};

// Создание таблицы
let schema = Schema::new("products".to_string());
let mut table = Table::new("products".to_string(), schema);

// Создание и добавление строки
let tuple = Tuple::new(1);
let row = Row::new(1, tuple);
table.insert_row(row)?;

// Обновление строки
let mut new_values = HashMap::new();
new_values.insert("price".to_string(), ColumnValue::new(DataType::Double(99.99)));
table.update_row(1, new_values)?;

// Удаление строки
table.delete_row(1)?;
```

### 6. Управление схемой (Schema Manager)

**Файл:** `src/storage/schema_manager.rs`

Менеджер схем обеспечивает ALTER TABLE операции и валидацию изменений.

#### Ключевые структуры:

- **`SchemaManager`** - основной менеджер схем
- **`SchemaOperation`** - операции изменения схемы
- **`SchemaValidator`** - валидаторы схем
- **`SchemaChange`** - история изменений

#### Поддерживаемые операции:

- ✅ ADD COLUMN
- ✅ DROP COLUMN
- ✅ MODIFY COLUMN
- ✅ RENAME COLUMN
- ✅ ADD/DROP CONSTRAINT
- ✅ ADD/DROP INDEX
- ✅ MODIFY PRIMARY KEY
- ✅ MODIFY TABLE OPTIONS

#### Возможности:

- ✅ Валидация изменений схемы
- ✅ История изменений
- ✅ Откат изменений
- ✅ Плагинная система валидации
- ✅ Проверка совместимости типов

#### Пример использования:

```rust
use rustbd::storage::schema_manager::{SchemaManager, SchemaOperation, BasicSchemaValidator};

// Создание менеджера схем
let mut schema_manager = SchemaManager::new();

// Регистрация валидатора
let validator = Box::new(BasicSchemaValidator);
schema_manager.register_validator(validator);

// Добавление колонки
let new_column = Column::new("age".to_string(), DataType::Integer(0));
let operation = SchemaOperation::AddColumn {
    column: new_column,
    after: None,
};

schema_manager.alter_table("users", operation)?;
```

## 🧪 Тестирование

Все компоненты включают модульные тесты:

```bash
# Запуск всех тестов
cargo test

# Запуск тестов конкретного модуля
cargo test storage::page
cargo test core::buffer
```

## 📊 Примеры использования

Полные примеры использования всех компонентов находятся в файле `examples/basic_structures.rs`:

```bash
# Запуск примеров
cargo run --example basic_structures
```

## 🔍 Производительность

### Менеджер буферов

- **LRU стратегия**: O(1) для доступа, O(1) для вытеснения
- **Clock стратегия**: O(n) в худшем случае для вытеснения
- **Адаптивная стратегия**: автоматический выбор между LRU и Clock

### Страницы

- **Поиск свободного места**: O(n) где n - размер страницы
- **Добавление записи**: O(1) после поиска места
- **Удаление записи**: O(1) по ID

### Таблицы

- **Поиск строки**: O(1) по ID
- **Вставка строки**: O(1) + валидация схемы
- **Обновление строки**: O(1) + создание новой версии

## 🚀 Планы развития

### Краткосрочные (1-2 месяца)

- [ ] Полная сериализация/десериализация страниц и блоков
- [ ] Оптимизация алгоритмов поиска свободного места
- [ ] Расширенные стратегии вытеснения для буфера

### Среднесрочные (3-6 месяцев)

- [ ] Сжатие данных на страницах
- [ ] Адаптивные размеры страниц
- [ ] Распределенное кэширование

### Долгосрочные (6+ месяцев)

- [ ] Машинное обучение для оптимизации буфера
- [ ] Гибридные стратегии хранения
- [ ] Интеграция с внешними системами кэширования

## 📚 Дополнительные ресурсы

- [Архитектура RustBD](ARCHITECTURE.md)
- [Руководство по разработке](DEVELOPMENT.md)
- [Стандарты кодирования](CODING_STANDARDS.md)
- [Руководство по тестированию](TESTING_GUIDE.md)

## 🤝 Вклад в проект

Если вы хотите внести вклад в развитие базовых структур данных:

1. Изучите [руководство по вкладу](CONTRIBUTING.md)
2. Следуйте [стандартам кодирования](CODING_STANDARDS.md)
3. Добавьте тесты для новых функций
4. Обновите документацию

## 📄 Лицензия

Проект распространяется под лицензией MIT. См. файл [LICENSE](LICENSE) для подробностей.
