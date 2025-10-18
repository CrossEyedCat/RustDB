//! Операторы выполнения для rustdb

use crate::common::{Error, Result};
use crate::planner::{ExecutionPlan, PlanNode};
use crate::Row;
use crate::PageId;
use crate::storage::index::BPlusTree;
use crate::storage::page_manager::PageManager as StoragePageManager;
use crate::storage::tuple::Tuple;
use crate::common::types::{DataType, ColumnValue};

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Базовый трейт для всех операторов
pub trait Operator {
    /// Получить следующую строку результата
    fn next(&mut self) -> Result<Option<Row>>;
    
    /// Сбросить оператор для повторного выполнения
    fn reset(&mut self) -> Result<()>;
    
    /// Получить схему результата
    fn get_schema(&self) -> Result<Vec<String>>;
    
    /// Получить статистику выполнения
    fn get_statistics(&self) -> OperatorStatistics;
}

/// Статистика выполнения оператора
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperatorStatistics {
    /// Количество обработанных строк
    pub rows_processed: usize,
    /// Количество возвращенных строк
    pub rows_returned: usize,
    /// Время выполнения в миллисекундах
    pub execution_time_ms: u64,
    /// Количество операций I/O
    pub io_operations: usize,
    /// Количество операций с памятью
    pub memory_operations: usize,
    /// Использованная память в байтах
    pub memory_used_bytes: usize,
}

/// Оператор сканирования таблицы
pub struct TableScanOperator {
    /// Имя таблицы
    table_name: String,
    /// Менеджер страниц
    page_manager: Arc<Mutex<StoragePageManager>>,
    /// Текущая страница
    current_page_id: Option<PageId>,
    /// Текущая позиция в странице
    current_position: usize,
    /// Условие фильтрации
    filter_condition: Option<String>,
    /// Схема таблицы
    schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Буфер страниц
    page_buffer: HashMap<PageId, Vec<u8>>,
    /// Максимальный размер буфера
    max_buffer_size: usize,
}

impl TableScanOperator {
    /// Создать новый оператор сканирования таблицы
    pub fn new(
        table_name: String,
        page_manager: Arc<Mutex<StoragePageManager>>,
        filter_condition: Option<String>,
        schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            table_name,
            page_manager,
            current_page_id: None,
            current_position: 0,
            filter_condition,
            schema,
            statistics: OperatorStatistics::default(),
            page_buffer: HashMap::new(),
            max_buffer_size: 100, // Максимум 100 страниц в буфере
        })
    }

    /// Загрузить страницу в буфер (упрощенная версия)
    fn load_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        // Проверяем, есть ли страница в буфере
        if let Some(page_data) = self.page_buffer.get(&page_id) {
            return Ok(page_data.clone());
        }

        // Упрощенная реализация - создаем тестовые данные
        let page_data = vec![0u8; 4096]; // Стандартный размер страницы
        
        // Добавляем в буфер
        if self.page_buffer.len() >= self.max_buffer_size {
            // Удаляем самую старую страницу (простая стратегия FIFO)
            let oldest_page = self.page_buffer.keys().next().cloned();
            if let Some(old_page_id) = oldest_page {
                self.page_buffer.remove(&old_page_id);
            }
        }
        
        self.page_buffer.insert(page_id, page_data.clone());
        self.statistics.io_operations += 1;
        
        Ok(page_data)
    }

    /// Найти следующую страницу с данными (упрощенная версия)
    fn find_next_page(&mut self) -> Result<Option<PageId>> {
        // Упрощенная реализация - возвращаем последовательные страницы
        let next_page = if let Some(current) = self.current_page_id {
            current + 1
        } else {
            1 // Первая страница данных
        };

        // Ограничиваем количество страниц для тестирования
        if next_page <= 10 {
            Ok(Some(next_page))
        } else {
            Ok(None)
        }
    }

    /// Применить фильтр к строке
    fn apply_filter(&self, row: &Row) -> bool {
        if let Some(condition) = &self.filter_condition {
            // Упрощенная реализация фильтрации
            self.evaluate_condition(row, condition)
        } else {
            true // Нет фильтра - пропускаем все строки
        }
    }

    /// Оценить условие фильтрации
    fn evaluate_condition(&self, row: &Row, condition: &str) -> bool {
        // Упрощенная реализация - просто проверяем наличие подстроки
        let row_string = format!("{:?}", row);
        row_string.contains(condition)
    }
}

impl Operator for TableScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // Если у нас нет текущей страницы, находим следующую
            if self.current_page_id.is_none() {
                self.current_page_id = self.find_next_page()?;
                if self.current_page_id.is_none() {
                    // Больше страниц нет
                    break;
                }
                self.current_position = 0;
            }

            let page_id = self.current_page_id.unwrap();
            let page_data = self.load_page(page_id)?;

            // Парсим страницу и извлекаем строки
            let rows = self.parse_page_rows(&page_data)?;

            // Обрабатываем строки на текущей странице
            while self.current_position < rows.len() {
                let row = &rows[self.current_position];
                self.current_position += 1;
                self.statistics.rows_processed += 1;

                // Применяем фильтр
                if self.apply_filter(row) {
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(row.clone()));
                }
            }

            // Переходим к следующей странице
            self.current_page_id = None;
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.current_page_id = None;
        self.current_position = 0;
        self.statistics = OperatorStatistics::default();
        self.page_buffer.clear();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

impl TableScanOperator {
    /// Парсить строки из данных страницы
    fn parse_page_rows(&self, _page_data: &[u8]) -> Result<Vec<Row>> {
        // Упрощенная реализация - создаем тестовые строки
        let mut rows = Vec::new();
        
        // Создаем несколько тестовых строк
        for i in 0..10 {
            let mut row = Row::new();
            for col in &self.schema {
                row.set_value(col, ColumnValue::new(DataType::Varchar(format!("{}_{}", col, i))));
            }
            rows.push(row);
        }

        Ok(rows)
    }
}

/// Оператор сканирования по индексу
pub struct IndexScanOperator {
    /// Имя таблицы
    table_name: String,
    /// Имя индекса
    index_name: String,
    /// Индекс для сканирования
    index: Arc<Mutex<BPlusTree<String, PageId>>>,
    /// Менеджер страниц
    page_manager: Arc<Mutex<StoragePageManager>>,
    /// Условия поиска
    search_conditions: Vec<IndexCondition>,
    /// Текущая позиция в результате индекса
    current_position: usize,
    /// Результат поиска по индексу
    index_result: Vec<PageId>,
    /// Схема таблицы
    schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Буфер страниц
    page_buffer: HashMap<PageId, Vec<u8>>,
}

/// Условие поиска по индексу
#[derive(Debug, Clone)]
pub struct IndexCondition {
    /// Имя колонки
    pub column: String,
    /// Оператор сравнения
    pub operator: IndexOperator,
    /// Значение для сравнения
    pub value: String,
}

/// Оператор сравнения для индекса
#[derive(Debug, Clone)]
pub enum IndexOperator {
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Between,
    In,
}

impl IndexScanOperator {
    /// Создать новый оператор сканирования по индексу
    pub fn new(
        table_name: String,
        index_name: String,
        index: Arc<Mutex<BPlusTree<String, PageId>>>,
        page_manager: Arc<Mutex<StoragePageManager>>,
        search_conditions: Vec<IndexCondition>,
        schema: Vec<String>,
    ) -> Result<Self> {
        let mut operator = Self {
            table_name,
            index_name,
            index,
            page_manager,
            search_conditions,
            current_position: 0,
            index_result: Vec::new(),
            schema,
            statistics: OperatorStatistics::default(),
            page_buffer: HashMap::new(),
        };

        // Выполняем поиск по индексу
        operator.perform_index_search()?;

        Ok(operator)
    }

    /// Выполнить поиск по индексу (упрощенная версия)
    fn perform_index_search(&mut self) -> Result<()> {
        // Упрощенная реализация - создаем тестовые результаты
        self.index_result = vec![1, 2, 3, 4, 5]; // Тестовые page_id
        
        self.statistics.io_operations += 1;
        Ok(())
    }

    /// Загрузить страницу в буфер (упрощенная версия)
    fn load_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        // Проверяем, есть ли страница в буфере
        if let Some(page_data) = self.page_buffer.get(&page_id) {
            return Ok(page_data.clone());
        }

        // Упрощенная реализация - создаем тестовые данные
        let page_data = vec![0u8; 4096]; // Стандартный размер страницы
        
        // Добавляем в буфер
        self.page_buffer.insert(page_id, page_data.clone());
        self.statistics.io_operations += 1;
        
        Ok(page_data)
    }

    /// Применить условия поиска к строке
    fn apply_search_conditions(&self, _row: &Row) -> bool {
        // Упрощенная реализация - всегда возвращаем true
        true
    }
}

impl Operator for IndexScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        while self.current_position < self.index_result.len() {
            let page_id = self.index_result[self.current_position];
            self.current_position += 1;

            // Загружаем страницу
            let page_data = self.load_page(page_id)?;
            
            // Парсим строки из страницы
            let rows = self.parse_page_rows(&page_data)?;

            // Ищем подходящую строку
            for row in rows {
                self.statistics.rows_processed += 1;

                if self.apply_search_conditions(&row) {
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(row));
                }
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.current_position = 0;
        self.statistics = OperatorStatistics::default();
        self.page_buffer.clear();
        self.perform_index_search()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

impl IndexScanOperator {
    /// Парсить строки из данных страницы
    fn parse_page_rows(&self, _page_data: &[u8]) -> Result<Vec<Row>> {
        // Упрощенная реализация - создаем тестовые строки
        let mut rows = Vec::new();
        
        // Создаем несколько тестовых строк
        for i in 0..5 {
            let mut row = Row::new();
            for col in &self.schema {
                row.set_value(col, ColumnValue::new(DataType::Varchar(format!("{}_{}", col, i))));
            }
            rows.push(row);
        }

        Ok(rows)
    }
}

/// Оператор сканирования по диапазону
pub struct RangeScanOperator {
    /// Базовый оператор сканирования
    base_operator: Box<dyn Operator>,
    /// Начальное значение диапазона
    start_value: Option<String>,
    /// Конечное значение диапазона
    end_value: Option<String>,
    /// Статистика
    statistics: OperatorStatistics,
}

impl RangeScanOperator {
    /// Создать новый оператор сканирования по диапазону
    pub fn new(
        base_operator: Box<dyn Operator>,
        start_value: Option<String>,
        end_value: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            base_operator,
            start_value,
            end_value,
            statistics: OperatorStatistics::default(),
        })
    }

    /// Проверить, находится ли значение в диапазоне
    fn is_in_range(&self, value: &str) -> bool {
        if let Some(start) = &self.start_value {
            if value < start.as_str() {
                return false;
            }
        }
        
        if let Some(end) = &self.end_value {
            if value > end.as_str() {
                return false;
            }
        }
        
        true
    }
}

impl Operator for RangeScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        while let Some(row) = self.base_operator.next()? {
            self.statistics.rows_processed += 1;

            // Проверяем, находится ли первое значение строки в диапазоне
            // Упрощенная проверка - берем версию строки как строковое значение
            let row_value = row.version.to_string();
            if self.is_in_range(&row_value) {
                self.statistics.rows_returned += 1;
                self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                return Ok(Some(row));
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.base_operator.reset()?;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        self.base_operator.get_schema()
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Оператор сканирования по условию
pub struct ConditionalScanOperator {
    /// Базовый оператор сканирования
    base_operator: Box<dyn Operator>,
    /// Условие фильтрации
    condition: String,
    /// Статистика
    statistics: OperatorStatistics,
}

impl ConditionalScanOperator {
    /// Создать новый оператор сканирования по условию
    pub fn new(base_operator: Box<dyn Operator>, condition: String) -> Result<Self> {
        Ok(Self {
            base_operator,
            condition,
            statistics: OperatorStatistics::default(),
        })
    }

    /// Оценить условие для строки
    fn evaluate_condition(&self, row: &Row) -> bool {
        // Упрощенная реализация - проверяем наличие подстроки
        let row_string = format!("{:?}", row);
        row_string.contains(&self.condition)
    }
}

impl Operator for ConditionalScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        while let Some(row) = self.base_operator.next()? {
            self.statistics.rows_processed += 1;

            if self.evaluate_condition(&row) {
                self.statistics.rows_returned += 1;
                self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                return Ok(Some(row));
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.base_operator.reset()?;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        self.base_operator.get_schema()
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Тип соединения
#[derive(Debug, Clone)]
pub enum JoinType {
    /// INNER JOIN
    Inner,
    /// LEFT OUTER JOIN
    LeftOuter,
    /// RIGHT OUTER JOIN
    RightOuter,
    /// FULL OUTER JOIN
    FullOuter,
}

/// Условие соединения
#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// Левая колонка
    pub left_column: String,
    /// Правая колонка
    pub right_column: String,
    /// Оператор сравнения
    pub operator: JoinOperator,
}

/// Оператор сравнения для соединения
#[derive(Debug, Clone)]
pub enum JoinOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Оператор Nested Loop Join
pub struct NestedLoopJoinOperator {
    /// Левый входной оператор
    left_input: Box<dyn Operator>,
    /// Правый входной оператор
    right_input: Box<dyn Operator>,
    /// Условие соединения
    join_condition: JoinCondition,
    /// Тип соединения
    join_type: JoinType,
    /// Текущая строка из левого входа
    current_left_row: Option<Row>,
    /// Текущая позиция в правом входе
    current_right_position: usize,
    /// Буфер для правого входа
    right_buffer: Vec<Row>,
    /// Схема результата
    schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Размер блока для block nested loop
    block_size: usize,
}

impl NestedLoopJoinOperator {
    /// Создать новый оператор Nested Loop Join
    pub fn new(
        left_input: Box<dyn Operator>,
        right_input: Box<dyn Operator>,
        join_condition: JoinCondition,
        join_type: JoinType,
        block_size: usize,
    ) -> Result<Self> {
        let mut left_schema = left_input.get_schema()?;
        let right_schema = right_input.get_schema()?;
        
        // Объединяем схемы
        left_schema.extend(right_schema);
        
        Ok(Self {
            left_input,
            right_input,
            join_condition,
            join_type,
            current_left_row: None,
            current_right_position: 0,
            right_buffer: Vec::new(),
            schema: left_schema,
            statistics: OperatorStatistics::default(),
            block_size,
        })
    }

    /// Загрузить блок строк из правого входа
    fn load_right_block(&mut self) -> Result<()> {
        self.right_buffer.clear();
        self.current_right_position = 0;
        
        // Загружаем блок строк
        for _ in 0..self.block_size {
            if let Some(row) = self.right_input.next()? {
                self.right_buffer.push(row);
            } else {
                break;
            }
        }
        
        self.statistics.memory_operations += 1;
        Ok(())
    }

    /// Проверить условие соединения
    fn check_join_condition(&self, left_row: &Row, right_row: &Row) -> bool {
        let left_value = left_row.get_value(&self.join_condition.left_column);
        let right_value = right_row.get_value(&self.join_condition.right_column);
        
        match (left_value, right_value) {
            (Some(left), Some(right)) => {
                match self.join_condition.operator {
                    JoinOperator::Equal => left == right,
                    JoinOperator::NotEqual => left != right,
                    JoinOperator::LessThan => self.compare_values(left, right) == std::cmp::Ordering::Less,
                    JoinOperator::LessThanOrEqual => self.compare_values(left, right) != std::cmp::Ordering::Greater,
                    JoinOperator::GreaterThan => self.compare_values(left, right) == std::cmp::Ordering::Greater,
                    JoinOperator::GreaterThanOrEqual => self.compare_values(left, right) != std::cmp::Ordering::Less,
                }
            }
            _ => false,
        }
    }

    /// Сравнить значения колонок
    fn compare_values(&self, left: &ColumnValue, right: &ColumnValue) -> std::cmp::Ordering {
        // Упрощенное сравнение - сравниваем строковые представления
        let left_str = format!("{:?}", left);
        let right_str = format!("{:?}", right);
        left_str.cmp(&right_str)
    }

    /// Объединить строки
    fn combine_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut combined_row = Row::new();
        
        // Копируем значения из левой строки
        for (column, value) in &left_row.values {
            combined_row.set_value(column, value.clone());
        }
        
        // Копируем значения из правой строки
        for (column, value) in &right_row.values {
            combined_row.set_value(column, value.clone());
        }
        
        combined_row
    }
}

impl Operator for NestedLoopJoinOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // Если у нас нет текущей строки из левого входа, получаем следующую
            if self.current_left_row.is_none() {
                self.current_left_row = self.left_input.next()?;
                if self.current_left_row.is_none() {
                    // Больше строк в левом входе нет
                    break;
                }
                
                // Сбрасываем правый вход для новой строки из левого входа
                self.right_input.reset()?;
                self.load_right_block()?;
            }

            let left_row = self.current_left_row.as_ref().unwrap();
            self.statistics.rows_processed += 1;

            // Проверяем строки в текущем блоке правого входа
            while self.current_right_position < self.right_buffer.len() {
                let right_row = &self.right_buffer[self.current_right_position];
                self.current_right_position += 1;

                if self.check_join_condition(left_row, right_row) {
                    let combined_row = self.combine_rows(left_row, right_row);
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(combined_row));
                }
            }

            // Если блок закончился, загружаем следующий
            if self.current_right_position >= self.right_buffer.len() {
                self.load_right_block()?;
                
                // Если больше нет строк в правом входе, переходим к следующей строке из левого
                if self.right_buffer.is_empty() {
                    self.current_left_row = None;
                }
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.left_input.reset()?;
        self.right_input.reset()?;
        self.current_left_row = None;
        self.current_right_position = 0;
        self.right_buffer.clear();
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Оператор Hash Join
pub struct HashJoinOperator {
    /// Левый входной оператор
    left_input: Box<dyn Operator>,
    /// Правый входной оператор
    right_input: Box<dyn Operator>,
    /// Условие соединения
    join_condition: JoinCondition,
    /// Тип соединения
    join_type: JoinType,
    /// Хеш-таблица для правого входа
    hash_table: HashMap<String, Vec<Row>>,
    /// Текущая строка из левого входа
    current_left_row: Option<Row>,
    /// Текущая позиция в списке совпадений
    current_match_position: usize,
    /// Текущий список совпадений
    current_matches: Vec<Row>,
    /// Схема результата
    schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Размер хеш-таблицы
    hash_table_size: usize,
}

impl HashJoinOperator {
    /// Создать новый оператор Hash Join
    pub fn new(
        left_input: Box<dyn Operator>,
        right_input: Box<dyn Operator>,
        join_condition: JoinCondition,
        join_type: JoinType,
        hash_table_size: usize,
    ) -> Result<Self> {
        let mut left_schema = left_input.get_schema()?;
        let right_schema = right_input.get_schema()?;
        
        // Объединяем схемы
        left_schema.extend(right_schema);
        
        let mut operator = Self {
            left_input,
            right_input,
            join_condition,
            join_type,
            hash_table: HashMap::new(),
            current_left_row: None,
            current_match_position: 0,
            current_matches: Vec::new(),
            schema: left_schema,
            statistics: OperatorStatistics::default(),
            hash_table_size,
        };

        // Строим хеш-таблицу
        operator.build_hash_table()?;

        Ok(operator)
    }

    /// Построить хеш-таблицу из правого входа
    fn build_hash_table(&mut self) -> Result<()> {
        self.hash_table.clear();
        
        // Сканируем правый вход и строим хеш-таблицу
        while let Some(row) = self.right_input.next()? {
            let key = self.get_join_key(&row, &self.join_condition.right_column);
            self.hash_table.entry(key).or_insert_with(Vec::new).push(row);
        }
        
        self.statistics.memory_operations += 1;
        Ok(())
    }

    /// Получить ключ соединения
    fn get_join_key(&self, row: &Row, column: &str) -> String {
        if let Some(value) = row.get_value(column) {
            format!("{:?}", value)
        } else {
            "NULL".to_string()
        }
    }

    /// Проверить условие соединения
    fn check_join_condition(&self, left_row: &Row, right_row: &Row) -> bool {
        let left_value = left_row.get_value(&self.join_condition.left_column);
        let right_value = right_row.get_value(&self.join_condition.right_column);
        
        match (left_value, right_value) {
            (Some(left), Some(right)) => {
                match self.join_condition.operator {
                    JoinOperator::Equal => left == right,
                    JoinOperator::NotEqual => left != right,
                    JoinOperator::LessThan => self.compare_values(left, right) == std::cmp::Ordering::Less,
                    JoinOperator::LessThanOrEqual => self.compare_values(left, right) != std::cmp::Ordering::Greater,
                    JoinOperator::GreaterThan => self.compare_values(left, right) == std::cmp::Ordering::Greater,
                    JoinOperator::GreaterThanOrEqual => self.compare_values(left, right) != std::cmp::Ordering::Less,
                }
            }
            _ => false,
        }
    }

    /// Сравнить значения колонок
    fn compare_values(&self, left: &ColumnValue, right: &ColumnValue) -> std::cmp::Ordering {
        // Упрощенное сравнение - сравниваем строковые представления
        let left_str = format!("{:?}", left);
        let right_str = format!("{:?}", right);
        left_str.cmp(&right_str)
    }

    /// Объединить строки
    fn combine_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut combined_row = Row::new();
        
        // Копируем значения из левой строки
        for (column, value) in &left_row.values {
            combined_row.set_value(column, value.clone());
        }
        
        // Копируем значения из правой строки
        for (column, value) in &right_row.values {
            combined_row.set_value(column, value.clone());
        }
        
        combined_row
    }
}

impl Operator for HashJoinOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // Если у нас нет текущей строки из левого входа, получаем следующую
            if self.current_left_row.is_none() {
                self.current_left_row = self.left_input.next()?;
                if self.current_left_row.is_none() {
                    // Больше строк в левом входе нет
                    break;
                }
                
                // Ищем совпадения в хеш-таблице
                let left_row = self.current_left_row.as_ref().unwrap();
                let key = self.get_join_key(left_row, &self.join_condition.left_column);
                
                self.current_matches = self.hash_table.get(&key).cloned().unwrap_or_default();
                self.current_match_position = 0;
            }

            let left_row = self.current_left_row.as_ref().unwrap();
            self.statistics.rows_processed += 1;

            // Проверяем совпадения
            while self.current_match_position < self.current_matches.len() {
                let right_row = &self.current_matches[self.current_match_position];
                self.current_match_position += 1;

                if self.check_join_condition(left_row, right_row) {
                    let combined_row = self.combine_rows(left_row, right_row);
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(combined_row));
                }
            }

            // Если совпадения закончились, переходим к следующей строке из левого входа
            self.current_left_row = None;
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.left_input.reset()?;
        self.right_input.reset()?;
        self.current_left_row = None;
        self.current_match_position = 0;
        self.current_matches.clear();
        self.statistics = OperatorStatistics::default();
        self.build_hash_table()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Оператор Merge Join
pub struct MergeJoinOperator {
    /// Левый входной оператор
    left_input: Box<dyn Operator>,
    /// Правый входной оператор
    right_input: Box<dyn Operator>,
    /// Условие соединения
    join_condition: JoinCondition,
    /// Тип соединения
    join_type: JoinType,
    /// Текущая строка из левого входа
    current_left_row: Option<Row>,
    /// Текущая строка из правого входа
    current_right_row: Option<Row>,
    /// Буфер для строк с одинаковыми ключами
    left_buffer: Vec<Row>,
    right_buffer: Vec<Row>,
    /// Позиции в буферах
    left_buffer_pos: usize,
    right_buffer_pos: usize,
    /// Схема результата
    schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
}

impl MergeJoinOperator {
    /// Создать новый оператор Merge Join
    pub fn new(
        left_input: Box<dyn Operator>,
        right_input: Box<dyn Operator>,
        join_condition: JoinCondition,
        join_type: JoinType,
    ) -> Result<Self> {
        let mut left_schema = left_input.get_schema()?;
        let right_schema = right_input.get_schema()?;
        
        // Объединяем схемы
        left_schema.extend(right_schema);
        
        Ok(Self {
            left_input,
            right_input,
            join_condition,
            join_type,
            current_left_row: None,
            current_right_row: None,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            left_buffer_pos: 0,
            right_buffer_pos: 0,
            schema: left_schema,
            statistics: OperatorStatistics::default(),
        })
    }

    /// Получить ключ соединения
    fn get_join_key(&self, row: &Row, column: &str) -> String {
        if let Some(value) = row.get_value(column) {
            format!("{:?}", value)
        } else {
            "NULL".to_string()
        }
    }

    /// Сравнить ключи соединения
    fn compare_keys(&self, left_key: &str, right_key: &str) -> std::cmp::Ordering {
        left_key.cmp(right_key)
    }

    /// Загрузить строки с одинаковыми ключами в буферы
    fn load_matching_keys(&mut self) -> Result<()> {
        self.left_buffer.clear();
        self.right_buffer.clear();
        self.left_buffer_pos = 0;
        self.right_buffer_pos = 0;

        // Получаем текущие строки
        if self.current_left_row.is_none() {
            self.current_left_row = self.left_input.next()?;
        }
        if self.current_right_row.is_none() {
            self.current_right_row = self.right_input.next()?;
        }

        if let (Some(left_row), Some(right_row)) = (&self.current_left_row, &self.current_right_row) {
            let left_key = self.get_join_key(left_row, &self.join_condition.left_column);
            let right_key = self.get_join_key(right_row, &self.join_condition.right_column);

            match self.compare_keys(&left_key, &right_key) {
                std::cmp::Ordering::Equal => {
                    // Загружаем все строки с одинаковыми ключами
                    let target_key = left_key.clone();
                    
                    // Загружаем строки из левого входа
                    while let Some(row) = &self.current_left_row {
                        let key = self.get_join_key(row, &self.join_condition.left_column);
                        if key == target_key {
                            self.left_buffer.push(row.clone());
                            self.current_left_row = self.left_input.next()?;
                        } else {
                            break;
                        }
                    }

                    // Загружаем строки из правого входа
                    while let Some(row) = &self.current_right_row {
                        let key = self.get_join_key(row, &self.join_condition.right_column);
                        if key == target_key {
                            self.right_buffer.push(row.clone());
                            self.current_right_row = self.right_input.next()?;
                        } else {
                            break;
                        }
                    }
                }
                std::cmp::Ordering::Less => {
                    // Левая строка меньше, переходим к следующей левой
                    self.current_left_row = self.left_input.next()?;
                }
                std::cmp::Ordering::Greater => {
                    // Правая строка меньше, переходим к следующей правой
                    self.current_right_row = self.right_input.next()?;
                }
            }
        }

        Ok(())
    }

    /// Объединить строки
    fn combine_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut combined_row = Row::new();
        
        // Копируем значения из левой строки
        for (column, value) in &left_row.values {
            combined_row.set_value(column, value.clone());
        }
        
        // Копируем значения из правой строки
        for (column, value) in &right_row.values {
            combined_row.set_value(column, value.clone());
        }
        
        combined_row
    }
}

impl Operator for MergeJoinOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // Если буферы пусты, загружаем новые совпадающие ключи
            if self.left_buffer_pos >= self.left_buffer.len() || self.right_buffer_pos >= self.right_buffer.len() {
                self.load_matching_keys()?;
                
                // Если больше нет данных, завершаем
                if self.left_buffer.is_empty() || self.right_buffer.is_empty() {
                    if self.current_left_row.is_none() && self.current_right_row.is_none() {
                        break;
                    }
                    continue;
                }
            }

            // Возвращаем следующую комбинацию строк
            let left_row = &self.left_buffer[self.left_buffer_pos];
            let right_row = &self.right_buffer[self.right_buffer_pos];
            
            self.statistics.rows_processed += 1;
            
            let combined_row = self.combine_rows(left_row, right_row);
            self.statistics.rows_returned += 1;
            self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;

            // Переходим к следующей комбинации
            self.right_buffer_pos += 1;
            if self.right_buffer_pos >= self.right_buffer.len() {
                self.right_buffer_pos = 0;
                self.left_buffer_pos += 1;
            }

            return Ok(Some(combined_row));
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.left_input.reset()?;
        self.right_input.reset()?;
        self.current_left_row = None;
        self.current_right_row = None;
        self.left_buffer.clear();
        self.right_buffer.clear();
        self.left_buffer_pos = 0;
        self.right_buffer_pos = 0;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Фабрика для создания операторов сканирования
pub struct ScanOperatorFactory {
    /// Менеджер страниц
    page_manager: Arc<Mutex<StoragePageManager>>,
    /// Индексы для таблиц
    indexes: HashMap<String, Arc<Mutex<BPlusTree<String, PageId>>>>,
}

impl ScanOperatorFactory {
    /// Создать новую фабрику операторов сканирования
    pub fn new(page_manager: Arc<Mutex<StoragePageManager>>) -> Self {
        Self {
            page_manager,
            indexes: HashMap::new(),
        }
    }

    /// Добавить индекс для таблицы
    pub fn add_index(&mut self, table_name: String, index: Arc<Mutex<BPlusTree<String, PageId>>>) {
        self.indexes.insert(table_name, index);
    }

    /// Создать оператор сканирования таблицы
    pub fn create_table_scan(
        &self,
        table_name: String,
        filter: Option<String>,
        schema: Vec<String>,
    ) -> Result<Box<dyn Operator>> {
        let operator = TableScanOperator::new(
            table_name,
            self.page_manager.clone(),
            filter,
            schema,
        )?;
        Ok(Box::new(operator))
    }

    /// Создать оператор сканирования по диапазону
    pub fn create_range_scan(
        &self,
        base_operator: Box<dyn Operator>,
        start_value: Option<String>,
        end_value: Option<String>,
    ) -> Result<Box<dyn Operator>> {
        let operator = RangeScanOperator::new(
            base_operator,
            start_value,
            end_value,
        )?;
        Ok(Box::new(operator))
    }

    /// Создать оператор сканирования с условием
    pub fn create_conditional_scan(
        &self,
        base_operator: Box<dyn Operator>,
        condition: String,
    ) -> Result<Box<dyn Operator>> {
        let operator = ConditionalScanOperator::new(
            base_operator,
            condition,
        )?;
        Ok(Box::new(operator))
    }

    /// Создать оператор сканирования по индексу
    pub fn create_index_scan(
        &self,
        table_name: String,
        index_name: String,
        search_conditions: Vec<IndexCondition>,
        schema: Vec<String>,
    ) -> Result<Box<dyn Operator>> {
        if let Some(index) = self.indexes.get(&table_name) {
            let operator = IndexScanOperator::new(
                table_name,
                index_name,
                index.clone(),
                self.page_manager.clone(),
                search_conditions,
                schema,
            )?;
            Ok(Box::new(operator))
        } else {
            Err(Error::query_execution("Индекс не найден для таблицы"))
        }
    }
}

/// Тип агрегатной функции
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Группа для агрегации
#[derive(Debug, Clone)]
pub struct AggregateGroup {
    /// Ключи группировки
    pub keys: Vec<ColumnValue>,
    /// Значения агрегатных функций
    pub aggregates: Vec<ColumnValue>,
    /// Количество строк в группе
    pub count: usize,
}

/// Упрощенный оператор группировки (демонстрационная версия)
pub struct HashGroupByOperator {
    /// Входной оператор
    input: Box<dyn Operator>,
    /// Ключи группировки (индексы колонок)
    group_keys: Vec<usize>,
    /// Агрегатные функции
    aggregate_functions: Vec<(AggregateFunction, usize)>,
    /// Схема результата
    result_schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Обработанные результаты
    results: Vec<Row>,
    /// Текущий индекс
    current_index: usize,
}

impl HashGroupByOperator {
    /// Создать новый оператор группировки
    pub fn new(
        input: Box<dyn Operator>,
        group_keys: Vec<usize>,
        aggregate_functions: Vec<(AggregateFunction, usize)>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            input,
            group_keys,
            aggregate_functions,
            result_schema,
            statistics: OperatorStatistics::default(),
            results: Vec::new(),
            current_index: 0,
        })
    }

    /// Обработать входные данные (упрощенная версия)
    fn process_input(&mut self) -> Result<()> {
        // Читаем все строки из входного оператора
        while let Some(_row) = self.input.next()? {
            self.statistics.rows_processed += 1;
            
            // Создаем упрощенный результат группировки
            let mut result_tuple = Tuple::new(self.results.len() as u64);
            
            // Добавляем ключи группировки (используем первые несколько колонок)
            for (i, &key_index) in self.group_keys.iter().enumerate() {
                if key_index < 4 { // Ограничиваем для демонстрации
                    result_tuple.set_value(&format!("key_{}", i), 
                        ColumnValue::new(DataType::Integer(i as i32)));
                }
            }
            
            // Добавляем агрегатные функции (демонстрационные значения)
            for (i, (function, _)) in self.aggregate_functions.iter().enumerate() {
                match function {
                    AggregateFunction::Count => {
                        result_tuple.set_value(&format!("count_{}", i), 
                            ColumnValue::new(DataType::BigInt(1)));
                    }
                    AggregateFunction::Sum => {
                        result_tuple.set_value(&format!("sum_{}", i), 
                            ColumnValue::new(DataType::Double(100.0)));
                    }
                    AggregateFunction::Avg => {
                        result_tuple.set_value(&format!("avg_{}", i), 
                            ColumnValue::new(DataType::Double(50.0)));
                    }
                    AggregateFunction::Min => {
                        result_tuple.set_value(&format!("min_{}", i), 
                            ColumnValue::new(DataType::Integer(10)));
                    }
                    AggregateFunction::Max => {
                        result_tuple.set_value(&format!("max_{}", i), 
                            ColumnValue::new(DataType::Integer(100)));
                    }
                    AggregateFunction::CountDistinct => {
                        result_tuple.set_value(&format!("count_distinct_{}", i), 
                            ColumnValue::new(DataType::BigInt(5)));
                    }
                }
            }
            
            let result_row = Row::new();
            self.results.push(result_row);
        }
        
        Ok(())
    }
}

impl Operator for HashGroupByOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        // Если это первый вызов, обрабатываем входные данные
        if self.results.is_empty() && self.current_index == 0 {
            self.process_input()?;
        }

        // Возвращаем следующую группу
        if self.current_index < self.results.len() {
            let row = self.results[self.current_index].clone();
            self.current_index += 1;
            self.statistics.rows_returned += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.results.clear();
        self.current_index = 0;
        self.statistics = OperatorStatistics::default();
        self.input.reset()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Упрощенный оператор сортировки (демонстрационная версия)
pub struct SortOperator {
    /// Входной оператор
    input: Box<dyn Operator>,
    /// Индексы колонок для сортировки
    sort_columns: Vec<usize>,
    /// Направление сортировки (true = ASC, false = DESC)
    sort_directions: Vec<bool>,
    /// Схема результата
    result_schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Отсортированные строки
    sorted_rows: Vec<Row>,
    /// Текущий индекс
    current_index: usize,
}

impl SortOperator {
    /// Создать новый оператор сортировки
    pub fn new(
        input: Box<dyn Operator>,
        sort_columns: Vec<usize>,
        sort_directions: Vec<bool>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        if sort_columns.len() != sort_directions.len() {
            return Err(Error::QueryExecution { message: "Количество колонок и направлений сортировки не совпадает".to_string() });
        }

        Ok(Self {
            input,
            sort_columns,
            sort_directions,
            result_schema,
            statistics: OperatorStatistics::default(),
            sorted_rows: Vec::new(),
            current_index: 0,
        })
    }

    /// Загрузить и отсортировать все строки (упрощенная версия)
    fn load_and_sort(&mut self) -> Result<()> {
        let mut rows = Vec::new();
        
        // Читаем все строки из входного оператора
        while let Some(row) = self.input.next()? {
            rows.push(row);
            self.statistics.rows_processed += 1;
        }

        // Простая сортировка по версии (демонстрационная версия)
        rows.sort_by(|a, b| {
            a.version.cmp(&b.version)
        });

        self.sorted_rows = rows;
        self.current_index = 0;
        
        Ok(())
    }
}

impl Operator for SortOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        // Если это первый вызов, загружаем и сортируем данные
        if self.sorted_rows.is_empty() && self.current_index == 0 {
            self.load_and_sort()?;
        }

        // Возвращаем следующую строку
        if self.current_index < self.sorted_rows.len() {
            let row = self.sorted_rows[self.current_index].clone();
            self.current_index += 1;
            self.statistics.rows_returned += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.sorted_rows.clear();
        self.current_index = 0;
        self.statistics = OperatorStatistics::default();
        self.input.reset()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Упрощенный оператор сортировки-группировки (демонстрационная версия)
pub struct SortGroupByOperator {
    /// Входной оператор
    input: Box<dyn Operator>,
    /// Ключи группировки (индексы колонок)
    group_keys: Vec<usize>,
    /// Агрегатные функции
    aggregate_functions: Vec<(AggregateFunction, usize)>,
    /// Схема результата
    result_schema: Vec<String>,
    /// Статистика
    statistics: OperatorStatistics,
    /// Результаты групп
    group_results: Vec<Row>,
    /// Индекс результата
    result_index: usize,
}

impl SortGroupByOperator {
    /// Создать новый оператор сортировки-группировки
    pub fn new(
        input: Box<dyn Operator>,
        group_keys: Vec<usize>,
        aggregate_functions: Vec<(AggregateFunction, usize)>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            input,
            group_keys,
            aggregate_functions,
            result_schema,
            statistics: OperatorStatistics::default(),
            group_results: Vec::new(),
            result_index: 0,
        })
    }

    /// Загрузить и обработать данные (упрощенная версия)
    fn load_and_process(&mut self) -> Result<()> {
        let mut rows = Vec::new();
        
        // Читаем все строки из входного оператора
        while let Some(row) = self.input.next()? {
            rows.push(row);
            self.statistics.rows_processed += 1;
        }

        // Создаем демонстрационные результаты групп
        for (i, _) in rows.iter().enumerate().take(3) { // Ограничиваем для демонстрации
            let mut result_tuple = Tuple::new(i as u64);
            
            // Добавляем ключи группировки
            for (j, &key_index) in self.group_keys.iter().enumerate() {
                if key_index < 4 {
                    result_tuple.set_value(&format!("group_key_{}", j), 
                        ColumnValue::new(DataType::Integer(i as i32)));
                }
            }
            
            // Добавляем агрегатные функции
            for (j, (function, _)) in self.aggregate_functions.iter().enumerate() {
                match function {
                    AggregateFunction::Count => {
                        result_tuple.set_value(&format!("count_{}", j), 
                            ColumnValue::new(DataType::BigInt((i + 1) as i64)));
                    }
                    AggregateFunction::Sum => {
                        result_tuple.set_value(&format!("sum_{}", j), 
                            ColumnValue::new(DataType::Double((i + 1) as f64 * 100.0)));
                    }
                    AggregateFunction::Avg => {
                        result_tuple.set_value(&format!("avg_{}", j), 
                            ColumnValue::new(DataType::Double((i + 1) as f64 * 50.0)));
                    }
                    AggregateFunction::Min => {
                        result_tuple.set_value(&format!("min_{}", j), 
                            ColumnValue::new(DataType::Integer(((i + 1) * 10) as i32)));
                    }
                    AggregateFunction::Max => {
                        result_tuple.set_value(&format!("max_{}", j), 
                            ColumnValue::new(DataType::Integer(((i + 1) * 100) as i32)));
                    }
                    AggregateFunction::CountDistinct => {
                        result_tuple.set_value(&format!("count_distinct_{}", j), 
                            ColumnValue::new(DataType::BigInt((i + 1) as i64)));
                    }
                }
            }
            
            let result_row = Row::new();
            self.group_results.push(result_row);
        }
        
        Ok(())
    }
}

impl Operator for SortGroupByOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        // Если это первый вызов, загружаем и обрабатываем данные
        if self.group_results.is_empty() && self.result_index == 0 {
            self.load_and_process()?;
        }

        // Возвращаем следующий результат группы
        if self.result_index < self.group_results.len() {
            let row = self.group_results[self.result_index].clone();
            self.result_index += 1;
            self.statistics.rows_returned += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.group_results.clear();
        self.result_index = 0;
        self.statistics = OperatorStatistics::default();
        self.input.reset()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Фабрика для создания операторов агрегации и сортировки
pub struct AggregationSortOperatorFactory;

impl AggregationSortOperatorFactory {
    /// Создать оператор группировки
    pub fn create_group_by(
        input: Box<dyn Operator>,
        group_keys: Vec<usize>,
        aggregate_functions: Vec<(AggregateFunction, usize)>,
        result_schema: Vec<String>,
        use_hash: bool,
    ) -> Result<Box<dyn Operator>> {
        if use_hash {
            Ok(Box::new(HashGroupByOperator::new(
                input,
                group_keys,
                aggregate_functions,
                result_schema,
            )?))
        } else {
            Ok(Box::new(SortGroupByOperator::new(
                input,
                group_keys,
                aggregate_functions,
                result_schema,
            )?))
        }
    }

    /// Создать оператор сортировки
    pub fn create_sort(
        input: Box<dyn Operator>,
        sort_columns: Vec<usize>,
        sort_directions: Vec<bool>,
        result_schema: Vec<String>,
    ) -> Result<Box<dyn Operator>> {
        Ok(Box::new(SortOperator::new(
            input,
            sort_columns,
            sort_directions,
            result_schema,
        )?))
    }
}
