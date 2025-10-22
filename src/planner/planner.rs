//! Планировщик запросов для rustdb

use crate::analyzer::{AnalysisContext, SemanticAnalyzer};
use crate::common::{Error, Result};
use crate::parser::ast::{
    DeleteStatement, InsertStatement, SelectStatement, SqlStatement, UpdateStatement,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// План выполнения запроса
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Корневой оператор плана
    pub root: PlanNode,
    /// Метаданные плана
    pub metadata: PlanMetadata,
}

/// Метаданные плана выполнения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanMetadata {
    /// Оценка стоимости выполнения
    pub estimated_cost: f64,
    /// Оценка количества строк результата
    pub estimated_rows: usize,
    /// Время создания плана
    pub created_at: std::time::SystemTime,
    /// Статистика плана
    pub statistics: PlanStatistics,
}

/// Статистика плана выполнения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanStatistics {
    /// Количество операторов в плане
    pub operator_count: usize,
    /// Максимальная глубина плана
    pub max_depth: usize,
    /// Количество таблиц в запросе
    pub table_count: usize,
    /// Количество JOIN операций
    pub join_count: usize,
}

/// Узел плана выполнения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PlanNode {
    /// Сканирование таблицы
    TableScan(TableScanNode),
    /// Сканирование по индексу
    IndexScan(IndexScanNode),
    /// Фильтрация
    Filter(FilterNode),
    /// Проекция (выбор колонок)
    Projection(ProjectionNode),
    /// Соединение таблиц
    Join(JoinNode),
    /// Группировка
    GroupBy(GroupByNode),
    /// Сортировка
    Sort(SortNode),
    /// Ограничение количества строк
    Limit(LimitNode),
    /// Смещение
    Offset(OffsetNode),
    /// Агрегация
    Aggregate(AggregateNode),
    /// Вставка данных
    Insert(InsertNode),
    /// Обновление данных
    Update(UpdateNode),
    /// Удаление данных
    Delete(DeleteNode),
}

/// Узел сканирования таблицы
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableScanNode {
    /// Имя таблицы
    pub table_name: String,
    /// Псевдоним таблицы
    pub alias: Option<String>,
    /// Список колонок для чтения
    pub columns: Vec<String>,
    /// Условие фильтрации (если есть)
    pub filter: Option<String>,
    /// Оценка стоимости
    pub cost: f64,
    /// Оценка количества строк
    pub estimated_rows: usize,
}

/// Узел сканирования по индексу
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexScanNode {
    /// Имя таблицы
    pub table_name: String,
    /// Имя индекса
    pub index_name: String,
    /// Условия поиска по индексу
    pub conditions: Vec<IndexCondition>,
    /// Оценка стоимости
    pub cost: f64,
    /// Оценка количества строк
    pub estimated_rows: usize,
}

/// Условие поиска по индексу
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexCondition {
    /// Имя колонки
    pub column: String,
    /// Оператор сравнения
    pub operator: String,
    /// Значение для сравнения
    pub value: String,
}

/// Узел фильтрации
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterNode {
    /// Условие фильтрации
    pub condition: String,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка селективности
    pub selectivity: f64,
    /// Оценка стоимости
    pub cost: f64,
}

/// Узел проекции
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionNode {
    /// Список колонок для проекции
    pub columns: Vec<ProjectionColumn>,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Колонка проекции
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionColumn {
    /// Имя колонки
    pub name: String,
    /// Выражение для вычисления
    pub expression: Option<String>,
    /// Псевдоним
    pub alias: Option<String>,
}

/// Узел соединения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinNode {
    /// Тип соединения
    pub join_type: JoinType,
    /// Условие соединения
    pub condition: String,
    /// Левый входной узел
    pub left: Box<PlanNode>,
    /// Правый входной узел
    pub right: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Тип соединения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Узел группировки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupByNode {
    /// Колонки для группировки
    pub group_columns: Vec<String>,
    /// Агрегатные функции
    pub aggregates: Vec<AggregateFunction>,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Агрегатная функция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateFunction {
    /// Имя функции
    pub name: String,
    /// Аргумент функции
    pub argument: String,
    /// Псевдоним результата
    pub alias: Option<String>,
}

/// Узел сортировки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortNode {
    /// Колонки для сортировки
    pub sort_columns: Vec<SortColumn>,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Колонка сортировки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortColumn {
    /// Имя колонки
    pub column: String,
    /// Направление сортировки
    pub direction: SortDirection,
}

/// Направление сортировки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Узел ограничения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitNode {
    /// Количество строк для ограничения
    pub limit: usize,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Узел смещения
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OffsetNode {
    /// Количество строк для пропуска
    pub offset: usize,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Узел агрегации
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateNode {
    /// Агрегатные функции
    pub aggregates: Vec<AggregateFunction>,
    /// Входной узел
    pub input: Box<PlanNode>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Узел вставки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertNode {
    /// Имя таблицы
    pub table_name: String,
    /// Колонки для вставки
    pub columns: Vec<String>,
    /// Значения для вставки
    pub values: Vec<Vec<String>>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Узел обновления
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateNode {
    /// Имя таблицы
    pub table_name: String,
    /// Назначения (колонка = значение)
    pub assignments: Vec<Assignment>,
    /// Условие WHERE
    pub where_condition: Option<String>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Назначение в UPDATE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    /// Имя колонки
    pub column: String,
    /// Новое значение
    pub value: String,
}

/// Узел удаления
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteNode {
    /// Имя таблицы
    pub table_name: String,
    /// Условие WHERE
    pub where_condition: Option<String>,
    /// Оценка стоимости
    pub cost: f64,
}

/// Планировщик запросов
pub struct QueryPlanner {
    /// Семантический анализатор
    semantic_analyzer: SemanticAnalyzer,
    /// Настройки планировщика
    settings: PlannerSettings,
    /// Кэш планов
    plan_cache: HashMap<String, ExecutionPlan>,
}

/// Настройки планировщика
#[derive(Debug, Clone)]
pub struct PlannerSettings {
    /// Включить кэширование планов
    pub enable_plan_cache: bool,
    /// Максимальный размер кэша планов
    pub max_cache_size: usize,
    /// Включить оптимизацию
    pub enable_optimization: bool,
    /// Максимальная глубина рекурсии
    pub max_recursion_depth: usize,
    /// Включить детальное логирование
    pub enable_debug_logging: bool,
}

impl Default for PlannerSettings {
    fn default() -> Self {
        Self {
            enable_plan_cache: true,
            max_cache_size: 1000,
            enable_optimization: true,
            max_recursion_depth: 100,
            enable_debug_logging: false,
        }
    }
}

impl QueryPlanner {
    /// Создать новый планировщик запросов
    pub fn new() -> Result<Self> {
        let semantic_analyzer = SemanticAnalyzer::default();
        Ok(Self {
            semantic_analyzer,
            settings: PlannerSettings::default(),
            plan_cache: HashMap::new(),
        })
    }

    /// Создать планировщик с настройками
    pub fn with_settings(settings: PlannerSettings) -> Result<Self> {
        let semantic_analyzer = SemanticAnalyzer::default();
        Ok(Self {
            semantic_analyzer,
            settings,
            plan_cache: HashMap::new(),
        })
    }

    /// Создать план выполнения для SQL запроса
    pub fn create_plan(&mut self, sql_statement: &SqlStatement) -> Result<ExecutionPlan> {
        // Сначала выполняем семантический анализ
        let context = AnalysisContext::default();
        let analysis_result = self.semantic_analyzer.analyze(sql_statement, &context)?;

        if !analysis_result.errors.is_empty() {
            return Err(Error::semantic_analysis(format!(
                "Семантические ошибки: {:?}",
                analysis_result.errors
            )));
        }

        // Создаем план в зависимости от типа запроса
        let root = match sql_statement {
            SqlStatement::Select(select) => self.create_select_plan(select)?,
            SqlStatement::Insert(insert) => self.create_insert_plan(insert)?,
            SqlStatement::Update(update) => self.create_update_plan(update)?,
            SqlStatement::Delete(delete) => self.create_delete_plan(delete)?,
            _ => return Err(Error::semantic_analysis("Неподдерживаемый тип запроса")),
        };

        // Создаем метаданные плана
        let metadata = self.create_plan_metadata(&root)?;

        Ok(ExecutionPlan { root, metadata })
    }

    /// Создать план для SELECT запроса
    fn create_select_plan(&self, select: &SelectStatement) -> Result<PlanNode> {
        // Создаем базовый план сканирования таблицы
        let mut current_plan = if let Some(from) = &select.from {
            self.create_table_scan_plan(&from.table)?
        } else {
            // Если нет FROM, создаем пустой план
            PlanNode::TableScan(TableScanNode {
                table_name: "".to_string(),
                alias: None,
                columns: vec![],
                filter: None,
                cost: 0.0,
                estimated_rows: 0,
            })
        };

        // Добавляем JOIN операции
        if let Some(from) = &select.from {
            for join in &from.joins {
                let join_plan = self.create_table_scan_plan(&join.table)?;
                current_plan = PlanNode::Join(JoinNode {
                    join_type: match join.join_type {
                        crate::parser::ast::JoinType::Inner => JoinType::Inner,
                        crate::parser::ast::JoinType::Left => JoinType::Left,
                        crate::parser::ast::JoinType::Right => JoinType::Right,
                        crate::parser::ast::JoinType::Full => JoinType::Full,
                        crate::parser::ast::JoinType::Cross => JoinType::Cross,
                    },
                    condition: join
                        .condition
                        .as_ref()
                        .map(|e| format!("{:?}", e))
                        .unwrap_or_default(),
                    left: Box::new(current_plan),
                    right: Box::new(join_plan),
                    cost: 0.0, // TODO: Рассчитать стоимость
                });
            }
        }

        // Добавляем WHERE условие
        if let Some(where_clause) = &select.where_clause {
            current_plan = PlanNode::Filter(FilterNode {
                condition: format!("{:?}", where_clause),
                input: Box::new(current_plan),
                selectivity: 0.5, // TODO: Рассчитать селективность
                cost: 0.0,
            });
        }

        // Добавляем GROUP BY
        if !select.group_by.is_empty() {
            current_plan = PlanNode::GroupBy(GroupByNode {
                group_columns: select.group_by.iter().map(|e| format!("{:?}", e)).collect(),
                aggregates: vec![], // TODO: Извлечь агрегатные функции
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Добавляем HAVING
        if let Some(having) = &select.having {
            current_plan = PlanNode::Filter(FilterNode {
                condition: format!("{:?}", having),
                input: Box::new(current_plan),
                selectivity: 0.5,
                cost: 0.0,
            });
        }

        // Добавляем ORDER BY
        if !select.order_by.is_empty() {
            current_plan = PlanNode::Sort(SortNode {
                sort_columns: select
                    .order_by
                    .iter()
                    .map(|item| SortColumn {
                        column: format!("{:?}", item.expr),
                        direction: match item.direction {
                            crate::parser::ast::OrderDirection::Asc => SortDirection::Asc,
                            crate::parser::ast::OrderDirection::Desc => SortDirection::Desc,
                        },
                    })
                    .collect(),
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Добавляем LIMIT
        if let Some(limit) = select.limit {
            current_plan = PlanNode::Limit(LimitNode {
                limit: limit as usize,
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Добавляем OFFSET
        if let Some(offset) = select.offset {
            current_plan = PlanNode::Offset(OffsetNode {
                offset: offset as usize,
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Добавляем проекцию
        current_plan = PlanNode::Projection(ProjectionNode {
            columns: select
                .select_list
                .iter()
                .map(|item| match item {
                    crate::parser::ast::SelectItem::Wildcard => ProjectionColumn {
                        name: "*".to_string(),
                        expression: None,
                        alias: None,
                    },
                    crate::parser::ast::SelectItem::Expression { expr, alias } => {
                        ProjectionColumn {
                            name: format!("{:?}", expr),
                            expression: Some(format!("{:?}", expr)),
                            alias: alias.clone(),
                        }
                    }
                })
                .collect(),
            input: Box::new(current_plan),
            cost: 0.0,
        });

        Ok(current_plan)
    }

    /// Создать план сканирования таблицы
    fn create_table_scan_plan(
        &self,
        table_ref: &crate::parser::ast::TableReference,
    ) -> Result<PlanNode> {
        match table_ref {
            crate::parser::ast::TableReference::Table { name, alias } => {
                Ok(PlanNode::TableScan(TableScanNode {
                    table_name: name.clone(),
                    alias: alias.clone(),
                    columns: vec!["*".to_string()], // TODO: Определить конкретные колонки
                    filter: None,
                    cost: 1.0,            // Базовая стоимость сканирования
                    estimated_rows: 1000, // TODO: Получить из статистики
                }))
            }
            crate::parser::ast::TableReference::Subquery { query, alias } => {
                // Рекурсивно создаем план для подзапроса
                let subquery_plan = self.create_select_plan(query)?;
                Ok(PlanNode::Projection(ProjectionNode {
                    columns: vec![ProjectionColumn {
                        name: alias.clone(),
                        expression: None,
                        alias: Some(alias.clone()),
                    }],
                    input: Box::new(subquery_plan),
                    cost: 0.0,
                }))
            }
        }
    }

    /// Создать план для INSERT запроса
    fn create_insert_plan(&self, insert: &InsertStatement) -> Result<PlanNode> {
        Ok(PlanNode::Insert(InsertNode {
            table_name: insert.table.clone(),
            columns: insert.columns.clone().unwrap_or_default(),
            values: match &insert.values {
                crate::parser::ast::InsertValues::Values(values) => values
                    .iter()
                    .map(|row| row.iter().map(|val| format!("{:?}", val)).collect())
                    .collect(),
                crate::parser::ast::InsertValues::Select(_) => {
                    vec![] // TODO: Обработать INSERT ... SELECT
                }
            },
            cost: 1.0,
        }))
    }

    /// Создать план для UPDATE запроса
    fn create_update_plan(&self, update: &UpdateStatement) -> Result<PlanNode> {
        Ok(PlanNode::Update(UpdateNode {
            table_name: update.table.clone(),
            assignments: update
                .assignments
                .iter()
                .map(|assignment| Assignment {
                    column: assignment.column.clone(),
                    value: format!("{:?}", assignment.value),
                })
                .collect(),
            where_condition: update.where_clause.as_ref().map(|e| format!("{:?}", e)),
            cost: 1.0,
        }))
    }

    /// Создать план для DELETE запроса
    fn create_delete_plan(&self, delete: &DeleteStatement) -> Result<PlanNode> {
        Ok(PlanNode::Delete(DeleteNode {
            table_name: delete.table.clone(),
            where_condition: delete.where_clause.as_ref().map(|e| format!("{:?}", e)),
            cost: 1.0,
        }))
    }

    /// Создать метаданные плана
    fn create_plan_metadata(&self, root: &PlanNode) -> Result<PlanMetadata> {
        let (operator_count, max_depth, table_count, join_count) =
            self.analyze_plan_structure(root, 0);

        Ok(PlanMetadata {
            estimated_cost: self.estimate_plan_cost(root),
            estimated_rows: self.estimate_plan_rows(root),
            created_at: std::time::SystemTime::now(),
            statistics: PlanStatistics {
                operator_count,
                max_depth,
                table_count,
                join_count,
            },
        })
    }

    /// Анализировать структуру плана
    fn analyze_plan_structure(
        &self,
        node: &PlanNode,
        depth: usize,
    ) -> (usize, usize, usize, usize) {
        let mut operator_count = 1;
        let mut max_depth = depth;
        let mut table_count = 0;
        let mut join_count = 0;

        // Подсчитываем специфичные операторы
        match node {
            PlanNode::TableScan(_) => table_count += 1,
            PlanNode::Join(_) => join_count += 1,
            _ => {}
        }

        // Рекурсивно анализируем дочерние узлы
        let child_nodes = self.get_child_nodes(node);
        for child in child_nodes {
            let (child_ops, child_depth, child_tables, child_joins) =
                self.analyze_plan_structure(child, depth + 1);
            operator_count += child_ops;
            max_depth = max_depth.max(child_depth);
            table_count += child_tables;
            join_count += child_joins;
        }

        (operator_count, max_depth, table_count, join_count)
    }

    /// Получить дочерние узлы плана
    fn get_child_nodes<'a>(&self, node: &'a PlanNode) -> Vec<&'a PlanNode> {
        match node {
            PlanNode::Filter(node) => vec![&node.input],
            PlanNode::Projection(node) => vec![&node.input],
            PlanNode::Join(node) => vec![&node.left, &node.right],
            PlanNode::GroupBy(node) => vec![&node.input],
            PlanNode::Sort(node) => vec![&node.input],
            PlanNode::Limit(node) => vec![&node.input],
            PlanNode::Offset(node) => vec![&node.input],
            PlanNode::Aggregate(node) => vec![&node.input],
            _ => vec![],
        }
    }

    /// Оценить стоимость плана
    fn estimate_plan_cost(&self, node: &PlanNode) -> f64 {
        match node {
            PlanNode::TableScan(node) => node.cost,
            PlanNode::IndexScan(node) => node.cost,
            PlanNode::Filter(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Projection(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Join(node) => {
                node.cost
                    + self.estimate_plan_cost(&node.left)
                    + self.estimate_plan_cost(&node.right)
            }
            PlanNode::GroupBy(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Sort(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Limit(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Offset(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Aggregate(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Insert(node) => node.cost,
            PlanNode::Update(node) => node.cost,
            PlanNode::Delete(node) => node.cost,
        }
    }

    /// Оценить количество строк в результате
    fn estimate_plan_rows(&self, node: &PlanNode) -> usize {
        match node {
            PlanNode::TableScan(node) => node.estimated_rows,
            PlanNode::IndexScan(node) => node.estimated_rows,
            PlanNode::Filter(node) => {
                (self.estimate_plan_rows(&node.input) as f64 * node.selectivity) as usize
            }
            PlanNode::Projection(node) => self.estimate_plan_rows(&node.input),
            PlanNode::Join(node) => {
                let left_rows = self.estimate_plan_rows(&node.left);
                let right_rows = self.estimate_plan_rows(&node.right);
                left_rows * right_rows / 1000 // Упрощенная оценка
            }
            PlanNode::GroupBy(node) => self.estimate_plan_rows(&node.input) / 10, // Упрощенная оценка
            PlanNode::Sort(node) => self.estimate_plan_rows(&node.input),
            PlanNode::Limit(node) => node.limit.min(self.estimate_plan_rows(&node.input)),
            PlanNode::Offset(node) => {
                let input_rows = self.estimate_plan_rows(&node.input);
                if node.offset >= input_rows {
                    0
                } else {
                    input_rows - node.offset
                }
            }
            PlanNode::Aggregate(node) => self.estimate_plan_rows(&node.input) / 10,
            PlanNode::Insert(_) => 1,
            PlanNode::Update(_) => 1,
            PlanNode::Delete(_) => 1,
        }
    }

    /// Получить настройки планировщика
    pub fn settings(&self) -> &PlannerSettings {
        &self.settings
    }

    /// Обновить настройки планировщика
    pub fn update_settings(&mut self, settings: PlannerSettings) {
        self.settings = settings;
    }

    /// Очистить кэш планов
    pub fn clear_cache(&mut self) {
        self.plan_cache.clear();
    }

    /// Получить статистику кэша
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            size: self.plan_cache.len(),
            max_size: self.settings.max_cache_size,
        }
    }
}

/// Статистика кэша планов
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Текущий размер кэша
    pub size: usize,
    /// Максимальный размер кэша
    pub max_size: usize,
}
