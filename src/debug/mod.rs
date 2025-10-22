//! Модуль отладки и профилирования для rustdb
//!
//! Этот модуль предоставляет инструменты для:
//! - Детального логирования операций
//! - Трассировки запросов
//! - CPU и Memory профилирования
//! - Анализа производительности
//! - Отладки проблем

pub mod debug_logger;
pub mod performance_analyzer;
pub mod profiler;
pub mod query_tracer;

pub use debug_logger::DebugLogger;
pub use performance_analyzer::PerformanceAnalyzer;
pub use profiler::Profiler;
pub use query_tracer::QueryTracer;

/// Конфигурация отладки
#[derive(Debug, Clone)]
pub struct DebugConfig {
    /// Включить детальное логирование
    pub enable_debug_logging: bool,
    /// Включить трассировку запросов
    pub enable_query_tracing: bool,
    /// Включить CPU профилирование
    pub enable_cpu_profiling: bool,
    /// Включить Memory профилирование
    pub enable_memory_profiling: bool,
    /// Уровень детализации (0-5)
    pub detail_level: u8,
    /// Максимальный размер лог-файла (МБ)
    pub max_log_file_size_mb: u64,
    /// Количество ротируемых лог-файлов
    pub max_log_files: u32,
    /// Интервал сбора метрик (секунды)
    pub metrics_collection_interval: u64,
    /// Включить экспорт в Prometheus
    pub enable_prometheus_export: bool,
    /// Порт для Prometheus метрик
    pub prometheus_port: u16,
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            enable_debug_logging: false,
            enable_query_tracing: false,
            enable_cpu_profiling: false,
            enable_memory_profiling: false,
            detail_level: 2,
            max_log_file_size_mb: 100,
            max_log_files: 5,
            metrics_collection_interval: 10,
            enable_prometheus_export: false,
            prometheus_port: 9090,
        }
    }
}

/// Менеджер отладки
pub struct DebugManager {
    config: DebugConfig,
    debug_logger: Option<DebugLogger>,
    query_tracer: Option<QueryTracer>,
    profiler: Option<Profiler>,
    performance_analyzer: Option<PerformanceAnalyzer>,
}

impl DebugManager {
    /// Создает новый менеджер отладки
    pub fn new(config: DebugConfig) -> Self {
        let mut manager = Self {
            config: config.clone(),
            debug_logger: None,
            query_tracer: None,
            profiler: None,
            performance_analyzer: None,
        };

        // Инициализируем компоненты в зависимости от конфигурации
        if config.enable_debug_logging {
            manager.debug_logger = Some(DebugLogger::new(&config));
        }

        if config.enable_query_tracing {
            manager.query_tracer = Some(QueryTracer::new(&config));
        }

        if config.enable_cpu_profiling || config.enable_memory_profiling {
            manager.profiler = Some(Profiler::new(&config));
        }

        manager.performance_analyzer = Some(PerformanceAnalyzer::new(&config));

        manager
    }

    /// Получает конфигурацию
    pub fn config(&self) -> &DebugConfig {
        &self.config
    }

    /// Получает логгер отладки
    pub fn debug_logger(&self) -> Option<&DebugLogger> {
        self.debug_logger.as_ref()
    }

    /// Получает трассировщик запросов
    pub fn query_tracer(&self) -> Option<&QueryTracer> {
        self.query_tracer.as_ref()
    }

    /// Получает профилировщик
    pub fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }

    /// Получает анализатор производительности
    pub fn performance_analyzer(&self) -> Option<&PerformanceAnalyzer> {
        self.performance_analyzer.as_ref()
    }

    /// Обновляет конфигурацию
    pub fn update_config(&mut self, new_config: DebugConfig) {
        self.config = new_config.clone();

        // Пересоздаем компоненты при необходимости
        if self.config.enable_debug_logging && self.debug_logger.is_none() {
            self.debug_logger = Some(DebugLogger::new(&self.config));
        }

        if self.config.enable_query_tracing && self.query_tracer.is_none() {
            self.query_tracer = Some(QueryTracer::new(&self.config));
        }

        if (self.config.enable_cpu_profiling || self.config.enable_memory_profiling)
            && self.profiler.is_none()
        {
            self.profiler = Some(Profiler::new(&self.config));
        }
    }

    /// Создает отчет о состоянии отладки
    pub fn generate_debug_report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== Отчет о состоянии отладки rustdb ===\n\n");

        report.push_str("Конфигурация:\n");
        report.push_str(&format!(
            "  - Детальное логирование: {}\n",
            self.config.enable_debug_logging
        ));
        report.push_str(&format!(
            "  - Трассировка запросов: {}\n",
            self.config.enable_query_tracing
        ));
        report.push_str(&format!(
            "  - CPU профилирование: {}\n",
            self.config.enable_cpu_profiling
        ));
        report.push_str(&format!(
            "  - Memory профилирование: {}\n",
            self.config.enable_memory_profiling
        ));
        report.push_str(&format!(
            "  - Уровень детализации: {}\n",
            self.config.detail_level
        ));
        report.push_str(&format!(
            "  - Экспорт Prometheus: {}\n",
            self.config.enable_prometheus_export
        ));
        report.push_str("\n");

        report.push_str("Активные компоненты:\n");
        report.push_str(&format!(
            "  - Debug Logger: {}\n",
            self.debug_logger.is_some()
        ));
        report.push_str(&format!(
            "  - Query Tracer: {}\n",
            self.query_tracer.is_some()
        ));
        report.push_str(&format!("  - Profiler: {}\n", self.profiler.is_some()));
        report.push_str(&format!(
            "  - Performance Analyzer: {}\n",
            self.performance_analyzer.is_some()
        ));
        report.push_str("\n");

        // Добавляем отчеты от активных компонентов
        if let Some(logger) = &self.debug_logger {
            report.push_str("=== Debug Logger ===\n");
            report.push_str(&logger.generate_status_report());
            report.push_str("\n");
        }

        if let Some(tracer) = &self.query_tracer {
            report.push_str("=== Query Tracer ===\n");
            report.push_str(&tracer.generate_status_report());
            report.push_str("\n");
        }

        if let Some(profiler) = &self.profiler {
            report.push_str("=== Profiler ===\n");
            report.push_str(&profiler.generate_status_report());
            report.push_str("\n");
        }

        if let Some(analyzer) = &self.performance_analyzer {
            report.push_str("=== Performance Analyzer ===\n");
            report.push_str(&analyzer.generate_status_report());
            report.push_str("\n");
        }

        report
    }

    /// Останавливает все компоненты отладки
    pub fn shutdown(&mut self) {
        if let Some(logger) = &mut self.debug_logger {
            logger.shutdown();
        }

        if let Some(tracer) = &mut self.query_tracer {
            tracer.shutdown();
        }

        if let Some(profiler) = &mut self.profiler {
            profiler.shutdown();
        }

        if let Some(analyzer) = &mut self.performance_analyzer {
            analyzer.shutdown();
        }
    }
}

impl Drop for DebugManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
