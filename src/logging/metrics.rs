//! Система метрик и мониторинга логирования для RustBD
//!
//! Этот модуль собирает и предоставляет метрики производительности
//! системы логирования:
//! - Счетчики операций и производительности
//! - Временные метрики и латентность
//! - Мониторинг ресурсов и пропускной способности
//! - Экспорт метрик для внешних систем мониторинга

use crate::logging::log_record::LogRecordType;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

/// Счетчик операций
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperationCounter {
    /// Общее количество операций
    pub total: u64,
    /// Успешные операции
    pub success: u64,
    /// Неудачные операции
    pub failed: u64,
    /// Количество операций в секунду (скользящее среднее)
    pub rate_per_second: f64,
    /// Время последнего обновления
    pub last_updated: u64,
}

impl OperationCounter {
    /// Увеличивает счетчик успешных операций
    pub fn increment_success(&mut self) {
        self.total += 1;
        self.success += 1;
        self.update_timestamp();
    }

    /// Увеличивает счетчик неудачных операций
    pub fn increment_failed(&mut self) {
        self.total += 1;
        self.failed += 1;
        self.update_timestamp();
    }

    /// Обновляет временную метку
    fn update_timestamp(&mut self) {
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Возвращает коэффициент успешности
    pub fn success_rate(&self) -> f64 {
        if self.total > 0 {
            self.success as f64 / self.total as f64
        } else {
            0.0
        }
    }
}

/// Временная метрика
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimingMetric {
    /// Общее время (микросекунды)
    pub total_time_us: u64,
    /// Количество измерений
    pub count: u64,
    /// Минимальное время
    pub min_time_us: u64,
    /// Максимальное время
    pub max_time_us: u64,
    /// Среднее время
    pub avg_time_us: u64,
    /// 95-й процентиль
    pub p95_time_us: u64,
    /// 99-й процентиль
    pub p99_time_us: u64,
    /// История измерений (для вычисления процентилей)
    samples: Vec<u64>,
}

impl TimingMetric {
    /// Добавляет новое измерение времени
    pub fn record_time(&mut self, duration: Duration) {
        let time_us = duration.as_micros() as u64;
        
        self.total_time_us += time_us;
        self.count += 1;
        
        if self.count == 1 {
            self.min_time_us = time_us;
            self.max_time_us = time_us;
        } else {
            self.min_time_us = self.min_time_us.min(time_us);
            self.max_time_us = self.max_time_us.max(time_us);
        }
        
        self.avg_time_us = self.total_time_us / self.count;
        
        // Сохраняем образец для процентилей (ограничиваем размер)
        self.samples.push(time_us);
        if self.samples.len() > 10000 {
            self.samples.remove(0);
        }
        
        self.update_percentiles();
    }

    /// Обновляет процентили
    fn update_percentiles(&mut self) {
        if self.samples.is_empty() {
            return;
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort_unstable();

        let len = sorted_samples.len();
        
        if len > 0 {
            let p95_index = ((len as f64) * 0.95) as usize;
            let p99_index = ((len as f64) * 0.99) as usize;
            
            self.p95_time_us = sorted_samples[p95_index.min(len - 1)];
            self.p99_time_us = sorted_samples[p99_index.min(len - 1)];
        }
    }

    /// Возвращает среднее время в миллисекундах
    pub fn avg_time_ms(&self) -> f64 {
        self.avg_time_us as f64 / 1000.0
    }

    /// Возвращает пропускную способность (операций в секунду)
    pub fn throughput_per_second(&self) -> f64 {
        if self.avg_time_us > 0 {
            1_000_000.0 / self.avg_time_us as f64
        } else {
            0.0
        }
    }
}

/// Метрика размера
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SizeMetric {
    /// Общий размер (байты)
    pub total_bytes: u64,
    /// Количество элементов
    pub count: u64,
    /// Минимальный размер
    pub min_bytes: u64,
    /// Максимальный размер
    pub max_bytes: u64,
    /// Средний размер
    pub avg_bytes: u64,
}

impl SizeMetric {
    /// Добавляет новое измерение размера
    pub fn record_size(&mut self, size_bytes: u64) {
        self.total_bytes += size_bytes;
        self.count += 1;
        
        if self.count == 1 {
            self.min_bytes = size_bytes;
            self.max_bytes = size_bytes;
        } else {
            self.min_bytes = self.min_bytes.min(size_bytes);
            self.max_bytes = self.max_bytes.max(size_bytes);
        }
        
        self.avg_bytes = self.total_bytes / self.count;
    }

    /// Возвращает общий размер в МБ
    pub fn total_mb(&self) -> f64 {
        self.total_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Возвращает средний размер в КБ
    pub fn avg_kb(&self) -> f64 {
        self.avg_bytes as f64 / 1024.0
    }
}

/// Полная статистика логирования
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoggingMetrics {
    /// Время начала сбора метрик
    pub start_time: u64,
    /// Время последнего обновления
    pub last_updated: u64,
    /// Время работы системы (секунды)
    pub uptime_seconds: u64,
    
    // Счетчики операций по типам
    /// Операции с транзакциями
    pub transaction_operations: HashMap<String, OperationCounter>,
    /// Операции с данными
    pub data_operations: HashMap<String, OperationCounter>,
    /// Системные операции
    pub system_operations: HashMap<String, OperationCounter>,
    
    // Временные метрики
    /// Время записи логов
    pub write_timing: TimingMetric,
    /// Время синхронизации
    pub sync_timing: TimingMetric,
    /// Время создания контрольных точек
    pub checkpoint_timing: TimingMetric,
    /// Время восстановления
    pub recovery_timing: TimingMetric,
    
    // Метрики размера
    /// Размеры лог-записей
    pub log_record_size: SizeMetric,
    /// Размеры лог-файлов
    pub log_file_size: SizeMetric,
    
    // Специальные метрики
    /// Использование буферов (%)
    pub buffer_utilization: f64,
    /// Коэффициент попаданий в кэш
    pub cache_hit_ratio: f64,
    /// Пропускная способность (записей/сек)
    pub throughput_records_per_sec: f64,
    /// Пропускная способность (байт/сек)
    pub throughput_bytes_per_sec: f64,
    /// Использование диска (%)
    pub disk_utilization: f64,
    /// Фрагментация логов (%)
    pub log_fragmentation: f64,
}

impl LoggingMetrics {
    /// Создает новую коллекцию метрик
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            start_time: now,
            last_updated: now,
            uptime_seconds: 0,
            transaction_operations: HashMap::new(),
            data_operations: HashMap::new(),
            system_operations: HashMap::new(),
            write_timing: TimingMetric::default(),
            sync_timing: TimingMetric::default(),
            checkpoint_timing: TimingMetric::default(),
            recovery_timing: TimingMetric::default(),
            log_record_size: SizeMetric::default(),
            log_file_size: SizeMetric::default(),
            buffer_utilization: 0.0,
            cache_hit_ratio: 0.0,
            throughput_records_per_sec: 0.0,
            throughput_bytes_per_sec: 0.0,
            disk_utilization: 0.0,
            log_fragmentation: 0.0,
        }
    }

    /// Обновляет время последнего обновления и время работы
    pub fn update_timestamp(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        self.last_updated = now;
        self.uptime_seconds = now.saturating_sub(self.start_time);
    }

    /// Записывает операцию с лог-записью
    pub fn record_log_operation(&mut self, record_type: LogRecordType, success: bool, duration: Duration, size: u64) {
        self.update_timestamp();
        
        // Обновляем счетчики по типам операций
        let operation_name = match record_type {
            LogRecordType::TransactionBegin => "transaction_begin",
            LogRecordType::TransactionCommit => "transaction_commit",
            LogRecordType::TransactionAbort => "transaction_abort",
            LogRecordType::DataInsert => "data_insert",
            LogRecordType::DataUpdate => "data_update", 
            LogRecordType::DataDelete => "data_delete",
            LogRecordType::Checkpoint => "checkpoint",
            LogRecordType::CheckpointEnd => "checkpoint_end",
            LogRecordType::Compaction => "compaction",
            LogRecordType::FileCreate => "file_create",
            LogRecordType::FileDelete => "file_delete",
            LogRecordType::FileExtend => "file_extend",
            LogRecordType::MetadataUpdate => "metadata_update",
        };

        let counter = match record_type {
            LogRecordType::TransactionBegin | LogRecordType::TransactionCommit | LogRecordType::TransactionAbort => {
                self.transaction_operations.entry(operation_name.to_string()).or_default()
            }
            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                self.data_operations.entry(operation_name.to_string()).or_default()
            }
            _ => {
                self.system_operations.entry(operation_name.to_string()).or_default()
            }
        };

        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }

        // Обновляем временные метрики
        self.write_timing.record_time(duration);
        
        // Обновляем метрики размера
        self.log_record_size.record_size(size);
        
        // Пересчитываем пропускную способность
        self.recalculate_throughput();
    }

    /// Записывает операцию синхронизации
    pub fn record_sync_operation(&mut self, duration: Duration, success: bool) {
        self.update_timestamp();
        
        let counter = self.system_operations.entry("sync".to_string()).or_default();
        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }
        
        self.sync_timing.record_time(duration);
    }

    /// Записывает операцию создания контрольной точки
    pub fn record_checkpoint_operation(&mut self, duration: Duration, success: bool) {
        self.update_timestamp();
        
        let counter = self.system_operations.entry("checkpoint".to_string()).or_default();
        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }
        
        self.checkpoint_timing.record_time(duration);
    }

    /// Записывает операцию восстановления
    pub fn record_recovery_operation(&mut self, duration: Duration, success: bool) {
        self.update_timestamp();
        
        let counter = self.system_operations.entry("recovery".to_string()).or_default();
        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }
        
        self.recovery_timing.record_time(duration);
    }

    /// Обновляет использование буферов
    pub fn update_buffer_utilization(&mut self, utilization: f64) {
        self.buffer_utilization = utilization.clamp(0.0, 100.0);
        self.update_timestamp();
    }

    /// Обновляет коэффициент попаданий в кэш
    pub fn update_cache_hit_ratio(&mut self, hit_ratio: f64) {
        self.cache_hit_ratio = hit_ratio.clamp(0.0, 1.0);
        self.update_timestamp();
    }

    /// Обновляет использование диска
    pub fn update_disk_utilization(&mut self, utilization: f64) {
        self.disk_utilization = utilization.clamp(0.0, 100.0);
        self.update_timestamp();
    }

    /// Обновляет фрагментацию логов
    pub fn update_log_fragmentation(&mut self, fragmentation: f64) {
        self.log_fragmentation = fragmentation.clamp(0.0, 100.0);
        self.update_timestamp();
    }

    /// Пересчитывает пропускную способность
    fn recalculate_throughput(&mut self) {
        if self.uptime_seconds > 0 {
            self.throughput_records_per_sec = self.log_record_size.count as f64 / self.uptime_seconds as f64;
            self.throughput_bytes_per_sec = self.log_record_size.total_bytes as f64 / self.uptime_seconds as f64;
        }
    }

    /// Возвращает общее количество операций
    pub fn total_operations(&self) -> u64 {
        let tx_ops: u64 = self.transaction_operations.values().map(|c| c.total).sum();
        let data_ops: u64 = self.data_operations.values().map(|c| c.total).sum();
        let sys_ops: u64 = self.system_operations.values().map(|c| c.total).sum();
        tx_ops + data_ops + sys_ops
    }

    /// Возвращает общий коэффициент успешности
    pub fn overall_success_rate(&self) -> f64 {
        let total_success: u64 = self.transaction_operations.values().map(|c| c.success).sum::<u64>()
            + self.data_operations.values().map(|c| c.success).sum::<u64>()
            + self.system_operations.values().map(|c| c.success).sum::<u64>();
        
        let total_ops = self.total_operations();
        
        if total_ops > 0 {
            total_success as f64 / total_ops as f64
        } else {
            0.0
        }
    }

    /// Возвращает топ операций по количеству
    pub fn top_operations_by_count(&self, limit: usize) -> Vec<(String, u64)> {
        let mut all_operations = Vec::new();
        
        for (name, counter) in &self.transaction_operations {
            all_operations.push((format!("tx_{}", name), counter.total));
        }
        
        for (name, counter) in &self.data_operations {
            all_operations.push((format!("data_{}", name), counter.total));
        }
        
        for (name, counter) in &self.system_operations {
            all_operations.push((format!("sys_{}", name), counter.total));
        }
        
        all_operations.sort_by(|a, b| b.1.cmp(&a.1));
        all_operations.into_iter().take(limit).collect()
    }

    /// Экспортирует метрики в формате Prometheus
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        
        // Базовые метрики
        output.push_str(&format!("# HELP rustbd_logging_uptime_seconds Uptime of the logging system\n"));
        output.push_str(&format!("# TYPE rustbd_logging_uptime_seconds counter\n"));
        output.push_str(&format!("rustbd_logging_uptime_seconds {}\n", self.uptime_seconds));
        
        output.push_str(&format!("# HELP rustbd_logging_operations_total Total number of logging operations\n"));
        output.push_str(&format!("# TYPE rustbd_logging_operations_total counter\n"));
        output.push_str(&format!("rustbd_logging_operations_total {}\n", self.total_operations()));
        
        // Временные метрики
        output.push_str(&format!("# HELP rustbd_logging_write_duration_microseconds Write operation duration\n"));
        output.push_str(&format!("# TYPE rustbd_logging_write_duration_microseconds histogram\n"));
        output.push_str(&format!("rustbd_logging_write_duration_microseconds_avg {}\n", self.write_timing.avg_time_us));
        output.push_str(&format!("rustbd_logging_write_duration_microseconds_p95 {}\n", self.write_timing.p95_time_us));
        output.push_str(&format!("rustbd_logging_write_duration_microseconds_p99 {}\n", self.write_timing.p99_time_us));
        
        // Метрики ресурсов
        output.push_str(&format!("# HELP rustbd_logging_buffer_utilization_percent Buffer utilization percentage\n"));
        output.push_str(&format!("# TYPE rustbd_logging_buffer_utilization_percent gauge\n"));
        output.push_str(&format!("rustbd_logging_buffer_utilization_percent {}\n", self.buffer_utilization));
        
        output.push_str(&format!("# HELP rustbd_logging_cache_hit_ratio Cache hit ratio\n"));
        output.push_str(&format!("# TYPE rustbd_logging_cache_hit_ratio gauge\n"));
        output.push_str(&format!("rustbd_logging_cache_hit_ratio {}\n", self.cache_hit_ratio));
        
        // Пропускная способность
        output.push_str(&format!("# HELP rustbd_logging_throughput_records_per_second Records processed per second\n"));
        output.push_str(&format!("# TYPE rustbd_logging_throughput_records_per_second gauge\n"));
        output.push_str(&format!("rustbd_logging_throughput_records_per_second {}\n", self.throughput_records_per_sec));
        
        output
    }

    /// Экспортирует метрики в формате JSON
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Создает отчет о производительности
    pub fn generate_performance_report(&self) -> String {
        let mut report = String::new();
        
        report.push_str("=== Отчет о производительности системы логирования RustBD ===\n\n");
        
        // Общая информация
        report.push_str(&format!("Время работы: {} секунд ({:.1} часов)\n", 
                                self.uptime_seconds, self.uptime_seconds as f64 / 3600.0));
        report.push_str(&format!("Всего операций: {}\n", self.total_operations()));
        report.push_str(&format!("Коэффициент успешности: {:.2}%\n", self.overall_success_rate() * 100.0));
        report.push_str("\n");
        
        // Производительность
        report.push_str("=== Производительность ===\n");
        report.push_str(&format!("Пропускная способность: {:.1} записей/сек\n", self.throughput_records_per_sec));
        report.push_str(&format!("Пропускная способность: {:.1} МБ/сек\n", self.throughput_bytes_per_sec / (1024.0 * 1024.0)));
        report.push_str(&format!("Среднее время записи: {:.2} мс\n", self.write_timing.avg_time_ms()));
        report.push_str(&format!("95-й процентиль времени записи: {:.2} мс\n", self.write_timing.p95_time_us as f64 / 1000.0));
        report.push_str("\n");
        
        // Ресурсы
        report.push_str("=== Использование ресурсов ===\n");
        report.push_str(&format!("Использование буферов: {:.1}%\n", self.buffer_utilization));
        report.push_str(&format!("Коэффициент попаданий в кэш: {:.2}%\n", self.cache_hit_ratio * 100.0));
        report.push_str(&format!("Использование диска: {:.1}%\n", self.disk_utilization));
        report.push_str(&format!("Фрагментация логов: {:.1}%\n", self.log_fragmentation));
        report.push_str("\n");
        
        // Топ операций
        report.push_str("=== Топ операций ===\n");
        let top_ops = self.top_operations_by_count(5);
        for (i, (name, count)) in top_ops.iter().enumerate() {
            report.push_str(&format!("{}. {}: {} операций\n", i + 1, name, count));
        }
        report.push_str("\n");
        
        // Размеры
        report.push_str("=== Размеры данных ===\n");
        report.push_str(&format!("Средний размер лог-записи: {:.1} КБ\n", self.log_record_size.avg_kb()));
        report.push_str(&format!("Общий объем логов: {:.1} МБ\n", self.log_record_size.total_mb()));
        report.push_str(&format!("Средний размер лог-файла: {:.1} МБ\n", self.log_file_size.total_mb() / self.log_file_size.count.max(1) as f64));
        
        report
    }
}

/// Менеджер метрик логирования
pub struct LoggingMetricsManager {
    /// Метрики
    metrics: Arc<RwLock<LoggingMetrics>>,
    /// Фоновая задача обновления
    background_handle: Option<JoinHandle<()>>,
}

impl LoggingMetricsManager {
    /// Создает новый менеджер метрик
    pub fn new() -> Self {
        let metrics = Arc::new(RwLock::new(LoggingMetrics::new()));
        
        let mut manager = Self {
            metrics: metrics.clone(),
            background_handle: None,
        };
        
        // Запускаем фоновую задачу обновления метрик
        manager.start_background_updates();
        
        manager
    }

    /// Запускает фоновые обновления метрик
    fn start_background_updates(&mut self) {
        let metrics = self.metrics.clone();
        
        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            
            loop {
                interval.tick().await;
                
                {
                    let mut m = metrics.write().unwrap();
                    m.update_timestamp();
                }
            }
        }));
    }

    /// Записывает операцию с лог-записью
    pub fn record_log_operation(&self, record_type: LogRecordType, success: bool, duration: Duration, size: u64) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.record_log_operation(record_type, success, duration, size);
    }

    /// Записывает операцию синхронизации
    pub fn record_sync_operation(&self, duration: Duration, success: bool) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.record_sync_operation(duration, success);
    }

    /// Записывает операцию создания контрольной точки
    pub fn record_checkpoint_operation(&self, duration: Duration, success: bool) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.record_checkpoint_operation(duration, success);
    }

    /// Обновляет метрики ресурсов
    pub fn update_resource_metrics(&self, buffer_util: f64, cache_hit_ratio: f64, disk_util: f64, fragmentation: f64) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.update_buffer_utilization(buffer_util);
        metrics.update_cache_hit_ratio(cache_hit_ratio);
        metrics.update_disk_utilization(disk_util);
        metrics.update_log_fragmentation(fragmentation);
    }

    /// Возвращает снимок метрик
    pub fn get_metrics(&self) -> LoggingMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Экспортирует метрики в формате Prometheus
    pub fn export_prometheus(&self) -> String {
        let metrics = self.metrics.read().unwrap();
        metrics.export_prometheus()
    }

    /// Экспортирует метрики в формате JSON
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        let metrics = self.metrics.read().unwrap();
        metrics.export_json()
    }

    /// Создает отчет о производительности
    pub fn generate_performance_report(&self) -> String {
        let metrics = self.metrics.read().unwrap();
        metrics.generate_performance_report()
    }

    /// Сбрасывает все метрики
    pub fn reset_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        *metrics = LoggingMetrics::new();
    }

    /// Останавливает менеджер метрик
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for LoggingMetricsManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_counter() {
        let mut counter = OperationCounter::default();
        
        counter.increment_success();
        counter.increment_success();
        counter.increment_failed();
        
        assert_eq!(counter.total, 3);
        assert_eq!(counter.success, 2);
        assert_eq!(counter.failed, 1);
        assert!((counter.success_rate() - 0.6666666666666666).abs() < f64::EPSILON);
    }

    #[test]
    fn test_timing_metric() {
        let mut timing = TimingMetric::default();
        
        timing.record_time(Duration::from_millis(100));
        timing.record_time(Duration::from_millis(200));
        timing.record_time(Duration::from_millis(150));
        
        assert_eq!(timing.count, 3);
        assert_eq!(timing.min_time_us, 100_000);
        assert_eq!(timing.max_time_us, 200_000);
        assert_eq!(timing.avg_time_us, 150_000);
        assert_eq!(timing.avg_time_ms(), 150.0);
    }

    #[test]
    fn test_size_metric() {
        let mut size = SizeMetric::default();
        
        size.record_size(1024);
        size.record_size(2048);
        size.record_size(1536);
        
        assert_eq!(size.count, 3);
        assert_eq!(size.total_bytes, 4608);
        assert_eq!(size.avg_bytes, 1536);
        assert_eq!(size.min_bytes, 1024);
        assert_eq!(size.max_bytes, 2048);
    }

    #[tokio::test]
    async fn test_logging_metrics() {
        let mut metrics = LoggingMetrics::new();
        
        // Записываем несколько операций
        metrics.record_log_operation(
            LogRecordType::TransactionBegin,
            true,
            Duration::from_millis(10),
            256
        );
        
        metrics.record_log_operation(
            LogRecordType::DataInsert,
            true,
            Duration::from_millis(5),
            512
        );
        
        metrics.record_sync_operation(Duration::from_millis(50), true);
        
        assert_eq!(metrics.total_operations(), 3);
        assert_eq!(metrics.overall_success_rate(), 1.0);
        assert_eq!(metrics.log_record_size.count, 2);
        assert_eq!(metrics.log_record_size.total_bytes, 768);
        
        // Проверяем топ операций
        let top_ops = metrics.top_operations_by_count(5);
        assert!(!top_ops.is_empty());
    }

    #[tokio::test]
    async fn test_metrics_manager() {
        let manager = LoggingMetricsManager::new();
        
        manager.record_log_operation(
            LogRecordType::TransactionCommit,
            true,
            Duration::from_millis(15),
            1024
        );
        
        manager.update_resource_metrics(75.0, 0.85, 60.0, 5.0);
        
        let metrics = manager.get_metrics();
        assert!(metrics.total_operations() > 0);
        assert_eq!(metrics.buffer_utilization, 75.0);
        assert_eq!(metrics.cache_hit_ratio, 0.85);
        
        // Тестируем экспорт
        let prometheus_export = manager.export_prometheus();
        assert!(prometheus_export.contains("rustbd_logging"));
        
        let json_export = manager.export_json().unwrap();
        assert!(json_export.contains("uptime_seconds"));
        
        let report = manager.generate_performance_report();
        assert!(report.contains("Отчет о производительности"));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = LoggingMetrics::new();
        let export = metrics.export_prometheus();
        
        assert!(export.contains("# HELP"));
        assert!(export.contains("# TYPE"));
        assert!(export.contains("rustbd_logging_uptime_seconds"));
        assert!(export.contains("rustbd_logging_operations_total"));
    }

    #[test]
    fn test_performance_report() {
        let mut metrics = LoggingMetrics::new();
        
        // Добавляем тестовые данные
        metrics.record_log_operation(
            LogRecordType::DataUpdate,
            true,
            Duration::from_millis(20),
            2048
        );
        
        let report = metrics.generate_performance_report();
        
        assert!(report.contains("Отчет о производительности"));
        assert!(report.contains("Время работы"));
        assert!(report.contains("Всего операций"));
        assert!(report.contains("Производительность"));
        assert!(report.contains("Использование ресурсов"));
    }
}
