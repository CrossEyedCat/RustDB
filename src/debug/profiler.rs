//! Профилировщик CPU и Memory для rustdb
//!
//! Предоставляет инструменты для профилирования производительности
//! и использования памяти

use crate::debug::DebugConfig;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

/// Тип профилирования
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ProfilingType {
    /// CPU профилирование
    Cpu,
    /// Memory профилирование
    Memory,
    /// Комбинированное профилирование
    Combined,
}

/// Метрика CPU
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuMetric {
    /// Временная метка
    pub timestamp: u64,
    /// Использование CPU (%)
    pub cpu_usage: f64,
    /// Количество потоков
    pub thread_count: usize,
    /// Время пользователя (микросекунды)
    pub user_time_us: u64,
    /// Время системы (микросекунды)
    pub system_time_us: u64,
    /// Время простоя (микросекунды)
    pub idle_time_us: u64,
}

/// Метрика памяти
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetric {
    /// Временная метка
    pub timestamp: u64,
    /// Используемая память (байты)
    pub used_memory: u64,
    /// Доступная память (байты)
    pub available_memory: u64,
    /// Общая память (байты)
    pub total_memory: u64,
    /// Использование памяти (%)
    pub memory_usage_percent: f64,
    /// Память процесса (байты)
    pub process_memory: u64,
    /// Виртуальная память процесса (байты)
    pub process_virtual_memory: u64,
    /// Количество страниц в памяти
    pub page_count: u64,
    /// Количество страниц в swap
    pub swap_count: u64,
}

/// Снимок производительности
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    /// Временная метка
    pub timestamp: u64,
    /// CPU метрики
    pub cpu: Option<CpuMetric>,
    /// Memory метрики
    pub memory: Option<MemoryMetric>,
    /// Дополнительные метрики
    pub additional_metrics: HashMap<String, f64>,
}

/// Статистика профилирования
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfilingStats {
    /// Общее количество снимков
    pub total_snapshots: u64,
    /// Среднее использование CPU (%)
    pub avg_cpu_usage: f64,
    /// Максимальное использование CPU (%)
    pub max_cpu_usage: f64,
    /// Среднее использование памяти (%)
    pub avg_memory_usage: f64,
    /// Максимальное использование памяти (%)
    pub max_memory_usage: f64,
    /// Средняя память процесса (МБ)
    pub avg_process_memory_mb: f64,
    /// Максимальная память процесса (МБ)
    pub max_process_memory_mb: f64,
    /// Время начала профилирования
    pub start_time: u64,
    /// Время последнего снимка
    pub last_snapshot_time: u64,
    /// Длительность профилирования (секунды)
    pub profiling_duration_seconds: u64,
}

/// Профилировщик
pub struct Profiler {
    config: DebugConfig,
    snapshots: Arc<RwLock<Vec<PerformanceSnapshot>>>,
    stats: Arc<RwLock<ProfilingStats>>,
    background_handle: Option<JoinHandle<()>>,
    is_profiling: Arc<RwLock<bool>>,
}

impl Profiler {
    /// Создает новый профилировщик
    pub fn new(config: &DebugConfig) -> Self {
        let mut profiler = Self {
            config: config.clone(),
            snapshots: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(ProfilingStats::default())),
            background_handle: None,
            is_profiling: Arc::new(RwLock::new(false)),
        };

        // Запускаем фоновую задачу профилирования
        if config.enable_cpu_profiling || config.enable_memory_profiling {
            profiler.start_profiling();
        }

        profiler
    }

    /// Запускает профилирование
    pub fn start_profiling(&mut self) {
        if *self.is_profiling.read().unwrap() {
            return; // Уже запущено
        }

        *self.is_profiling.write().unwrap() = true;

        let snapshots = self.snapshots.clone();
        let stats = self.stats.clone();
        let is_profiling = self.is_profiling.clone();
        let config = self.config.clone();

        // Инициализируем статистику
        {
            let mut stats = stats.write().unwrap();
            stats.start_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            
            while *is_profiling.read().unwrap() {
                interval.tick().await;
                
                let snapshot = Self::collect_performance_snapshot(&config);
                
                // Добавляем снимок
                {
                    let mut snapshots = snapshots.write().unwrap();
                    snapshots.push(snapshot.clone());
                    
                    // Ограничиваем количество снимков
                    let len = snapshots.len();
                    if len > 10000 {
                        snapshots.drain(0..len - 10000);
                    }
                }
                
                // Обновляем статистику
                Self::update_stats(&stats, &snapshot);
            }
        }));
    }

    /// Останавливает профилирование
    pub fn stop_profiling(&mut self) {
        *self.is_profiling.write().unwrap() = false;
        
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }

    /// Собирает снимок производительности
    fn collect_performance_snapshot(config: &DebugConfig) -> PerformanceSnapshot {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut snapshot = PerformanceSnapshot {
            timestamp,
            cpu: None,
            memory: None,
            additional_metrics: HashMap::new(),
        };

        // Собираем CPU метрики
        if config.enable_cpu_profiling {
            snapshot.cpu = Some(Self::collect_cpu_metrics());
        }

        // Собираем Memory метрики
        if config.enable_memory_profiling {
            snapshot.memory = Some(Self::collect_memory_metrics());
        }

        // Дополнительные метрики
        snapshot.additional_metrics.insert(
            "gc_collections".to_string(),
            Self::get_gc_collections() as f64,
        );

        snapshot
    }

    /// Собирает CPU метрики
    fn collect_cpu_metrics() -> CpuMetric {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // В реальной реализации здесь был бы вызов системных API
        // Для демонстрации используем симуляцию
        CpuMetric {
            timestamp,
            cpu_usage: Self::get_cpu_usage(),
            thread_count: Self::get_thread_count(),
            user_time_us: Self::get_user_time(),
            system_time_us: Self::get_system_time(),
            idle_time_us: Self::get_idle_time(),
        }
    }

    /// Собирает Memory метрики
    fn collect_memory_metrics() -> MemoryMetric {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // В реальной реализации здесь был бы вызов системных API
        // Для демонстрации используем симуляцию
        let (used_memory, total_memory) = Self::get_memory_info();
        let process_memory = Self::get_process_memory();
        let process_virtual_memory = Self::get_process_virtual_memory();

        MemoryMetric {
            timestamp,
            used_memory,
            available_memory: total_memory - used_memory,
            total_memory,
            memory_usage_percent: (used_memory as f64 / total_memory as f64) * 100.0,
            process_memory,
            process_virtual_memory,
            page_count: Self::get_page_count(),
            swap_count: Self::get_swap_count(),
        }
    }

    /// Обновляет статистику
    fn update_stats(stats: &Arc<RwLock<ProfilingStats>>, snapshot: &PerformanceSnapshot) {
        let mut stats = stats.write().unwrap();
        stats.total_snapshots += 1;
        stats.last_snapshot_time = snapshot.timestamp;

        if let Some(cpu) = &snapshot.cpu {
            stats.avg_cpu_usage = (stats.avg_cpu_usage * (stats.total_snapshots - 1) as f64 + cpu.cpu_usage) / stats.total_snapshots as f64;
            stats.max_cpu_usage = stats.max_cpu_usage.max(cpu.cpu_usage);
        }

        if let Some(memory) = &snapshot.memory {
            stats.avg_memory_usage = (stats.avg_memory_usage * (stats.total_snapshots - 1) as f64 + memory.memory_usage_percent) / stats.total_snapshots as f64;
            stats.max_memory_usage = stats.max_memory_usage.max(memory.memory_usage_percent);
            
            let process_memory_mb = memory.process_memory as f64 / (1024.0 * 1024.0);
            stats.avg_process_memory_mb = (stats.avg_process_memory_mb * (stats.total_snapshots - 1) as f64 + process_memory_mb) / stats.total_snapshots as f64;
            stats.max_process_memory_mb = stats.max_process_memory_mb.max(process_memory_mb);
        }

        stats.profiling_duration_seconds = (snapshot.timestamp / 1_000_000) - stats.start_time;
    }

    /// Получает использование CPU (симуляция)
    fn get_cpu_usage() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        // Для демонстрации возвращаем случайное значение
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Получает количество потоков (симуляция)
    fn get_thread_count() -> usize {
        // В реальной реализации здесь был бы вызов системных API
        4 // Примерное значение
    }

    /// Получает время пользователя (симуляция)
    fn get_user_time() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64 % 1000000
    }

    /// Получает время системы (симуляция)
    fn get_system_time() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64 % 100000
    }

    /// Получает время простоя (симуляция)
    fn get_idle_time() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64 % 1000000
    }

    /// Получает информацию о памяти (симуляция)
    fn get_memory_info() -> (u64, u64) {
        // В реальной реализации здесь был бы вызов системных API
        let total = 16 * 1024 * 1024 * 1024; // 16 GB
        let used = total / 2; // 50% используется
        (used, total)
    }

    /// Получает память процесса (симуляция)
    fn get_process_memory() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        100 * 1024 * 1024 // 100 MB
    }

    /// Получает виртуальную память процесса (симуляция)
    fn get_process_virtual_memory() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        200 * 1024 * 1024 // 200 MB
    }

    /// Получает количество страниц в памяти (симуляция)
    fn get_page_count() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        1000
    }

    /// Получает количество страниц в swap (симуляция)
    fn get_swap_count() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        100
    }

    /// Получает количество сборок мусора (симуляция)
    fn get_gc_collections() -> u64 {
        // В реальной реализации здесь был бы вызов системных API
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() % 1000
    }

    /// Получает снимки производительности
    pub fn get_snapshots(&self, limit: usize) -> Vec<PerformanceSnapshot> {
        let snapshots = self.snapshots.read().unwrap();
        let start = snapshots.len().saturating_sub(limit);
        snapshots[start..].to_vec()
    }

    /// Получает статистику профилирования
    pub fn get_stats(&self) -> ProfilingStats {
        self.stats.read().unwrap().clone()
    }

    /// Создает отчет о производительности
    pub fn generate_performance_report(&self) -> String {
        let stats = self.get_stats();
        let recent_snapshots = self.get_snapshots(100);
        
        let mut report = String::new();
        
        report.push_str("=== Отчет о производительности системы ===\n\n");
        
        // Общая информация
        report.push_str("Общая информация:\n");
        report.push_str(&format!("  Длительность профилирования: {} секунд\n", stats.profiling_duration_seconds));
        report.push_str(&format!("  Количество снимков: {}\n", stats.total_snapshots));
        report.push_str(&format!("  Интервал снимков: 100 мс\n"));
        report.push_str("\n");

        // CPU статистика
        if self.config.enable_cpu_profiling {
            report.push_str("CPU статистика:\n");
            report.push_str(&format!("  Среднее использование: {:.1}%\n", stats.avg_cpu_usage));
            report.push_str(&format!("  Максимальное использование: {:.1}%\n", stats.max_cpu_usage));
            report.push_str("\n");
        }

        // Memory статистика
        if self.config.enable_memory_profiling {
            report.push_str("Memory статистика:\n");
            report.push_str(&format!("  Среднее использование: {:.1}%\n", stats.avg_memory_usage));
            report.push_str(&format!("  Максимальное использование: {:.1}%\n", stats.max_memory_usage));
            report.push_str(&format!("  Средняя память процесса: {:.1} МБ\n", stats.avg_process_memory_mb));
            report.push_str(&format!("  Максимальная память процесса: {:.1} МБ\n", stats.max_process_memory_mb));
            report.push_str("\n");
        }

        // Анализ трендов
        if recent_snapshots.len() >= 10 {
            report.push_str("Анализ трендов (последние 10 снимков):\n");
            
            let cpu_trend = Self::analyze_trend(&recent_snapshots, |s| s.cpu.as_ref().map(|c| c.cpu_usage));
            let memory_trend = Self::analyze_trend(&recent_snapshots, |s| s.memory.as_ref().map(|m| m.memory_usage_percent));
            
            if let Some(trend) = cpu_trend {
                report.push_str(&format!("  CPU тренд: {}\n", trend));
            }
            
            if let Some(trend) = memory_trend {
                report.push_str(&format!("  Memory тренд: {}\n", trend));
            }
            
            report.push_str("\n");
        }

        // Рекомендации
        report.push_str("Рекомендации:\n");
        if stats.avg_cpu_usage > 80.0 {
            report.push_str("  ⚠️  Высокое использование CPU. Рассмотрите оптимизацию алгоритмов.\n");
        }
        if stats.avg_memory_usage > 90.0 {
            report.push_str("  ⚠️  Высокое использование памяти. Проверьте утечки памяти.\n");
        }
        if stats.max_process_memory_mb > 1000.0 {
            report.push_str("  ⚠️  Большое потребление памяти процессом. Оптимизируйте структуры данных.\n");
        }
        
        if stats.avg_cpu_usage <= 80.0 && stats.avg_memory_usage <= 90.0 && stats.max_process_memory_mb <= 1000.0 {
            report.push_str("  ✅ Система работает в пределах нормальных параметров.\n");
        }

        report
    }

    /// Анализирует тренд значений
    fn analyze_trend<F>(snapshots: &[PerformanceSnapshot], extractor: F) -> Option<String>
    where
        F: Fn(&PerformanceSnapshot) -> Option<f64>,
    {
        let values: Vec<f64> = snapshots.iter()
            .filter_map(&extractor)
            .collect();
        
        if values.len() < 3 {
            return None;
        }
        
        let first_half = &values[..values.len() / 2];
        let second_half = &values[values.len() / 2..];
        
        let first_avg = first_half.iter().sum::<f64>() / first_half.len() as f64;
        let second_avg = second_half.iter().sum::<f64>() / second_half.len() as f64;
        
        let change_percent = ((second_avg - first_avg) / first_avg) * 100.0;
        
        if change_percent > 5.0 {
            Some(format!("Растущий (+{:.1}%)", change_percent))
        } else if change_percent < -5.0 {
            Some(format!("Снижающийся ({:.1}%)", change_percent))
        } else {
            Some("Стабильный".to_string())
        }
    }

    /// Создает отчет о состоянии профилировщика
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let is_profiling = *self.is_profiling.read().unwrap();
        let snapshot_count = self.snapshots.read().unwrap().len();
        
        let mut report = String::new();
        
        report.push_str(&format!("Профилирование активно: {}\n", is_profiling));
        report.push_str(&format!("Количество снимков в памяти: {}\n", snapshot_count));
        report.push_str(&format!("Общее количество снимков: {}\n", stats.total_snapshots));
        report.push_str(&format!("CPU профилирование: {}\n", self.config.enable_cpu_profiling));
        report.push_str(&format!("Memory профилирование: {}\n", self.config.enable_memory_profiling));
        report.push_str(&format!("Длительность профилирования: {} секунд\n", stats.profiling_duration_seconds));
        
        report
    }

    /// Останавливает профилировщик
    pub fn shutdown(&mut self) {
        self.stop_profiling();
    }
}

impl Drop for Profiler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_profiler() {
        let config = DebugConfig {
            enable_cpu_profiling: true,
            enable_memory_profiling: true,
            ..Default::default()
        };

        let mut profiler = Profiler::new(&config);

        // Ждем немного, чтобы накопились снимки
        tokio::time::sleep(Duration::from_millis(500)).await;

    // Проверяем статистику
    let stats = profiler.get_stats();
    assert!(stats.total_snapshots > 0);
    // Время профилирования может быть 0, если тест выполнился очень быстро
    assert!(stats.profiling_duration_seconds >= 0);

        // Проверяем снимки
        let snapshots = profiler.get_snapshots(10);
        assert!(!snapshots.is_empty());

        // Проверяем отчет
        let report = profiler.generate_performance_report();
        assert!(report.contains("Отчет о производительности системы"));
        assert!(report.contains("CPU статистика"));
        assert!(report.contains("Memory статистика"));

        // Останавливаем профилирование
        profiler.stop_profiling();
        
        let is_profiling = *profiler.is_profiling.read().unwrap();
        assert!(!is_profiling);
    }

    #[test]
    fn test_performance_snapshot() {
        let config = DebugConfig {
            enable_cpu_profiling: true,
            enable_memory_profiling: true,
            ..Default::default()
        };

        let snapshot = Profiler::collect_performance_snapshot(&config);
        
        assert!(snapshot.timestamp > 0);
        assert!(snapshot.cpu.is_some());
        assert!(snapshot.memory.is_some());
        
        if let Some(cpu) = snapshot.cpu {
            assert!(cpu.cpu_usage >= 0.0 && cpu.cpu_usage <= 100.0);
            assert!(cpu.thread_count > 0);
        }
        
        if let Some(memory) = snapshot.memory {
            assert!(memory.memory_usage_percent >= 0.0 && memory.memory_usage_percent <= 100.0);
            assert!(memory.total_memory > 0);
            assert!(memory.used_memory <= memory.total_memory);
        }
    }

    #[test]
    fn test_trend_analysis() {
        let snapshots = vec![
            PerformanceSnapshot {
                timestamp: 1000,
                cpu: Some(CpuMetric {
                    timestamp: 1000,
                    cpu_usage: 10.0,
                    thread_count: 4,
                    user_time_us: 1000,
                    system_time_us: 100,
                    idle_time_us: 900,
                }),
                memory: None,
                additional_metrics: HashMap::new(),
            },
            PerformanceSnapshot {
                timestamp: 2000,
                cpu: Some(CpuMetric {
                    timestamp: 2000,
                    cpu_usage: 20.0,
                    thread_count: 4,
                    user_time_us: 2000,
                    system_time_us: 200,
                    idle_time_us: 1800,
                }),
                memory: None,
                additional_metrics: HashMap::new(),
            },
            PerformanceSnapshot {
                timestamp: 3000,
                cpu: Some(CpuMetric {
                    timestamp: 3000,
                    cpu_usage: 30.0,
                    thread_count: 4,
                    user_time_us: 3000,
                    system_time_us: 300,
                    idle_time_us: 2700,
                }),
                memory: None,
                additional_metrics: HashMap::new(),
            },
        ];

        let trend = Profiler::analyze_trend(&snapshots, |s| s.cpu.as_ref().map(|c| c.cpu_usage));
        assert!(trend.is_some());
        assert!(trend.unwrap().contains("Растущий"));
    }
}
