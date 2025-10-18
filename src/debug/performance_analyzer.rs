//! Анализатор производительности для rustdb
//!
//! Предоставляет инструменты для анализа производительности системы
//! и выявления узких мест

use crate::debug::DebugConfig;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

/// Тип узкого места
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BottleneckType {
    /// CPU узкое место
    Cpu,
    /// Memory узкое место
    Memory,
    /// I/O узкое место
    Io,
    /// Сетевое узкое место
    Network,
    /// Блокировки
    Locking,
    /// Кэш
    Cache,
}

impl std::fmt::Display for BottleneckType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BottleneckType::Cpu => write!(f, "CPU"),
            BottleneckType::Memory => write!(f, "Memory"),
            BottleneckType::Io => write!(f, "I/O"),
            BottleneckType::Network => write!(f, "Network"),
            BottleneckType::Locking => write!(f, "Locking"),
            BottleneckType::Cache => write!(f, "Cache"),
        }
    }
}

/// Уровень серьезности узкого места
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeverityLevel {
    /// Информация
    Info,
    /// Предупреждение
    Warning,
    /// Критическое
    Critical,
}

impl std::fmt::Display for SeverityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeverityLevel::Info => write!(f, "INFO"),
            SeverityLevel::Warning => write!(f, "WARNING"),
            SeverityLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Обнаруженное узкое место
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bottleneck {
    /// Тип узкого места
    pub bottleneck_type: BottleneckType,
    /// Уровень серьезности
    pub severity: SeverityLevel,
    /// Описание проблемы
    pub description: String,
    /// Рекомендации по устранению
    pub recommendations: Vec<String>,
    /// Временная метка обнаружения
    pub detected_at: u64,
    /// Компонент системы
    pub component: String,
    /// Дополнительные метрики
    pub metrics: HashMap<String, f64>,
    /// Влияние на производительность (%)
    pub performance_impact: f64,
}

/// Метрика производительности
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetric {
    /// Название метрики
    pub name: String,
    /// Значение
    pub value: f64,
    /// Единица измерения
    pub unit: String,
    /// Временная метка
    pub timestamp: u64,
    /// Компонент
    pub component: String,
    /// Пороговые значения
    pub thresholds: Thresholds,
}

/// Пороговые значения для метрик
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thresholds {
    /// Предупреждение
    pub warning: f64,
    /// Критическое
    pub critical: f64,
}

/// Анализ производительности
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAnalysis {
    /// Временная метка анализа
    pub timestamp: u64,
    /// Общая оценка производительности (0-100)
    pub overall_score: f64,
    /// Обнаруженные узкие места
    pub bottlenecks: Vec<Bottleneck>,
    /// Метрики производительности
    pub metrics: Vec<PerformanceMetric>,
    /// Рекомендации
    pub recommendations: Vec<String>,
    /// Тренды
    pub trends: HashMap<String, String>,
}

/// Статистика анализа
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnalysisStats {
    /// Общее количество анализов
    pub total_analyses: u64,
    /// Количество обнаруженных узких мест
    pub total_bottlenecks: u64,
    /// Количество критических проблем
    pub critical_issues: u64,
    /// Средняя оценка производительности
    pub avg_performance_score: f64,
    /// Время последнего анализа
    pub last_analysis_time: u64,
}

/// Анализатор производительности
pub struct PerformanceAnalyzer {
    config: DebugConfig,
    analyses: Arc<RwLock<Vec<PerformanceAnalysis>>>,
    stats: Arc<RwLock<AnalysisStats>>,
    background_handle: Option<JoinHandle<()>>,
    metrics_history: Arc<RwLock<Vec<PerformanceMetric>>>,
}

impl PerformanceAnalyzer {
    /// Создает новый анализатор производительности
    pub fn new(config: &DebugConfig) -> Self {
        let mut analyzer = Self {
            config: config.clone(),
            analyses: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(AnalysisStats::default())),
            background_handle: None,
            metrics_history: Arc::new(RwLock::new(Vec::new())),
        };

        // Запускаем фоновую задачу анализа
        analyzer.start_background_analysis();

        analyzer
    }

    /// Запускает фоновую задачу анализа
    fn start_background_analysis(&mut self) {
        let analyses = self.analyses.clone();
        let stats = self.stats.clone();
        let metrics_history = self.metrics_history.clone();
        let config = self.config.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(config.metrics_collection_interval));
            
            loop {
                interval.tick().await;
                
                // Собираем метрики
                let metrics = Self::collect_performance_metrics();
                
                // Добавляем в историю
                {
                    let mut history = metrics_history.write().unwrap();
                    history.extend(metrics.clone());
                    
                    // Ограничиваем размер истории
                    let len = history.len();
                    if len > 10000 {
                        history.drain(0..len - 10000);
                    }
                }
                
                // Выполняем анализ
                let analysis = Self::perform_analysis(&metrics);
                
                // Сохраняем анализ
                {
                    let mut analyses = analyses.write().unwrap();
                    analyses.push(analysis.clone());
                    
                    // Ограничиваем количество анализов
                    let len = analyses.len();
                    if len > 1000 {
                        analyses.drain(0..len - 1000);
                    }
                }
                
                // Обновляем статистику
                Self::update_analysis_stats(&stats, &analysis);
            }
        }));
    }

    /// Собирает метрики производительности
    fn collect_performance_metrics() -> Vec<PerformanceMetric> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        vec![
            PerformanceMetric {
                name: "cpu_usage".to_string(),
                value: Self::get_cpu_usage(),
                unit: "%".to_string(),
                timestamp,
                component: "System".to_string(),
                thresholds: Thresholds { warning: 70.0, critical: 90.0 },
            },
            PerformanceMetric {
                name: "memory_usage".to_string(),
                value: Self::get_memory_usage(),
                unit: "%".to_string(),
                timestamp,
                component: "System".to_string(),
                thresholds: Thresholds { warning: 80.0, critical: 95.0 },
            },
            PerformanceMetric {
                name: "disk_io".to_string(),
                value: Self::get_disk_io(),
                unit: "MB/s".to_string(),
                timestamp,
                component: "Storage".to_string(),
                thresholds: Thresholds { warning: 100.0, critical: 200.0 },
            },
            PerformanceMetric {
                name: "network_io".to_string(),
                value: Self::get_network_io(),
                unit: "MB/s".to_string(),
                timestamp,
                component: "Network".to_string(),
                thresholds: Thresholds { warning: 50.0, critical: 100.0 },
            },
            PerformanceMetric {
                name: "cache_hit_ratio".to_string(),
                value: Self::get_cache_hit_ratio(),
                unit: "%".to_string(),
                timestamp,
                component: "Cache".to_string(),
                thresholds: Thresholds { warning: 80.0, critical: 60.0 },
            },
            PerformanceMetric {
                name: "lock_contention".to_string(),
                value: Self::get_lock_contention(),
                unit: "%".to_string(),
                timestamp,
                component: "Concurrency".to_string(),
                thresholds: Thresholds { warning: 20.0, critical: 40.0 },
            },
        ]
    }

    /// Выполняет анализ производительности
    fn perform_analysis(metrics: &[PerformanceMetric]) -> PerformanceAnalysis {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut bottlenecks = Vec::new();
        let mut recommendations = Vec::new();
        let mut trends = HashMap::new();
        let mut overall_score = 100.0;

        // Анализируем каждую метрику
        for metric in metrics {
            let mut metric_impact = 0.0;
            let mut metric_bottlenecks = Vec::new();

            // Проверяем критические пороги
            if metric.value >= metric.thresholds.critical {
                metric_impact = 30.0;
                metric_bottlenecks.push(Bottleneck {
                    bottleneck_type: Self::get_bottleneck_type(&metric.name),
                    severity: SeverityLevel::Critical,
                    description: format!("{} превышает критический порог: {:.1}{} (порог: {:.1}{})", 
                        metric.name, metric.value, metric.unit, metric.thresholds.critical, metric.unit),
                    recommendations: Self::get_recommendations(&metric.name, SeverityLevel::Critical),
                    detected_at: timestamp,
                    component: metric.component.clone(),
                    metrics: HashMap::from([(metric.name.clone(), metric.value)]),
                    performance_impact: metric_impact,
                });
            }
            // Проверяем предупреждающие пороги
            else if metric.value >= metric.thresholds.warning {
                metric_impact = 15.0;
                metric_bottlenecks.push(Bottleneck {
                    bottleneck_type: Self::get_bottleneck_type(&metric.name),
                    severity: SeverityLevel::Warning,
                    description: format!("{} превышает предупреждающий порог: {:.1}{} (порог: {:.1}{})", 
                        metric.name, metric.value, metric.unit, metric.thresholds.warning, metric.unit),
                    recommendations: Self::get_recommendations(&metric.name, SeverityLevel::Warning),
                    detected_at: timestamp,
                    component: metric.component.clone(),
                    metrics: HashMap::from([(metric.name.clone(), metric.value)]),
                    performance_impact: metric_impact,
                });
            }

            overall_score -= metric_impact;
            bottlenecks.extend(metric_bottlenecks);

            // Анализируем тренды
            let trend = Self::analyze_metric_trend(metric);
            trends.insert(metric.name.clone(), trend);
        }

        // Генерируем общие рекомендации
        if overall_score < 70.0 {
            recommendations.push("Критическая производительность. Требуется немедленное вмешательство.".to_string());
        } else if overall_score < 85.0 {
            recommendations.push("Производительность ниже оптимальной. Рекомендуется оптимизация.".to_string());
        }

        if bottlenecks.iter().any(|b| matches!(b.bottleneck_type, BottleneckType::Cpu)) {
            recommendations.push("Оптимизируйте алгоритмы и рассмотрите масштабирование CPU.".to_string());
        }

        if bottlenecks.iter().any(|b| matches!(b.bottleneck_type, BottleneckType::Memory)) {
            recommendations.push("Проверьте утечки памяти и оптимизируйте использование памяти.".to_string());
        }

        if bottlenecks.iter().any(|b| matches!(b.bottleneck_type, BottleneckType::Io)) {
            recommendations.push("Оптимизируйте I/O операции и рассмотрите использование SSD.".to_string());
        }

        PerformanceAnalysis {
            timestamp,
            overall_score: overall_score.max(0.0),
            bottlenecks,
            metrics: metrics.to_vec(),
            recommendations,
            trends,
        }
    }

    /// Определяет тип узкого места по названию метрики
    fn get_bottleneck_type(metric_name: &str) -> BottleneckType {
        match metric_name {
            "cpu_usage" => BottleneckType::Cpu,
            "memory_usage" => BottleneckType::Memory,
            "disk_io" | "disk_usage" => BottleneckType::Io,
            "network_io" | "network_usage" => BottleneckType::Network,
            "lock_contention" => BottleneckType::Locking,
            "cache_hit_ratio" => BottleneckType::Cache,
            _ => BottleneckType::Cpu,
        }
    }

    /// Получает рекомендации по устранению проблем
    fn get_recommendations(metric_name: &str, severity: SeverityLevel) -> Vec<String> {
        match (metric_name, severity) {
            ("cpu_usage", SeverityLevel::Critical) => vec![
                "Немедленно оптимизируйте алгоритмы".to_string(),
                "Рассмотрите горизонтальное масштабирование".to_string(),
                "Проверьте наличие бесконечных циклов".to_string(),
            ],
            ("cpu_usage", SeverityLevel::Warning) => vec![
                "Оптимизируйте наиболее ресурсоемкие операции".to_string(),
                "Рассмотрите кэширование результатов".to_string(),
            ],
            ("memory_usage", SeverityLevel::Critical) => vec![
                "Проверьте утечки памяти".to_string(),
                "Увеличьте доступную память".to_string(),
                "Оптимизируйте структуры данных".to_string(),
            ],
            ("memory_usage", SeverityLevel::Warning) => vec![
                "Мониторьте использование памяти".to_string(),
                "Рассмотрите пулы объектов".to_string(),
            ],
            ("cache_hit_ratio", _) => vec![
                "Увеличьте размер кэша".to_string(),
                "Оптимизируйте алгоритм вытеснения".to_string(),
                "Проверьте корректность кэширования".to_string(),
            ],
            ("lock_contention", _) => vec![
                "Оптимизируйте гранулярность блокировок".to_string(),
                "Рассмотрите lock-free структуры данных".to_string(),
                "Уменьшите время удержания блокировок".to_string(),
            ],
            _ => vec!["Проверьте конфигурацию системы".to_string()],
        }
    }

    /// Анализирует тренд метрики
    fn analyze_metric_trend(metric: &PerformanceMetric) -> String {
        // В реальной реализации здесь был бы анализ исторических данных
        // Для демонстрации возвращаем статический результат
        if metric.value > metric.thresholds.warning {
            "Растущий".to_string()
        } else {
            "Стабильный".to_string()
        }
    }

    /// Обновляет статистику анализа
    fn update_analysis_stats(stats: &Arc<RwLock<AnalysisStats>>, analysis: &PerformanceAnalysis) {
        let mut stats = stats.write().unwrap();
        stats.total_analyses += 1;
        stats.last_analysis_time = analysis.timestamp;
        stats.total_bottlenecks += analysis.bottlenecks.len() as u64;
        stats.critical_issues += analysis.bottlenecks.iter()
            .filter(|b| matches!(b.severity, SeverityLevel::Critical))
            .count() as u64;
        
        stats.avg_performance_score = (stats.avg_performance_score * (stats.total_analyses - 1) as f64 + analysis.overall_score) / stats.total_analyses as f64;
    }

    /// Получает использование CPU (симуляция)
    fn get_cpu_usage() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Получает использование памяти (симуляция)
    fn get_memory_usage() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "memory").hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Получает I/O диска (симуляция)
    fn get_disk_io() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "disk").hash(&mut hasher);
        (hasher.finish() % 200) as f64
    }

    /// Получает I/O сети (симуляция)
    fn get_network_io() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "network").hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Получает коэффициент попаданий в кэш (симуляция)
    fn get_cache_hit_ratio() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "cache").hash(&mut hasher);
        60.0 + (hasher.finish() % 40) as f64
    }

    /// Получает конфликт блокировок (симуляция)
    fn get_lock_contention() -> f64 {
        // В реальной реализации здесь был бы вызов системных API
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "lock").hash(&mut hasher);
        (hasher.finish() % 50) as f64
    }

    /// Получает последний анализ
    pub fn get_latest_analysis(&self) -> Option<PerformanceAnalysis> {
        let analyses = self.analyses.read().unwrap();
        analyses.last().cloned()
    }

    /// Получает все анализы
    pub fn get_analyses(&self, limit: usize) -> Vec<PerformanceAnalysis> {
        let analyses = self.analyses.read().unwrap();
        let start = analyses.len().saturating_sub(limit);
        analyses[start..].to_vec()
    }

    /// Получает статистику анализа
    pub fn get_stats(&self) -> AnalysisStats {
        self.stats.read().unwrap().clone()
    }

    /// Создает отчет о производительности
    pub fn generate_performance_report(&self) -> String {
        let stats = self.get_stats();
        let latest_analysis = self.get_latest_analysis();
        
        let mut report = String::new();
        
        report.push_str("=== Отчет анализа производительности ===\n\n");
        
        // Общая статистика
        report.push_str("Общая статистика:\n");
        report.push_str(&format!("  Всего анализов: {}\n", stats.total_analyses));
        report.push_str(&format!("  Обнаружено узких мест: {}\n", stats.total_bottlenecks));
        report.push_str(&format!("  Критических проблем: {}\n", stats.critical_issues));
        report.push_str(&format!("  Средняя оценка производительности: {:.1}/100\n", stats.avg_performance_score));
        report.push_str("\n");

        // Последний анализ
        if let Some(analysis) = latest_analysis {
            report.push_str("Последний анализ:\n");
            report.push_str(&format!("  Общая оценка: {:.1}/100\n", analysis.overall_score));
            report.push_str(&format!("  Обнаружено проблем: {}\n", analysis.bottlenecks.len()));
            report.push_str("\n");

            // Узкие места
            if !analysis.bottlenecks.is_empty() {
                report.push_str("Обнаруженные узкие места:\n");
                for (i, bottleneck) in analysis.bottlenecks.iter().enumerate() {
                    report.push_str(&format!("  {}. [{}] {}: {}\n", 
                        i + 1, 
                        bottleneck.severity, 
                        bottleneck.bottleneck_type, 
                        bottleneck.description
                    ));
                    
                    if !bottleneck.recommendations.is_empty() {
                        report.push_str("     Рекомендации:\n");
                        for rec in &bottleneck.recommendations {
                            report.push_str(&format!("       - {}\n", rec));
                        }
                    }
                }
                report.push_str("\n");
            }

            // Общие рекомендации
            if !analysis.recommendations.is_empty() {
                report.push_str("Общие рекомендации:\n");
                for (i, rec) in analysis.recommendations.iter().enumerate() {
                    report.push_str(&format!("  {}. {}\n", i + 1, rec));
                }
                report.push_str("\n");
            }

            // Тренды
            if !analysis.trends.is_empty() {
                report.push_str("Тренды метрик:\n");
                for (metric, trend) in &analysis.trends {
                    report.push_str(&format!("  {}: {}\n", metric, trend));
                }
                report.push_str("\n");
            }
        }

        // Рекомендации по улучшению
        report.push_str("Рекомендации по улучшению:\n");
        if stats.avg_performance_score < 70.0 {
            report.push_str("  🔴 Критическая производительность. Требуется немедленное вмешательство.\n");
        } else if stats.avg_performance_score < 85.0 {
            report.push_str("  🟡 Производительность ниже оптимальной. Рекомендуется оптимизация.\n");
        } else {
            report.push_str("  🟢 Производительность в пределах нормы.\n");
        }

        if stats.critical_issues > 0 {
            report.push_str(&format!("  ⚠️  Обнаружено {} критических проблем.\n", stats.critical_issues));
        }

        report
    }

    /// Создает отчет о состоянии анализатора
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let analyses_count = self.analyses.read().unwrap().len();
        let metrics_count = self.metrics_history.read().unwrap().len();
        
        let mut report = String::new();
        
        report.push_str(&format!("Анализов в памяти: {}\n", analyses_count));
        report.push_str(&format!("Метрик в истории: {}\n", metrics_count));
        report.push_str(&format!("Всего анализов: {}\n", stats.total_analyses));
        report.push_str(&format!("Обнаружено узких мест: {}\n", stats.total_bottlenecks));
        report.push_str(&format!("Критических проблем: {}\n", stats.critical_issues));
        report.push_str(&format!("Средняя оценка: {:.1}/100\n", stats.avg_performance_score));
        report.push_str(&format!("Интервал сбора метрик: {} сек\n", self.config.metrics_collection_interval));
        
        report
    }

    /// Останавливает анализатор
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for PerformanceAnalyzer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_analyzer() {
        let config = DebugConfig {
            metrics_collection_interval: 1,
            ..Default::default()
        };

        let analyzer = PerformanceAnalyzer::new(&config);

        // Ждем немного, чтобы накопились анализы
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Проверяем статистику
        let stats = analyzer.get_stats();
        assert!(stats.total_analyses > 0);

        // Проверяем последний анализ
        let latest_analysis = analyzer.get_latest_analysis();
        assert!(latest_analysis.is_some());

        if let Some(analysis) = latest_analysis {
            assert!(analysis.overall_score >= 0.0 && analysis.overall_score <= 100.0);
            assert!(!analysis.metrics.is_empty());
        }

        // Проверяем отчет
        let report = analyzer.generate_performance_report();
        assert!(report.contains("Отчет анализа производительности"));
        assert!(report.contains("Общая статистика"));
        assert!(report.contains("Рекомендации по улучшению"));
    }

    #[test]
    fn test_bottleneck_detection() {
        let metrics = vec![
            PerformanceMetric {
                name: "cpu_usage".to_string(),
                value: 95.0, // Критическое значение
                unit: "%".to_string(),
                timestamp: 1000,
                component: "System".to_string(),
                thresholds: Thresholds { warning: 70.0, critical: 90.0 },
            },
            PerformanceMetric {
                name: "memory_usage".to_string(),
                value: 75.0, // Предупреждающее значение
                unit: "%".to_string(),
                timestamp: 1000,
                component: "System".to_string(),
                thresholds: Thresholds { warning: 80.0, critical: 95.0 },
            },
        ];

        let analysis = PerformanceAnalyzer::perform_analysis(&metrics);

        assert!(analysis.overall_score < 100.0);
        assert!(!analysis.bottlenecks.is_empty());
        
        let critical_bottlenecks: Vec<_> = analysis.bottlenecks.iter()
            .filter(|b| matches!(b.severity, SeverityLevel::Critical))
            .collect();
        
        assert!(!critical_bottlenecks.is_empty());
        assert!(critical_bottlenecks.iter().any(|b| matches!(b.bottleneck_type, BottleneckType::Cpu)));
    }

    #[test]
    fn test_recommendations() {
        let cpu_recommendations = PerformanceAnalyzer::get_recommendations("cpu_usage", SeverityLevel::Critical);
        assert!(!cpu_recommendations.is_empty());
        assert!(cpu_recommendations.iter().any(|r| r.contains("оптимизируйте")));

    let memory_recommendations = PerformanceAnalyzer::get_recommendations("memory_usage", SeverityLevel::Warning);
    assert!(!memory_recommendations.is_empty());
    // Проверяем, что рекомендации содержат ключевые слова
    assert!(memory_recommendations.iter().any(|r| r.contains("память") || r.contains("объектов")));
    }
}
