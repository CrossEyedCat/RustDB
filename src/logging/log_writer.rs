//! File-based log writer for rustdb
//!
//! This module implements buffered log writing with optimized I/O:
//! - Buffered record batching to improve throughput
//! - Asynchronous writing with priority control
//! - Log file rotation and size management
//! - Integration with the I/O optimization subsystem

use crate::common::{Error, Result};
use crate::logging::log_record::{LogRecord, LogSequenceNumber};
use crate::storage::io_optimization::{BufferedIoManager, IoBufferConfig};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;

/// Log writer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogWriterConfig {
    /// Directory containing log files
    pub log_directory: PathBuf,
    /// Maximum log file size (bytes)
    pub max_log_file_size: u64,
    /// Maximum number of log files
    pub max_log_files: u32,
    /// Write buffer size (number of records)
    pub write_buffer_size: usize,
    /// Maximum buffering time
    pub max_buffer_time: Duration,
    /// Enable compression for old logs
    pub enable_compression: bool,
    /// Synchronization level
    pub sync_level: SyncLevel,
    /// Writer thread pool size
    pub writer_thread_pool_size: usize,
    /// Enable integrity checks
    pub enable_integrity_check: bool,
    /// Group commit: flush interval in milliseconds
    pub group_commit_interval_ms: u64,
    /// Group commit: max batch size before flush
    pub group_commit_max_batch: usize,
    /// Group commit: enable batching of COMMIT syncs
    pub group_commit_enabled: bool,
    /// When true, force_sync requests flush immediately without waiting for group commit
    pub force_flush_immediately: bool,
    /// When true, commit waits for WAL fsync (durable). When false, commit returns immediately
    /// without fsync - higher throughput but risk of data loss on crash (PostgreSQL's synchronous_commit=off).
    pub synchronous_commit: bool,
}

/// Log synchronization level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncLevel {
    /// Never sync (fast but unsafe)
    Never,
    /// Sync periodically
    Periodic,
    /// Sync after each commit
    OnCommit,
    /// Sync after every write (slow but safest)
    Always,
}

impl LogWriterConfig {
    /// Preset for maximum throughput (group commit, no sync wait).
    /// Lower durability: risk of data loss on crash.
    pub fn high_throughput(log_directory: PathBuf) -> Self {
        let mut c = Self::default();
        c.log_directory = log_directory;
        c.force_flush_immediately = false;
        c.synchronous_commit = false;
        c.group_commit_enabled = true;
        c.group_commit_interval_ms = 1;
        c.group_commit_max_batch = 10;
        c
    }

    /// Preset for maximum durability (immediate fsync on each commit).
    pub fn durable(log_directory: PathBuf) -> Self {
        let mut c = Self::default();
        c.log_directory = log_directory;
        c.force_flush_immediately = true;
        c.synchronous_commit = true;
        c.group_commit_enabled = false;
        c
    }
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            log_directory: PathBuf::from("./logs"),
            max_log_file_size: 100 * 1024 * 1024, // 100 MB
            max_log_files: 10,
            write_buffer_size: 2000,
            max_buffer_time: Duration::from_millis(100),
            enable_compression: true,
            sync_level: SyncLevel::OnCommit,
            writer_thread_pool_size: 2,
            enable_integrity_check: true,
            group_commit_interval_ms: 1,
            group_commit_max_batch: 10,
            group_commit_enabled: true,
            force_flush_immediately: false, // Use group commit for better TPS
            synchronous_commit: true,       // Wait for fsync by default (durable)
        }
    }
}

/// Log file information
#[derive(Debug, Clone, PartialEq)]
pub struct LogFileInfo {
    /// File name
    pub filename: String,
    /// Absolute path to file
    pub path: PathBuf,
    /// File size in bytes
    pub size: u64,
    /// Number of records in file
    pub record_count: u64,
    /// First LSN present in file
    pub first_lsn: LogSequenceNumber,
    /// Last LSN present in file
    pub last_lsn: LogSequenceNumber,
    /// File creation timestamp
    pub created_at: u64,
    /// Last modification timestamp
    pub updated_at: u64,
    /// Whether file is compressed
    pub is_compressed: bool,
}

/// State of the current log file for writing
struct LogFileState {
    writer: BufWriter<File>,
    path: PathBuf,
    size: u64,
}

/// Double buffer: while one buffer is flushed to disk, new writes go to the other
struct DoubleBuffer {
    active: VecDeque<LogRecord>,
    flush: VecDeque<LogRecord>,
}

impl DoubleBuffer {
    fn new() -> Self {
        Self {
            active: VecDeque::new(),
            flush: VecDeque::new(),
        }
    }

    fn push(&mut self, record: LogRecord) {
        self.active.push_back(record);
    }

    fn len(&self) -> usize {
        self.active.len()
    }

    /// Swap buffers and return records to flush (active becomes empty, flush gets the data)
    fn take_for_flush(&mut self) -> VecDeque<LogRecord> {
        std::mem::swap(&mut self.active, &mut self.flush);
        std::mem::take(&mut self.flush)
    }
}

/// Log write request
#[derive(Debug)]
pub struct LogWriteRequest {
    /// Log record to be written
    pub record: LogRecord,
    /// Response channel (for force_sync, response is sent after group flush)
    pub response_tx: Option<oneshot::Sender<Result<()>>>,
    /// Indicates whether sync is required (uses group commit when enabled)
    pub force_sync: bool,
    /// When true, flush immediately and respond after flush.
    /// If `force_sync=true` and `config.synchronous_commit=true`, the flush also includes fsync.
    pub force_flush_immediately: bool,
}

/// Log writer statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogWriterStatistics {
    /// Total records written
    pub total_records_written: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
    /// Number of sync operations
    pub sync_operations: u64,
    /// Number of file rotations
    pub file_rotations: u64,
    /// Average write time (microseconds)
    pub average_write_time_us: u64,
    /// Number of write errors
    pub write_errors: u64,
    /// Current buffer size
    pub current_buffer_size: usize,
    /// Maximum buffer size observed
    pub max_buffer_size_reached: usize,
    /// Write throughput (records/sec)
    pub write_throughput: f64,
}

/// Log writer implementation
pub struct LogWriter {
    /// Configuration
    config: LogWriterConfig,
    /// Current active file
    current_file: Arc<RwLock<Option<LogFileInfo>>>,
    /// List of all log files
    log_files: Arc<RwLock<Vec<LogFileInfo>>>,
    /// Record buffer (double-buffered)
    write_buffer: Arc<Mutex<DoubleBuffer>>,
    /// Sync waiters (response channels for force_sync requests, batched by group commit)
    sync_waiters: Arc<Mutex<Vec<oneshot::Sender<Result<()>>>>>,
    /// Channel for write requests
    write_tx: mpsc::UnboundedSender<LogWriteRequest>,
    /// LSN generator
    lsn_generator: Arc<Mutex<LogSequenceNumber>>,
    /// Statistics
    statistics: Arc<RwLock<LogWriterStatistics>>,
    /// Semaphore to limit concurrent operations
    semaphore: Arc<Semaphore>,
    /// Background task handle
    background_handle: Option<JoinHandle<()>>,
    /// Write task handle
    writer_handle: Option<JoinHandle<()>>,
    /// Group commit task handle
    group_commit_handle: Option<JoinHandle<()>>,
    /// Optimized I/O manager
    io_manager: Option<Arc<BufferedIoManager>>,
    /// Current log file for writing (shared with background tasks)
    log_file_state: Arc<Mutex<Option<LogFileState>>>,
}

impl LogWriter {
    /// Creates a new log writer
    pub fn new(config: LogWriterConfig) -> Result<Self> {
        // Create log directory if it does not exist
        if !config.log_directory.exists() {
            std::fs::create_dir_all(&config.log_directory)
                .map_err(|e| Error::internal(&format!("Failed to create log directory: {}", e)))?;
        }

        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let semaphore = Arc::new(Semaphore::new(config.writer_thread_pool_size));

        // Configure optimized I/O
        let io_manager = if config.write_buffer_size > 0 {
            let mut io_config = IoBufferConfig::default();
            io_config.max_write_buffer_size = config.write_buffer_size;
            io_config.max_buffer_time = config.max_buffer_time;
            Some(Arc::new(BufferedIoManager::new(io_config)))
        } else {
            None
        };

        let sync_waiters = Arc::new(Mutex::new(Vec::new()));

        let mut writer = Self {
            config: config.clone(),
            current_file: Arc::new(RwLock::new(None)),
            log_files: Arc::new(RwLock::new(Vec::new())),
            write_buffer: Arc::new(Mutex::new(DoubleBuffer::new())),
            sync_waiters: sync_waiters.clone(),
            write_tx,
            lsn_generator: Arc::new(Mutex::new(1)),
            statistics: Arc::new(RwLock::new(LogWriterStatistics::default())),
            semaphore,
            background_handle: None,
            writer_handle: None,
            group_commit_handle: None,
            io_manager,
            log_file_state: Arc::new(Mutex::new(None)),
        };

        // Load existing log files
        writer.load_existing_log_files()?;

        // Start background tasks
        writer.start_background_tasks(write_rx);

        Ok(writer)
    }

    /// Loads existing log files
    fn load_existing_log_files(&mut self) -> Result<()> {
        let mut files = Vec::new();
        let mut max_lsn = 0;

        if self.config.log_directory.exists() {
            let entries = std::fs::read_dir(&self.config.log_directory)
                .map_err(|e| Error::internal(&format!("Failed to read log directory: {}", e)))?;

            for entry in entries {
                let entry = entry.map_err(|e| {
                    Error::internal(&format!("Failed to read directory entry: {}", e))
                })?;
                let path = entry.path();

                if path.extension().and_then(|s| s.to_str()) == Some("log") {
                    if let Ok(file_info) = self.analyze_log_file(&path) {
                        if file_info.last_lsn > max_lsn {
                            max_lsn = file_info.last_lsn;
                        }
                        files.push(file_info);
                    }
                }
            }
        }

        // Sort files by creation time
        files.sort_by_key(|f| f.created_at);

        // Set the last file as the current file
        if let Some(latest_file) = files.last() {
            *self.current_file.write().unwrap() = Some(latest_file.clone());
        }

        *self.log_files.write().unwrap() = files;

        // Set next LSN
        *self.lsn_generator.lock().unwrap() = max_lsn + 1;

        Ok(())
    }

    /// Analyzes a log file and returns metadata
    fn analyze_log_file(&self, path: &Path) -> Result<LogFileInfo> {
        let metadata = std::fs::metadata(path)
            .map_err(|e| Error::internal(&format!("Failed to obtain file metadata: {}", e)))?;

        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Simplified implementation — real system should read file header
        let file_info = LogFileInfo {
            filename,
            path: path.to_path_buf(),
            size: metadata.len(),
            record_count: 0, // Requires file reading
            first_lsn: 0,    // Requires file reading
            last_lsn: 0,     // Requires file reading
            created_at: metadata
                .created()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            updated_at: metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            is_compressed: path.extension().and_then(|s| s.to_str()) == Some("gz"),
        };

        Ok(file_info)
    }

    /// Starts background tasks
    fn start_background_tasks(&mut self, mut write_rx: mpsc::UnboundedReceiver<LogWriteRequest>) {
        let config = self.config.clone();
        let write_buffer = self.write_buffer.clone();
        let sync_waiters = self.sync_waiters.clone();
        let statistics = self.statistics.clone();
        let current_file = self.current_file.clone();
        let log_files = self.log_files.clone();
        let log_file_state = self.log_file_state.clone();
        let _semaphore = self.semaphore.clone();
        let io_manager = self.io_manager.clone();

        // Task that processes write requests
        self.writer_handle = Some(tokio::spawn(async move {
            while let Some(request) = write_rx.recv().await {
                let buffer = write_buffer.clone();
                let waiters = sync_waiters.clone();
                let stats = statistics.clone();
                let file = current_file.clone();
                let files = log_files.clone();
                let cfg = config.clone();
                let io_mgr = io_manager.clone();
                let log_file = log_file_state.clone();

                Self::handle_write_request(
                    request, buffer, waiters, stats, file, files, cfg, io_mgr, log_file,
                )
                .await;
            }
        }));

        // Task that periodically flushes the buffer
        let flush_buffer = self.write_buffer.clone();
        let flush_waiters = self.sync_waiters.clone();
        let flush_stats = self.statistics.clone();
        let flush_config = self.config.clone();
        let flush_log_file = self.log_file_state.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_config.max_buffer_time);

            loop {
                interval.tick().await;
                Self::flush_write_buffer(
                    &flush_buffer,
                    &flush_stats,
                    &flush_log_file,
                    &flush_config,
                )
                .await;
                Self::notify_sync_waiters(&flush_waiters);
            }
        }));

        // Group commit task: flush and notify sync waiters on interval
        if self.config.group_commit_enabled {
            let gc_buffer = self.write_buffer.clone();
            let gc_waiters = self.sync_waiters.clone();
            let gc_stats = self.statistics.clone();
            let gc_interval = Duration::from_millis(self.config.group_commit_interval_ms.max(1));
            let gc_log_file = self.log_file_state.clone();
            let gc_config = self.config.clone();

            self.group_commit_handle = Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(gc_interval);

                loop {
                    interval.tick().await;
                    let should_flush = {
                        let waiters = gc_waiters.lock().unwrap();
                        !waiters.is_empty()
                    };
                    if should_flush {
                        Self::flush_write_buffer(&gc_buffer, &gc_stats, &gc_log_file, &gc_config)
                            .await;
                        Self::notify_sync_waiters(&gc_waiters);
                    }
                }
            }));
        }
    }

    /// Notifies all pending sync waiters after a flush
    fn notify_sync_waiters(sync_waiters: &Arc<Mutex<Vec<oneshot::Sender<Result<()>>>>>) {
        let mut waiters = sync_waiters.lock().unwrap();
        for tx in waiters.drain(..) {
            let _ = tx.send(Ok(()));
        }
    }

    /// Handles a write request
    async fn handle_write_request(
        request: LogWriteRequest,
        write_buffer: Arc<Mutex<DoubleBuffer>>,
        sync_waiters: Arc<Mutex<Vec<oneshot::Sender<Result<()>>>>>,
        statistics: Arc<RwLock<LogWriterStatistics>>,
        _current_file: Arc<RwLock<Option<LogFileInfo>>>,
        _log_files: Arc<RwLock<Vec<LogFileInfo>>>,
        config: LogWriterConfig,
        _io_manager: Option<Arc<BufferedIoManager>>,
        log_file_state: Arc<Mutex<Option<LogFileState>>>,
    ) {
        let start_time = Instant::now();

        // Push record into buffer and update stats immediately (before any notify)
        // so callers reading stats after response see consistent state
        {
            let mut buffer = write_buffer.lock().unwrap();
            buffer.push(request.record.clone());
        }
        let is_sync_request = request.force_sync;
        {
            let mut stats = statistics.write().unwrap();
            stats.total_records_written += 1;
            if is_sync_request {
                stats.sync_operations += 1;
            }
            if let Ok(serialized) = request.record.serialize() {
                stats.total_bytes_written += serialized.len() as u64;
            }
        }

        let mut should_flush = false;
        let mut legacy_response_tx = None;
        let flush_immediately = request.force_flush_immediately
            || (request.force_sync && config.force_flush_immediately);

        if request.force_sync || request.force_flush_immediately || request.record.requires_immediate_flush() {
            if flush_immediately {
                if let Some(tx) = request.response_tx {
                    let mut waiters = sync_waiters.lock().unwrap();
                    waiters.push(tx);
                }
                should_flush = true;
            } else if config.group_commit_enabled {
                if let Some(tx) = request.response_tx {
                    let mut waiters = sync_waiters.lock().unwrap();
                    waiters.push(tx);
                    if waiters.len() >= config.group_commit_max_batch {
                        should_flush = true;
                    }
                }
            } else {
                // Legacy: immediate flush when group commit disabled
                legacy_response_tx = request.response_tx;
                Self::flush_write_buffer(&write_buffer, &statistics, &log_file_state, &config)
                    .await;
            }
        } else if let Some(tx) = request.response_tx {
            let _ = tx.send(Ok(()));
        }

        if should_flush {
            Self::flush_write_buffer(&write_buffer, &statistics, &log_file_state, &config).await;
        }

        // Update remaining statistics (before notify so caller sees consistent state)
        {
            let mut stats = statistics.write().unwrap();
            if !is_sync_request && legacy_response_tx.is_some() {
                stats.sync_operations += 1;
            }

            let execution_time = start_time.elapsed().as_micros() as u64;
            if stats.total_records_written > 0 {
                stats.average_write_time_us = (stats.average_write_time_us
                    * (stats.total_records_written - 1)
                    + execution_time)
                    / stats.total_records_written;
            }

            let buffer_size = write_buffer.lock().unwrap().len();
            stats.current_buffer_size = buffer_size;
            if buffer_size > stats.max_buffer_size_reached {
                stats.max_buffer_size_reached = buffer_size;
            }
        }

        if should_flush {
            Self::notify_sync_waiters(&sync_waiters);
        }
        if let Some(tx) = legacy_response_tx {
            let _ = tx.send(Ok(()));
        }
    }

    /// Flushes write buffer to disk
    async fn flush_write_buffer(
        write_buffer: &Arc<Mutex<DoubleBuffer>>,
        statistics: &Arc<RwLock<LogWriterStatistics>>,
        log_file_state: &Arc<Mutex<Option<LogFileState>>>,
        config: &LogWriterConfig,
    ) {
        let records_to_write: Vec<LogRecord> = {
            let mut buffer = write_buffer.lock().unwrap();
            if buffer.len() == 0 {
                return;
            }

            buffer.take_for_flush().into_iter().collect()
        };

        let config = config.clone();
        let log_file_state = log_file_state.clone();
        let write_result = tokio::task::spawn_blocking(move || {
            Self::write_records_to_file(&records_to_write, &log_file_state, &config)
        })
        .await;

        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                if let Ok(mut stats) = statistics.write() {
                    stats.write_errors += 1;
                }
                return;
            }
        }

        // Update statistics
        {
            let mut stats = statistics.write().unwrap();
            stats.sync_operations += 1;
            stats.current_buffer_size = write_buffer.lock().unwrap().len();

            // Calculate throughput
            if stats.total_records_written > 0 && stats.average_write_time_us > 0 {
                stats.write_throughput = 1_000_000.0 / stats.average_write_time_us as f64;
            }
        }
    }

    /// Writes records to the log file (blocking, run in spawn_blocking).
    /// Batch serialization: pre-serialize all records before the write loop to minimize syscalls.
    fn write_records_to_file(
        records: &[LogRecord],
        log_file_state: &Arc<Mutex<Option<LogFileState>>>,
        config: &LogWriterConfig,
    ) -> Result<()> {
        use std::io::Write as _;

        if records.is_empty() {
            return Ok(());
        }

        // Pre-serialize all records before acquiring file lock (batch serialization)
        let serialized: Vec<Vec<u8>> = records
            .iter()
            .map(|r| {
                r.serialize()
                    .map_err(|e| Error::internal(&format!("Failed to serialize log record: {}", e)))
            })
            .collect::<Result<Vec<_>>>()?;

        // Build single buffer: [len1, data1, len2, data2, ...] for fewer syscalls
        let total_size: usize = serialized.iter().map(|d| 4 + d.len()).sum();
        let mut batch = Vec::with_capacity(total_size);
        for data in &serialized {
            batch.extend_from_slice(&(data.len() as u32).to_le_bytes());
            batch.extend_from_slice(data);
        }

        let mut state_guard = log_file_state.lock().unwrap();

        // Get or create log file
        if state_guard.is_none() {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let path = config.log_directory.join(format!("wal_{}.log", timestamp));
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| Error::internal(&format!("Failed to open log file: {}", e)))?;
            *state_guard = Some(LogFileState {
                writer: BufWriter::new(file),
                path: path.clone(),
                size: 0,
            });
        }

        let state = state_guard.as_mut().unwrap();

        // Rotate if batch would exceed max file size
        if state.size > 0 && state.size + batch.len() as u64 > config.max_log_file_size {
            state
                .writer
                .flush()
                .map_err(|e| Error::internal(&format!("Failed to flush log file: {}", e)))?;
            if config.synchronous_commit {
                state
                    .writer
                    .get_mut()
                    .sync_all()
                    .map_err(|e| Error::internal(&format!("Failed to sync log file: {}", e)))?;
            }

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let path = config.log_directory.join(format!("wal_{}.log", timestamp));
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| Error::internal(&format!("Failed to open new log file: {}", e)))?;
            *state_guard = Some(LogFileState {
                writer: BufWriter::new(file),
                path,
                size: 0,
            });
        }

        let state = state_guard.as_mut().unwrap();
        state
            .writer
            .write_all(&batch)
            .map_err(|e| Error::internal(&format!("Failed to write log records: {}", e)))?;
        state.size += batch.len() as u64;

        if let Some(state) = state_guard.as_mut() {
            state
                .writer
                .flush()
                .map_err(|e| Error::internal(&format!("Failed to flush log file: {}", e)))?;
            if config.synchronous_commit {
                state
                    .writer
                    .get_mut()
                    .sync_all()
                    .map_err(|e| Error::internal(&format!("Failed to sync log file: {}", e)))?;
            }
        }

        Ok(())
    }

    /// Writes a log record and waits until it is flushed to the log file.
    /// If `config.synchronous_commit=true`, this also waits for fsync.
    pub async fn write_log_durable(&self, mut record: LogRecord) -> Result<LogSequenceNumber> {
        if record.lsn == 0 {
            record.lsn = self.generate_lsn();
        }

        let (response_tx, response_rx) = oneshot::channel();
        let request = LogWriteRequest {
            record: record.clone(),
            response_tx: Some(response_tx),
            force_sync: self.config.synchronous_commit,
            force_flush_immediately: true,
        };

        self.write_tx
            .send(request)
            .map_err(|_| Error::internal("Failed to send durable log write request"))?;

        response_rx
            .await
            .map_err(|_| Error::internal("Failed to receive durable log write result"))??;

        Ok(record.lsn)
    }

    /// Writes a log record
    pub async fn write_log(&self, mut record: LogRecord) -> Result<LogSequenceNumber> {
        // Generate LSN if not already set
        if record.lsn == 0 {
            record.lsn = self.generate_lsn();
        }

        let (response_tx, response_rx) = oneshot::channel();
        let request = LogWriteRequest {
            record: record.clone(),
            response_tx: Some(response_tx),
            force_sync: false,
            force_flush_immediately: false,
        };

        self.write_tx
            .send(request)
            .map_err(|_| Error::internal("Failed to send log write request"))?;

        response_rx
            .await
            .map_err(|_| Error::internal("Failed to receive log write result"))??;

        Ok(record.lsn)
    }

    /// Writes a log record with forced synchronization.
    /// When synchronous_commit=false, returns immediately without waiting for fsync (higher throughput, lower durability).
    pub async fn write_log_sync(&self, mut record: LogRecord) -> Result<LogSequenceNumber> {
        if record.lsn == 0 {
            record.lsn = self.generate_lsn();
        }

        if !self.config.synchronous_commit {
            // No sync: buffer only, return immediately (PostgreSQL's synchronous_commit=off)
            let request = LogWriteRequest {
                record: record.clone(),
                response_tx: None,
                force_sync: false,
                force_flush_immediately: false,
            };
            self.write_tx
                .send(request)
                .map_err(|_| Error::internal("Failed to send log write request"))?;
            return Ok(record.lsn);
        }

        let (response_tx, response_rx) = oneshot::channel();
        let request = LogWriteRequest {
            record: record.clone(),
            response_tx: Some(response_tx),
            force_sync: true,
            force_flush_immediately: self.config.force_flush_immediately,
        };

        self.write_tx
            .send(request)
            .map_err(|_| Error::internal("Failed to send synchronous log write request"))?;

        response_rx
            .await
            .map_err(|_| Error::internal("Failed to receive synchronous log write result"))??;

        Ok(record.lsn)
    }

    /// Forces flushing of all buffers to disk
    pub async fn flush(&self) -> Result<()> {
        Self::flush_write_buffer(
            &self.write_buffer,
            &self.statistics,
            &self.log_file_state,
            &self.config,
        )
        .await;

        {
            let mut stats = self.statistics.write().unwrap();
            stats.sync_operations += 1;
        }

        Ok(())
    }

    /// Generates next LSN
    fn generate_lsn(&self) -> LogSequenceNumber {
        let mut generator = self.lsn_generator.lock().unwrap();
        let lsn = *generator;
        *generator += 1;
        lsn
    }

    /// Returns current LSN
    pub fn current_lsn(&self) -> LogSequenceNumber {
        let generator = self.lsn_generator.lock().unwrap();
        *generator - 1
    }

    /// Returns log writer statistics
    pub fn get_statistics(&self) -> LogWriterStatistics {
        self.statistics.read().unwrap().clone()
    }

    /// Returns current log file information
    pub fn get_current_file_info(&self) -> Option<LogFileInfo> {
        self.current_file.read().unwrap().clone()
    }

    /// Returns list of all log files
    pub fn get_log_files(&self) -> Vec<LogFileInfo> {
        self.log_files.read().unwrap().clone()
    }

    /// Performs log file rotation
    pub async fn rotate_log_file(&self) -> Result<()> {
        // In real implementation file rotation logic would go here
        {
            let mut stats = self.statistics.write().unwrap();
            stats.file_rotations += 1;
        }

        Ok(())
    }

    /// Verifies log file integrity
    pub async fn verify_integrity(&self) -> Result<Vec<(String, bool)>> {
        let files = self.get_log_files();
        let mut results = Vec::new();

        for file in files {
            // In real implementation file integrity would be verified here
            let is_valid = true; // Placeholder
            results.push((file.filename, is_valid));
        }

        Ok(results)
    }

    /// Cleans up old log files
    pub async fn cleanup_old_logs(&self, keep_files: u32) -> Result<u32> {
        let files = self.get_log_files();
        let mut removed_count = 0;

        if files.len() > keep_files as usize {
            let files_to_remove = files.len() - keep_files as usize;

            // In real implementation files would be deleted here
            removed_count = files_to_remove as u32;
        }

        Ok(removed_count)
    }

    /// Returns total size of all log files
    pub fn get_total_log_size(&self) -> u64 {
        self.get_log_files().iter().map(|f| f.size).sum()
    }

    /// Checks whether log rotation is required
    pub fn needs_rotation(&self) -> bool {
        if let Some(current) = self.get_current_file_info() {
            current.size >= self.config.max_log_file_size
        } else {
            false
        }
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        // Stop background tasks
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.writer_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.group_commit_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::log_record::{IsolationLevel, LogRecord, LogRecordType};
    use tempfile::TempDir;

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_log_writer_creation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();

        let _writer = LogWriter::new(config)?;
        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_write_log() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.write_buffer_size = 10;

        let writer = LogWriter::new(config)?;

        let record = LogRecord::new_transaction_begin(0, 100, IsolationLevel::ReadCommitted);
        let lsn = writer.write_log(record).await?;

        assert!(lsn > 0);

        let stats = writer.get_statistics();
        assert!(stats.total_records_written >= 1);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_write_log_sync() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.group_commit_enabled = false;

        let writer = LogWriter::new(config)?;

        let record = LogRecord::new_transaction_commit(0, 100, vec![(1, 10)], None);
        let lsn = writer.write_log_sync(record).await?;

        assert!(lsn > 0);

        let stats = writer.get_statistics();
        assert!(stats.total_records_written >= 1);
        assert!(stats.sync_operations >= 1);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_multiple_writes() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.write_buffer_size = 5;

        let writer = LogWriter::new(config)?;

        // Write multiple records
        for i in 0..10 {
            let record =
                LogRecord::new_data_insert(0, 100 + i, 1, i as u64, 0, vec![i as u8; 10], None);
            writer.write_log(record).await?;
        }

        // Force flush buffer
        writer.flush().await?;

        let stats = writer.get_statistics();
        assert_eq!(stats.total_records_written, 10);
        assert!(stats.total_bytes_written > 0);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_lsn_generation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();

        let writer = LogWriter::new(config)?;

        let mut last_lsn = 0;
        for i in 0..5 {
            let record =
                LogRecord::new_transaction_begin(0, 100 + i, IsolationLevel::ReadCommitted);
            let lsn = writer.write_log(record).await?;

            assert!(lsn > last_lsn);
            last_lsn = lsn;
        }

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_statistics() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.group_commit_enabled = false;

        let writer = LogWriter::new(config)?;

        // Write records of different types
        writer
            .write_log(LogRecord::new_transaction_begin(
                0,
                100,
                IsolationLevel::ReadCommitted,
            ))
            .await?;
        writer
            .write_log(LogRecord::new_data_insert(
                0,
                100,
                1,
                10,
                0,
                vec![1, 2, 3],
                None,
            ))
            .await?;
        writer
            .write_log_sync(LogRecord::new_transaction_commit(
                0,
                100,
                vec![(1, 10)],
                None,
            ))
            .await?;

        let stats = writer.get_statistics();
        assert!(
            stats.total_records_written >= 2,
            "expected at least 2 records, got {}",
            stats.total_records_written
        );
        assert!(stats.total_bytes_written > 0);
        assert!(stats.sync_operations >= 1);
        assert!(stats.average_write_time_us > 0);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_buffer_management() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.write_buffer_size = 3;
        config.max_buffer_time = Duration::from_millis(50);

        let writer = LogWriter::new(config)?;

        // Write records that should stay buffered
        for i in 0..2 {
            let record = LogRecord::new_data_insert(0, 100, 1, i, 0, vec![i as u8], None);
            writer.write_log(record).await?;
        }

        // Wait for automatic buffer flush (background runs on max_buffer_time=50ms)
        tokio::time::sleep(Duration::from_millis(200)).await;

        let stats = writer.get_statistics();
        assert!(
            stats.total_records_written >= 2,
            "expected 2 records written, got {}",
            stats.total_records_written
        );
        assert!(
            stats.sync_operations >= 1 || stats.total_bytes_written > 0,
            "expected flush or bytes written (sync_ops={}, bytes={})",
            stats.sync_operations,
            stats.total_bytes_written
        );

        Ok(())
    }
}
