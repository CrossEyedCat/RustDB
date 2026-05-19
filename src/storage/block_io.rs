//! Block I/O backend abstraction for rustdb
//!
//! Provides a trait for pluggable I/O backends (std::fs, io_uring on Linux).
//! Enables zero-copy and high-throughput disk I/O when using io_uring.

use crate::common::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Mutex, Once, OnceLock};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use std::os::unix::io::AsRawFd;

/// When set (non-`0`), dirty-page flush batches multiple page writes via
/// [`BlockIoBackend::write_at_batch`] (linked io_uring on Linux when available, else sequential).
pub fn io_uring_batch_writes_enabled() -> bool {
    std::env::var_os("RUSTDB_IO_URING_BATCH")
        .is_some_and(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn io_uring_disabled_by_env() -> bool {
    std::env::var_os("RUSTDB_USE_IO_URING")
        .is_some_and(|v| v == "0" || v.eq_ignore_ascii_case("false"))
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn io_uring_ring_error_is_fallback(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(1) | Some(13) | Some(38) | Some(95) // EPERM, EACCES, ENOSYS, ENOTSUP
    ) || matches!(
        err.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
    )
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
static IO_URING_PROBE: OnceLock<bool> = OnceLock::new();

#[cfg(all(target_os = "linux", feature = "io-uring"))]
static IO_URING_FALLBACK_LOG: Once = Once::new();

#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn io_uring_runtime_available() -> bool {
    *IO_URING_PROBE.get_or_init(|| {
        if io_uring_disabled_by_env() {
            return false;
        }
        match io_uring::IoUring::new(IoUringBackend::RING_ENTRIES) {
            Ok(_) => true,
            Err(e) if io_uring_ring_error_is_fallback(&e) => {
                IO_URING_FALLBACK_LOG.call_once(|| {
                    tracing::warn!(
                        error = %e,
                        "io_uring unavailable (e.g. Docker without CAP_SYS_ADMIN); \
                         using std::fs block I/O — batched flush still uses sequential write_at"
                    );
                });
                false
            }
            Err(e) => {
                IO_URING_FALLBACK_LOG.call_once(|| {
                    tracing::warn!(
                        error = %e,
                        "io_uring probe failed; using std::fs block I/O"
                    );
                });
                false
            }
        }
    })
}

/// Block I/O backend trait.
/// Implementations: StdFileBackend (all platforms), IoUringBackend (Linux).
pub trait BlockIoBackend: Send + Sync {
    /// Reads exactly `buf.len()` bytes at the given offset.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// Writes `data` at the given offset.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()>;

    /// Writes multiple `(offset, payload)` pairs. Default loops [`Self::write_at`].
    fn write_at_batch(&mut self, writes: &[(u64, &[u8])]) -> Result<()> {
        for (offset, data) in writes {
            self.write_at(*offset, data)?;
        }
        Ok(())
    }

    /// Synchronizes all data to disk.
    fn sync(&mut self) -> Result<()>;

    /// Extends the file to `new_size` bytes (by writing a zero byte at the end).
    fn extend(&mut self, new_size: u64) -> Result<()>;
}

/// Standard library file backend (std::fs::File).
/// Used on all platforms; fallback on Linux when io_uring is unavailable.
/// Wrapped in Mutex for Sync (safe sharing across threads).
pub struct StdFileBackend {
    file: Mutex<File>,
}

impl StdFileBackend {
    /// Creates a new file for writing (truncates if exists).
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| Error::database(format!("Failed to create file: {}", e)))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Opens an existing file.
    pub fn open(path: &Path, read_only: bool) -> Result<Self> {
        let file = if read_only {
            OpenOptions::new().read(true).open(path)
        } else {
            OpenOptions::new().read(true).write(true).open(path)
        }
        .map_err(|e| Error::database(format!("Failed to open file: {}", e)))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

/// Creates an appropriate I/O backend for the platform.
/// On Linux with `io-uring` feature: uses `IoUringBackend` when the ring is available,
/// otherwise [`StdFileBackend`]. Set `RUSTDB_USE_IO_URING=0` to skip io_uring entirely.
#[allow(clippy::missing_panics_doc)]
pub fn create_backend_for_file(
    path: &Path,
    create: bool,
    read_only: bool,
) -> Result<Box<dyn BlockIoBackend>> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        if io_uring_runtime_available() {
            return if create {
                IoUringBackend::create(path).map(|b| Box::new(b) as Box<dyn BlockIoBackend>)
            } else {
                IoUringBackend::open(path, read_only)
                    .map(|b| Box::new(b) as Box<dyn BlockIoBackend>)
            };
        }
        if create {
            Ok(Box::new(StdFileBackend::create(path)?))
        } else {
            Ok(Box::new(StdFileBackend::open(path, read_only)?))
        }
    }

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        if create {
            Ok(Box::new(StdFileBackend::create(path)?))
        } else {
            Ok(Box::new(StdFileBackend::open(path, read_only)?))
        }
    }
}

impl BlockIoBackend for StdFileBackend {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::database(format!("Seek error: {}", e)))?;
        file.read_exact(buf)
            .map_err(|e| Error::database(format!("Read error: {}", e)))?;
        Ok(())
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::database(format!("Seek error: {}", e)))?;
        file.write_all(data)
            .map_err(|e| Error::database(format!("Write error: {}", e)))?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        file.sync_all()
            .map_err(|e| Error::database(format!("Sync error: {}", e)))?;
        Ok(())
    }

    fn extend(&mut self, new_size: u64) -> Result<()> {
        if new_size == 0 {
            return Ok(());
        }
        let mut file = self
            .file
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        file.seek(SeekFrom::Start(new_size - 1))
            .map_err(|e| Error::database(format!("Seek error: {}", e)))?;
        file.write_all(&[0])
            .map_err(|e| Error::database(format!("Extend error: {}", e)))?;
        Ok(())
    }
}

/// IoUring-based backend for Linux (kernel 5.6+).
/// Requires `--features io-uring` and `target_os = "linux"`.
/// Uses io_uring for low-overhead, high-throughput disk I/O.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub struct IoUringBackend {
    file: File,
    ring: Mutex<io_uring::IoUring>,
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
impl IoUringBackend {
    const RING_ENTRIES: u32 = 64;

    /// Creates a new file for writing (truncates if exists).
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| Error::database(format!("Failed to create file: {}", e)))?;
        let ring = io_uring::IoUring::new(Self::RING_ENTRIES)
            .map_err(|e| Error::database(format!("Failed to create io_uring: {}", e)))?;
        Ok(Self {
            file,
            ring: Mutex::new(ring),
        })
    }

    /// Opens an existing file.
    pub fn open(path: &Path, read_only: bool) -> Result<Self> {
        let file = if read_only {
            OpenOptions::new().read(true).open(path)
        } else {
            OpenOptions::new().read(true).write(true).open(path)
        }
        .map_err(|e| Error::database(format!("Failed to open file: {}", e)))?;
        let ring = io_uring::IoUring::new(Self::RING_ENTRIES)
            .map_err(|e| Error::database(format!("Failed to create io_uring: {}", e)))?;
        Ok(Self {
            file,
            ring: Mutex::new(ring),
        })
    }

    fn write_at_single(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let len = data.len();
        if len == 0 {
            return Ok(());
        }
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write_e = io_uring::opcode::Write::new(fd, data.as_ptr(), len as u32)
            .offset(offset)
            .build()
            .user_data(0);
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        unsafe {
            ring.submission()
                .push(&write_e)
                .map_err(|e| Error::database(format!("Failed to push write: {}", e)))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| Error::database(format!("io_uring submit_and_wait: {}", e)))?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| Error::database("io_uring completion queue empty"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(Error::database(format!(
                "io_uring write failed: {}",
                std::io::Error::from_raw_os_error(-res)
            )));
        }
        if res as usize != len {
            return Err(Error::database(format!(
                "io_uring short write: got {} expected {}",
                res, len
            )));
        }
        Ok(())
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
impl BlockIoBackend for IoUringBackend {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let len = buf.len();
        if len == 0 {
            return Ok(());
        }
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let read_e = io_uring::opcode::Read::new(fd, buf.as_mut_ptr(), len as u32)
            .offset(offset)
            .build()
            .user_data(0);
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        unsafe {
            ring.submission()
                .push(&read_e)
                .map_err(|e| Error::database(format!("Failed to push read: {}", e)))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| Error::database(format!("io_uring submit_and_wait: {}", e)))?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| Error::database("io_uring completion queue empty"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(Error::database(format!(
                "io_uring read failed: {}",
                std::io::Error::from_raw_os_error(-res)
            )));
        }
        // res is bytes read; we expect exactly buf.len()
        if res as usize != len {
            return Err(Error::database(format!(
                "io_uring short read: got {} expected {}",
                res, len
            )));
        }
        Ok(())
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        self.write_at_batch(&[(offset, data)])
    }

    fn write_at_batch(&mut self, writes: &[(u64, &[u8])]) -> Result<()> {
        use io_uring::squeue::Flags;
        let non_empty: Vec<(u64, &[u8])> = writes
            .iter()
            .copied()
            .filter(|(_, data)| !data.is_empty())
            .collect();
        if non_empty.is_empty() {
            return Ok(());
        }
        if !io_uring_batch_writes_enabled() || non_empty.len() == 1 {
            for (offset, data) in non_empty {
                self.write_at_single(offset, data)?;
            }
            return Ok(());
        }

        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        let mut submission = ring.submission();
        let last = non_empty.len() - 1;
        for (i, (offset, data)) in non_empty.iter().enumerate() {
            let len = data.len() as u32;
            let entry = io_uring::opcode::Write::new(fd, data.as_ptr(), len)
                .offset(*offset)
                .build()
                .user_data(i as u64);
            let mut flags = Flags::empty();
            if i < last {
                flags |= Flags::IO_LINK;
            } else {
                flags |= Flags::IO_DRAIN;
            }
            let entry = entry.flags(flags);
            unsafe {
                submission
                    .push(&entry)
                    .map_err(|e| Error::database(format!("Failed to push batched write: {}", e)))?;
            }
        }
        drop(submission);
        ring.submit_and_wait(non_empty.len())
            .map_err(|e| Error::database(format!("io_uring submit_and_wait batch: {}", e)))?;
        let mut completion = ring.completion();
        for _ in 0..non_empty.len() {
            let cqe = completion
                .next()
                .ok_or_else(|| Error::database("io_uring completion queue empty (batch)"))?;
            let res = cqe.result();
            if res < 0 {
                return Err(Error::database(format!(
                    "io_uring batched write failed: {}",
                    std::io::Error::from_raw_os_error(-res)
                )));
            }
            let idx = cqe.user_data() as usize;
            let expected = non_empty[idx].1.len();
            if res as usize != expected {
                return Err(Error::database(format!(
                    "io_uring short write at index {}: got {} expected {}",
                    idx, res, expected
                )));
            }
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let fsync_e = io_uring::opcode::Fsync::new(fd).build().user_data(0);
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        unsafe {
            ring.submission()
                .push(&fsync_e)
                .map_err(|e| Error::database(format!("Failed to push fsync: {}", e)))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| Error::database(format!("io_uring submit_and_wait: {}", e)))?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| Error::database("io_uring completion queue empty"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(Error::database(format!(
                "io_uring fsync failed: {}",
                std::io::Error::from_raw_os_error(-res)
            )));
        }
        Ok(())
    }

    fn extend(&mut self, new_size: u64) -> Result<()> {
        if new_size == 0 {
            return Ok(());
        }
        // Write single zero byte at position new_size - 1 to extend the file
        let zero: [u8; 1] = [0];
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write_e = io_uring::opcode::Write::new(fd, zero.as_ptr(), 1)
            .offset(new_size - 1)
            .build()
            .user_data(0);
        let mut ring = self
            .ring
            .lock()
            .map_err(|e| Error::database(format!("Lock error: {}", e)))?;
        unsafe {
            ring.submission()
                .push(&write_e)
                .map_err(|e| Error::database(format!("Failed to push extend write: {}", e)))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| Error::database(format!("io_uring submit_and_wait: {}", e)))?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| Error::database("io_uring completion queue empty"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(Error::database(format!(
                "io_uring extend failed: {}",
                std::io::Error::from_raw_os_error(-res)
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn io_uring_batch_writes_env_defaults_off() {
        std::env::remove_var("RUSTDB_IO_URING_BATCH");
        assert!(!io_uring_batch_writes_enabled());
        assert!(!io_uring_batch_writes_enabled());
    }

    #[test]
    fn io_uring_batch_writes_env_respects_values() {
        std::env::set_var("RUSTDB_IO_URING_BATCH", "1");
        assert!(io_uring_batch_writes_enabled());
        std::env::set_var("RUSTDB_IO_URING_BATCH", "false");
        assert!(!io_uring_batch_writes_enabled());
        std::env::remove_var("RUSTDB_IO_URING_BATCH");
    }

    #[test]
    fn std_backend_write_at_batch_roundtrip() -> Result<()> {
        let dir = TempDir::new().map_err(|e| Error::database(e.to_string()))?;
        let path = dir.path().join("std_batch.bin");
        let mut backend = StdFileBackend::create(&path)?;
        let chunk_a = vec![0xAAu8; 32];
        let chunk_b = vec![0xBBu8; 32];
        backend.write_at_batch(&[(0, &chunk_a), (64, &chunk_b)])?;
        backend.sync()?;

        let mut buf = vec![0u8; 32];
        backend.read_at(0, &mut buf)?;
        assert_eq!(buf, chunk_a);
        backend.read_at(64, &mut buf)?;
        assert_eq!(buf, chunk_b);
        Ok(())
    }

    #[test]
    fn create_backend_write_at_batch_roundtrip() -> Result<()> {
        let dir = TempDir::new().map_err(|e| Error::database(e.to_string()))?;
        let path = dir.path().join("backend_batch.bin");
        let mut backend = create_backend_for_file(&path, true, false)?;
        backend.write_at_batch(&[(4, b"abcd"), (16, b"efgh")])?;
        backend.sync()?;

        let mut buf = [0u8; 4];
        backend.read_at(4, &mut buf)?;
        assert_eq!(&buf, b"abcd");
        backend.read_at(16, &mut buf)?;
        assert_eq!(&buf, b"efgh");
        Ok(())
    }

    #[test]
    fn write_at_batch_skips_empty_payloads() -> Result<()> {
        let dir = TempDir::new().map_err(|e| Error::database(e.to_string()))?;
        let path = dir.path().join("empty_skip.bin");
        let mut backend = StdFileBackend::create(&path)?;
        backend.write_at_batch(&[(0, b""), (8, b"x")])?;
        let mut one = [0u8; 1];
        backend.read_at(8, &mut one)?;
        assert_eq!(one, [b'x']);
        Ok(())
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
#[cfg(test)]
mod io_uring_tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};
    use tempfile::TempDir;

    #[test]
    fn io_uring_ring_error_is_fallback_classifies_os_errors() {
        assert!(io_uring_ring_error_is_fallback(
            &IoError::from_raw_os_error(1)
        ));
        assert!(io_uring_ring_error_is_fallback(
            &IoError::from_raw_os_error(13)
        ));
        assert!(io_uring_ring_error_is_fallback(
            &IoError::from_raw_os_error(38)
        ));
        assert!(io_uring_ring_error_is_fallback(
            &IoError::from_raw_os_error(95)
        ));
        assert!(io_uring_ring_error_is_fallback(&IoError::new(
            ErrorKind::PermissionDenied,
            "denied"
        )));
        assert!(io_uring_ring_error_is_fallback(&IoError::new(
            ErrorKind::Unsupported,
            "unsupported"
        )));
        assert!(!io_uring_ring_error_is_fallback(&IoError::new(
            ErrorKind::Other,
            "other"
        )));
    }

    #[test]
    fn create_backend_uses_std_when_io_uring_disabled_by_env() -> Result<()> {
        std::env::set_var("RUSTDB_USE_IO_URING", "0");
        let dir = TempDir::new().map_err(|e| Error::database(e.to_string()))?;
        let path = dir.path().join("std_fallback.bin");
        let mut backend = create_backend_for_file(&path, true, false)?;
        backend.write_at_batch(&[(0, b"std")])?;
        let mut buf = [0u8; 3];
        backend.read_at(0, &mut buf)?;
        assert_eq!(&buf, b"std");
        std::env::remove_var("RUSTDB_USE_IO_URING");
        Ok(())
    }

    #[test]
    fn test_io_uring_read_write() {
        if !super::io_uring_runtime_available() {
            eprintln!("skip test_io_uring_read_write: io_uring not available");
            return;
        }
        let dir = std::env::temp_dir();
        let path = dir.join("rustdb_io_uring_test");
        let _ = std::fs::remove_file(&path);

        let mut backend = IoUringBackend::create(&path).expect("create");
        let data = b"hello io_uring";
        backend.write_at(0, data).expect("write");
        backend.sync().expect("sync");

        let mut buf = vec![0u8; data.len()];
        backend.read_at(0, &mut buf).expect("read");
        assert_eq!(&buf[..], data.as_slice());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_io_uring_write_at_batch_linked() {
        if !super::io_uring_runtime_available() {
            eprintln!("skip test_io_uring_write_at_batch_linked: io_uring not available");
            return;
        }
        std::env::set_var("RUSTDB_IO_URING_BATCH", "1");
        let dir = std::env::temp_dir();
        let path = dir.join("rustdb_io_uring_batch_test");
        let _ = std::fs::remove_file(&path);

        let mut backend = IoUringBackend::create(&path).expect("create");
        let a = vec![1u8; 16];
        let b = vec![2u8; 16];
        backend
            .write_at_batch(&[(0, &a), (32, &b)])
            .expect("batched write");
        backend.sync().expect("sync");

        let mut buf = vec![0u8; 16];
        backend.read_at(0, &mut buf).expect("read a");
        assert_eq!(buf, a);
        backend.read_at(32, &mut buf).expect("read b");
        assert_eq!(buf, b);

        std::env::remove_var("RUSTDB_IO_URING_BATCH");
        let _ = std::fs::remove_file(&path);
    }
}
