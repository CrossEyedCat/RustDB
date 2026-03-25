//! Block I/O backend abstraction for rustdb
//!
//! Provides a trait for pluggable I/O backends (std::fs, io_uring on Linux).
//! Enables zero-copy and high-throughput disk I/O when using io_uring.

use crate::common::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use std::os::unix::io::AsRawFd;

/// Block I/O backend trait.
/// Implementations: StdFileBackend (all platforms), IoUringBackend (Linux).
pub trait BlockIoBackend: Send + Sync {
    /// Reads exactly `buf.len()` bytes at the given offset.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// Writes `data` at the given offset.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()>;

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
/// On Linux with `io-uring` feature: uses IoUringBackend.
/// Otherwise: uses StdFileBackend.
#[allow(clippy::missing_panics_doc)]
pub fn create_backend_for_file(
    path: &Path,
    create: bool,
    read_only: bool,
) -> Result<Box<dyn BlockIoBackend>> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        if create {
            Ok(Box::new(IoUringBackend::create(path)?))
        } else {
            Ok(Box::new(IoUringBackend::open(path, read_only)?))
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

#[cfg(all(target_os = "linux", feature = "io-uring"))]
#[cfg(test)]
mod io_uring_tests {
    use super::*;

    #[test]
    fn test_io_uring_read_write() {
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
}
