//! In-memory `StorageBackend` for unit tests.
//!
//! Enforces the same alignment rules as the real backend, counts reads, and
//! allows corrupting stored bytes.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::backend::{FenceToken, FileKey, StorageBackend, check_io_alignment};
use super::{Result, StorageError};

#[derive(Debug, Default)]
pub(crate) struct MemBackend {
    files: Mutex<HashMap<FileKey, Vec<u8>>>,
    reads: AtomicUsize,
}

impl MemBackend {
    pub(crate) fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// Flips one byte of a stored file.
    pub(crate) fn corrupt(&self, file: FileKey, offset: usize) {
        if let Some(data) = self.files.lock().get_mut(&file) {
            data[offset] ^= 0xFF;
        }
    }
}

fn not_found(file: FileKey) -> StorageError {
    StorageError::FileIo {
        op: "open",
        path: PathBuf::from(format!("mem:{file}")),
        source: io::Error::from(io::ErrorKind::NotFound),
    }
}

impl StorageBackend for MemBackend {
    fn read_at(&self, file: FileKey, offset: u64, buf: &mut [u8]) -> Result<()> {
        check_io_alignment(offset, buf)?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        let files = self.files.lock();
        let data = files.get(&file).ok_or_else(|| not_found(file))?;
        let start = offset as usize;
        if start + buf.len() > data.len() {
            return Err(StorageError::ShortRead {
                file,
                offset,
                len: buf.len(),
            });
        }
        buf.copy_from_slice(&data[start..start + buf.len()]);
        Ok(())
    }

    fn write_at(&self, file: FileKey, offset: u64, buf: &[u8], _fence: FenceToken) -> Result<()> {
        check_io_alignment(offset, buf)?;
        let mut files = self.files.lock();
        let data = files.get_mut(&file).ok_or_else(|| not_found(file))?;
        let start = offset as usize;
        if data.len() < start + buf.len() {
            data.resize(start + buf.len(), 0);
        }
        data[start..start + buf.len()].copy_from_slice(buf);
        Ok(())
    }

    fn create(&self, file: FileKey, _fence: FenceToken) -> Result<()> {
        let mut files = self.files.lock();
        if files.contains_key(&file) {
            return Err(StorageError::FileIo {
                op: "create",
                path: PathBuf::from(format!("mem:{file}")),
                source: io::Error::from(io::ErrorKind::AlreadyExists),
            });
        }
        files.insert(file, Vec::new());
        Ok(())
    }

    fn remove(&self, file: FileKey, _fence: FenceToken) -> Result<()> {
        self.files
            .lock()
            .remove(&file)
            .map(|_| ())
            .ok_or_else(|| not_found(file))
    }

    fn truncate(&self, file: FileKey, len: u64, _fence: FenceToken) -> Result<()> {
        let mut files = self.files.lock();
        let data = files.get_mut(&file).ok_or_else(|| not_found(file))?;
        data.resize(len as usize, 0);
        Ok(())
    }

    fn file_len(&self, file: FileKey) -> Result<u64> {
        let files = self.files.lock();
        let data = files.get(&file).ok_or_else(|| not_found(file))?;
        Ok(data.len() as u64)
    }

    fn sync(&self, file: FileKey) -> Result<()> {
        if self.files.lock().contains_key(&file) {
            Ok(())
        } else {
            Err(not_found(file))
        }
    }
}
