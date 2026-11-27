//! In-memory `StorageBackend` for unit tests.
//!
//! Enforces the same alignment rules as the real backend, counts I/O, and
//! records the fencing tokens it receives. Allows corrupting stored bytes.

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
    writes: AtomicUsize,
    /// Tokens of every mutating call, in order.
    fences: Mutex<Vec<FenceToken>>,
}

impl MemBackend {
    pub(crate) fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    pub(crate) fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    pub(crate) fn fences(&self) -> Vec<FenceToken> {
        self.fences.lock().clone()
    }

    /// Copies `len` bytes of a stored file at `offset`.
    pub(crate) fn raw(&self, file: FileKey, offset: usize, len: usize) -> Vec<u8> {
        self.files
            .lock()
            .get(&file)
            .map(|d| d[offset..offset + len].to_vec())
            .unwrap_or_default()
    }

    /// Overwrites stored bytes in place (simulating a torn write).
    pub(crate) fn overwrite(&self, file: FileKey, offset: usize, bytes: &[u8]) {
        if let Some(data) = self.files.lock().get_mut(&file) {
            data[offset..offset + bytes.len()].copy_from_slice(bytes);
        }
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

    fn write_at(&self, file: FileKey, offset: u64, buf: &[u8], fence: FenceToken) -> Result<()> {
        check_io_alignment(offset, buf)?;
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.fences.lock().push(fence);
        let mut files = self.files.lock();
        let data = files.get_mut(&file).ok_or_else(|| not_found(file))?;
        let start = offset as usize;
        if data.len() < start + buf.len() {
            data.resize(start + buf.len(), 0);
        }
        data[start..start + buf.len()].copy_from_slice(buf);
        Ok(())
    }

    fn create(&self, file: FileKey, fence: FenceToken) -> Result<()> {
        self.fences.lock().push(fence);
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

    fn remove(&self, file: FileKey, fence: FenceToken) -> Result<()> {
        self.fences.lock().push(fence);
        self.files
            .lock()
            .remove(&file)
            .map(|_| ())
            .ok_or_else(|| not_found(file))
    }

    fn truncate(&self, file: FileKey, len: u64, fence: FenceToken) -> Result<()> {
        self.fences.lock().push(fence);
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
