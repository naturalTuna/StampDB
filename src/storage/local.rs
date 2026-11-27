//! Local-directory storage backend (single install mode).
//!
//! Files are opened with `O_DIRECT` and accessed with pread/pwrite
//! (`FileExt::read_exact_at` / `write_all_at`). Durability points use
//! fdatasync, and file creation/removal also fsyncs the parent directory.
//!
//! Layout under the data directory:
//! ```text
//! <root>/rel/<relfileid>   relation data files (no segmenting)
//! <root>/wal/<TTTTTTTTSSSSSSSSSSSSSSSS>  WAL segments (timeline, segno)
//! <root>/control           control file
//! ```
//!
//! On open, the logical block size of the device holding `<root>` is read
//! from sysfs and startup is refused if it cannot be determined or exceeds
//! `IO_ALIGN`. (BLKSSZGET needs the block device to be opened, which
//! requires root or the `disk` group; sysfs does not.)

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use super::aligned::IO_ALIGN;
use super::backend::{FenceToken, FileKey, StorageBackend, check_io_alignment};
use super::{Result, StorageError};

const REL_DIR: &str = "rel";
const WAL_DIR: &str = "wal";
const CONTROL_FILE: &str = "control";
const SYSFS_DEV_BLOCK: &str = "/sys/dev/block";

/// Backend storing files in a local directory with Direct I/O.
#[derive(Debug)]
pub struct LocalDirBackend {
    root: PathBuf,
    rel_dir: PathBuf,
    wal_dir: PathBuf,
    /// Open handles, so each I/O does not reopen the file.
    files: Mutex<HashMap<FileKey, Arc<File>>>,
}

impl LocalDirBackend {
    /// Opens (and lays out, if needed) a data directory.
    ///
    /// Refuses to start if the device's logical block size is unknown or
    /// larger than `IO_ALIGN`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let meta = fs::metadata(root).map_err(|e| io_err("stat", root, e))?;
        if !meta.is_dir() {
            return Err(io_err(
                "open data directory",
                root,
                io::Error::from(io::ErrorKind::NotADirectory),
            ));
        }
        check_block_size(root, meta.dev(), Path::new(SYSFS_DEV_BLOCK))?;

        let rel_dir = root.join(REL_DIR);
        let wal_dir = root.join(WAL_DIR);
        for dir in [&rel_dir, &wal_dir] {
            match fs::create_dir(dir) {
                Ok(()) => fsync_dir(root)?,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(io_err("mkdir", dir, e)),
            }
        }
        Ok(Self {
            root: root.to_path_buf(),
            rel_dir,
            wal_dir,
            files: Mutex::new(HashMap::new()),
        })
    }

    fn path(&self, file: FileKey) -> PathBuf {
        match file {
            FileKey::Relation(r) => self.rel_dir.join(r.0.to_string()),
            FileKey::Wal { timeline, segno } => {
                self.wal_dir.join(format!("{timeline:08X}{segno:016X}"))
            }
            FileKey::Control => self.root.join(CONTROL_FILE),
        }
    }

    fn parent_dir(&self, file: FileKey) -> &Path {
        match file {
            FileKey::Relation(_) => &self.rel_dir,
            FileKey::Wal { .. } => &self.wal_dir,
            FileKey::Control => &self.root,
        }
    }

    fn handle(&self, file: FileKey) -> Result<Arc<File>> {
        let mut files = self.files.lock();
        if let Some(f) = files.get(&file) {
            return Ok(Arc::clone(f));
        }
        let path = self.path(file);
        let f = Arc::new(
            direct_options()
                .open(&path)
                .map_err(|e| io_err("open", &path, e))?,
        );
        files.insert(file, Arc::clone(&f));
        Ok(f)
    }
}

impl StorageBackend for LocalDirBackend {
    fn read_at(&self, file: FileKey, offset: u64, buf: &mut [u8]) -> Result<()> {
        check_io_alignment(offset, buf)?;
        let f = self.handle(file)?;
        f.read_exact_at(buf, offset).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                StorageError::ShortRead {
                    file,
                    offset,
                    len: buf.len(),
                }
            } else {
                io_err("pread", &self.path(file), e)
            }
        })
    }

    fn write_at(&self, file: FileKey, offset: u64, buf: &[u8], _fence: FenceToken) -> Result<()> {
        // Single mode: the fence token is intentionally not checked.
        check_io_alignment(offset, buf)?;
        let f = self.handle(file)?;
        f.write_all_at(buf, offset)
            .map_err(|e| io_err("pwrite", &self.path(file), e))
    }

    fn create(&self, file: FileKey, _fence: FenceToken) -> Result<()> {
        let path = self.path(file);
        let f = direct_options()
            .create_new(true)
            .open(&path)
            .map_err(|e| io_err("create", &path, e))?;
        fsync_dir(self.parent_dir(file))?;
        self.files.lock().insert(file, Arc::new(f));
        Ok(())
    }

    fn remove(&self, file: FileKey, _fence: FenceToken) -> Result<()> {
        self.files.lock().remove(&file);
        let path = self.path(file);
        fs::remove_file(&path).map_err(|e| io_err("unlink", &path, e))?;
        fsync_dir(self.parent_dir(file))
    }

    fn truncate(&self, file: FileKey, len: u64, _fence: FenceToken) -> Result<()> {
        if !len.is_multiple_of(IO_ALIGN as u64) {
            return Err(StorageError::Misaligned {
                what: "length",
                value: len,
            });
        }
        let f = self.handle(file)?;
        f.set_len(len)
            .map_err(|e| io_err("ftruncate", &self.path(file), e))
    }

    fn file_len(&self, file: FileKey) -> Result<u64> {
        let f = self.handle(file)?;
        let meta = f
            .metadata()
            .map_err(|e| io_err("fstat", &self.path(file), e))?;
        Ok(meta.len())
    }

    fn sync(&self, file: FileKey) -> Result<()> {
        let f = self.handle(file)?;
        f.sync_data()
            .map_err(|e| io_err("fdatasync", &self.path(file), e))
    }
}

fn direct_options() -> OpenOptions {
    let mut o = OpenOptions::new();
    o.read(true).write(true).custom_flags(libc::O_DIRECT);
    o
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync directory", dir, e))
}

fn io_err(op: &'static str, path: &Path, source: io::Error) -> StorageError {
    StorageError::FileIo {
        op,
        path: path.to_path_buf(),
        source,
    }
}

/// Splits a Linux `dev_t` into (major, minor) using the glibc encoding.
fn dev_major_minor(dev: u64) -> (u64, u64) {
    let major = ((dev >> 32) & 0xffff_f000) | ((dev >> 8) & 0x0fff);
    let minor = ((dev >> 12) & 0xffff_ff00) | (dev & 0x00ff);
    (major, minor)
}

/// Logical block size of device `dev`, read from `<sysfs>/<maj>:<min>`.
///
/// Partitions have no `queue/` directory; their parent disk's is used.
fn logical_block_size(dev: u64, sysfs_dev_block: &Path) -> std::result::Result<u64, String> {
    let (major, minor) = dev_major_minor(dev);
    let node = sysfs_dev_block.join(format!("{major}:{minor}"));
    let node =
        fs::canonicalize(&node).map_err(|e| format!("no sysfs entry {}: {e}", node.display()))?;
    let mut candidates = vec![node.join("queue/logical_block_size")];
    if let Some(parent) = node.parent() {
        candidates.push(parent.join("queue/logical_block_size"));
    }
    for c in &candidates {
        if let Ok(s) = fs::read_to_string(c) {
            return s
                .trim()
                .parse::<u64>()
                .map_err(|e| format!("cannot parse {}: {e}", c.display()));
        }
    }
    Err(format!(
        "no queue/logical_block_size under {}",
        node.display()
    ))
}

fn check_block_size(root: &Path, dev: u64, sysfs_dev_block: &Path) -> Result<()> {
    let size = logical_block_size(dev, sysfs_dev_block).map_err(|reason| {
        StorageError::BlockSizeUnknown {
            path: root.to_path_buf(),
            reason,
        }
    })?;
    if size == 0 || size > IO_ALIGN as u64 || !(IO_ALIGN as u64).is_multiple_of(size) {
        return Err(StorageError::BlockSizeUnsupported { size });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn makedev(major: u64, minor: u64) -> u64 {
        ((major & 0xffff_f000) << 32)
            | ((major & 0x0fff) << 8)
            | ((minor & 0xffff_ff00) << 12)
            | (minor & 0x00ff)
    }

    #[test]
    fn dev_split() {
        assert_eq!(dev_major_minor(64768), (253, 0)); // dm-0
        assert_eq!(dev_major_minor(makedev(8, 1)), (8, 1));
        assert_eq!(dev_major_minor(makedev(259, 300)), (259, 300));
    }

    /// Builds a fake `/sys/dev/block` with a disk (8:0) with `queue/` and
    /// a partition (8:1) without one, like real sysfs.
    fn fake_sysfs(block_size: &str) -> tempfile::TempDir {
        let t = tempfile::tempdir().unwrap();
        let disk = t.path().join("devices/sda");
        fs::create_dir_all(disk.join("queue")).unwrap();
        fs::write(disk.join("queue/logical_block_size"), block_size).unwrap();
        fs::create_dir_all(disk.join("sda1")).unwrap();
        let dev_block = t.path().join("dev/block");
        fs::create_dir_all(&dev_block).unwrap();
        std::os::unix::fs::symlink(&disk, dev_block.join("8:0")).unwrap();
        std::os::unix::fs::symlink(disk.join("sda1"), dev_block.join("8:1")).unwrap();
        t
    }

    #[test]
    fn reads_disk_and_partition_block_size() {
        let t = fake_sysfs("512\n");
        let sys = t.path().join("dev/block");
        assert_eq!(logical_block_size(makedev(8, 0), &sys), Ok(512));
        assert_eq!(logical_block_size(makedev(8, 1), &sys), Ok(512));
        assert!(logical_block_size(makedev(9, 9), &sys).is_err());
    }

    #[test]
    fn block_size_policy() {
        let root = Path::new("/data");
        for (size, ok) in [
            ("512", true),
            ("4096", true),
            ("8192", false),
            ("520", false),
        ] {
            let t = fake_sysfs(size);
            let r = check_block_size(root, makedev(8, 0), &t.path().join("dev/block"));
            assert_eq!(r.is_ok(), ok, "size {size}");
        }
        let t = fake_sysfs("512");
        assert!(matches!(
            check_block_size(root, makedev(1, 1), &t.path().join("dev/block")),
            Err(StorageError::BlockSizeUnknown { .. })
        ));
    }
}
