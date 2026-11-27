//! Integration tests for `LocalDirBackend` (real O_DIRECT I/O).
//!
//! Uses Cargo's target tmpdir rather than /tmp, which may be tmpfs
//! (no O_DIRECT support on EL9 kernels).

use mydb::storage::backend::{FenceToken, FileKey, RelFileId, StorageBackend};
use mydb::storage::local::LocalDirBackend;
use mydb::storage::{AlignedBuf, IO_ALIGN, StorageError};

const FENCE: FenceToken = FenceToken::new(1);
const REL: FileKey = FileKey::Relation(RelFileId(16384));

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap()
}

fn filled(len: usize, byte: u8) -> AlignedBuf {
    let mut b = AlignedBuf::new(len).unwrap();
    b.fill(byte);
    b
}

#[test]
fn write_read_roundtrip() {
    let dir = tempdir();
    let be = LocalDirBackend::open(dir.path()).unwrap();
    be.create(REL, FENCE).unwrap();
    assert_eq!(be.file_len(REL).unwrap(), 0);

    be.write_at(REL, 0, &filled(IO_ALIGN, 0xAA), FENCE).unwrap();
    be.write_at(REL, 2 * IO_ALIGN as u64, &filled(IO_ALIGN, 0xBB), FENCE)
        .unwrap();
    be.sync(REL).unwrap();
    assert_eq!(be.file_len(REL).unwrap(), 3 * IO_ALIGN as u64);

    let mut buf = AlignedBuf::new(3 * IO_ALIGN).unwrap();
    be.read_at(REL, 0, &mut buf).unwrap();
    assert!(buf[..IO_ALIGN].iter().all(|&b| b == 0xAA));
    assert!(buf[IO_ALIGN..2 * IO_ALIGN].iter().all(|&b| b == 0)); // hole
    assert!(buf[2 * IO_ALIGN..].iter().all(|&b| b == 0xBB));
}

#[test]
fn data_survives_reopen() {
    let dir = tempdir();
    {
        let be = LocalDirBackend::open(dir.path()).unwrap();
        be.create(REL, FENCE).unwrap();
        be.write_at(REL, 0, &filled(IO_ALIGN, 7), FENCE).unwrap();
        be.sync(REL).unwrap();
    }
    let be = LocalDirBackend::open(dir.path()).unwrap();
    let mut buf = AlignedBuf::new(IO_ALIGN).unwrap();
    be.read_at(REL, 0, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 7));
    assert!(dir.path().join("rel/16384").is_file());
}

#[test]
fn rejects_misaligned_io() {
    let dir = tempdir();
    let be = LocalDirBackend::open(dir.path()).unwrap();
    be.create(REL, FENCE).unwrap();
    let mut buf = AlignedBuf::new(2 * IO_ALIGN).unwrap();
    assert!(matches!(
        be.write_at(REL, 100, &buf[..IO_ALIGN], FENCE),
        Err(StorageError::Misaligned { what: "offset", .. })
    ));
    assert!(matches!(
        be.write_at(REL, 0, &buf[..100], FENCE),
        Err(StorageError::Misaligned { what: "length", .. })
    ));
    assert!(matches!(
        be.read_at(REL, 0, &mut buf[8..8 + IO_ALIGN]),
        Err(StorageError::Misaligned {
            what: "buffer address",
            ..
        })
    ));
    assert!(matches!(
        be.truncate(REL, 100, FENCE),
        Err(StorageError::Misaligned { .. })
    ));
}

#[test]
fn read_past_eof_is_short_read() {
    let dir = tempdir();
    let be = LocalDirBackend::open(dir.path()).unwrap();
    be.create(REL, FENCE).unwrap();
    be.write_at(REL, 0, &filled(IO_ALIGN, 1), FENCE).unwrap();
    let mut buf = AlignedBuf::new(2 * IO_ALIGN).unwrap();
    assert!(matches!(
        be.read_at(REL, 0, &mut buf),
        Err(StorageError::ShortRead { offset: 0, .. })
    ));
}

#[test]
fn truncate_and_remove() {
    let dir = tempdir();
    let be = LocalDirBackend::open(dir.path()).unwrap();
    be.create(REL, FENCE).unwrap();
    be.write_at(REL, 0, &filled(4 * IO_ALIGN, 1), FENCE)
        .unwrap();
    be.truncate(REL, IO_ALIGN as u64, FENCE).unwrap();
    be.sync(REL).unwrap();
    assert_eq!(be.file_len(REL).unwrap(), IO_ALIGN as u64);

    be.remove(REL, FENCE).unwrap();
    assert!(!dir.path().join("rel/16384").exists());
    assert!(matches!(
        be.file_len(REL),
        Err(StorageError::FileIo { op: "open", .. })
    ));
}

#[test]
fn create_existing_fails() {
    let dir = tempdir();
    let be = LocalDirBackend::open(dir.path()).unwrap();
    be.create(REL, FENCE).unwrap();
    assert!(matches!(
        be.create(REL, FENCE),
        Err(StorageError::FileIo { op: "create", .. })
    ));
}

#[test]
fn open_requires_directory() {
    let dir = tempdir();
    assert!(LocalDirBackend::open(dir.path().join("missing")).is_err());
    let file = dir.path().join("plain");
    std::fs::write(&file, b"x").unwrap();
    assert!(LocalDirBackend::open(&file).is_err());
}
