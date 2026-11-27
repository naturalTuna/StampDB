//! Integration tests for `DiskManager` over `LocalDirBackend`.

use std::sync::Arc;

use mydb::storage::backend::{FenceToken, FileKey, RelFileId, StorageBackend};
use mydb::storage::checksum::set_checksum;
use mydb::storage::disk::DiskManager;
use mydb::storage::local::LocalDirBackend;
use mydb::storage::page::{BlockNumber, Lsn, PAGE_SIZE, PageKind, is_new_page};
use mydb::storage::slotted::{SlotId, SlottedPage};
use mydb::storage::{AlignedBuf, IO_ALIGN, StorageError};

const FENCE: FenceToken = FenceToken::new(1);
const REL: RelFileId = RelFileId(16384);

struct Env {
    _dir: tempfile::TempDir,
    backend: Arc<LocalDirBackend>,
    dm: DiskManager,
}

fn env() -> Env {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let backend = Arc::new(LocalDirBackend::open(dir.path()).unwrap());
    let dm = DiskManager::new(backend.clone());
    dm.create_relation(REL, FENCE).unwrap();
    Env {
        _dir: dir,
        backend,
        dm,
    }
}

fn heap_page(item: &[u8], lsn: u64) -> AlignedBuf {
    let mut p = SlottedPage::init(AlignedBuf::new(PAGE_SIZE).unwrap(), PageKind::Heap, 0).unwrap();
    p.add_item(item).unwrap().unwrap();
    p.set_lsn(Lsn(lsn));
    p.into_inner()
}

fn page_buf() -> AlignedBuf {
    AlignedBuf::new(PAGE_SIZE).unwrap()
}

#[test]
fn extend_write_read_roundtrip() {
    let e = env();
    assert_eq!(e.dm.nblocks(REL).unwrap(), 0);
    assert_eq!(e.dm.extend(REL, FENCE).unwrap(), BlockNumber(0));
    assert_eq!(e.dm.extend(REL, FENCE).unwrap(), BlockNumber(1));
    assert_eq!(e.dm.nblocks(REL).unwrap(), 2);

    let mut page = heap_page(b"tuple-1", 100);
    e.dm.write_page(REL, BlockNumber(1), &mut page, FENCE)
        .unwrap();
    e.dm.sync_relation(REL).unwrap();

    let mut buf = page_buf();
    e.dm.read_page(REL, BlockNumber(1), &mut buf).unwrap();
    let p = SlottedPage::open(&buf[..]).unwrap();
    assert_eq!(p.get_item(SlotId(0)), Some(&b"tuple-1"[..]));
    assert_eq!(p.lsn(), Lsn(100));

    // Extended but never written page reads back as a new page.
    e.dm.read_page(REL, BlockNumber(0), &mut buf).unwrap();
    assert!(is_new_page(&buf));
}

#[test]
fn survives_reopen() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    {
        let dm = DiskManager::new(Arc::new(LocalDirBackend::open(dir.path()).unwrap()));
        dm.create_relation(REL, FENCE).unwrap();
        let blk = dm.extend(REL, FENCE).unwrap();
        dm.write_page(REL, blk, &mut heap_page(b"durable", 1), FENCE)
            .unwrap();
        dm.sync_relation(REL).unwrap();
    }
    let dm = DiskManager::new(Arc::new(LocalDirBackend::open(dir.path()).unwrap()));
    let mut buf = page_buf();
    dm.read_page(REL, BlockNumber(0), &mut buf).unwrap();
    let p = SlottedPage::open(&buf[..]).unwrap();
    assert_eq!(p.get_item(SlotId(0)), Some(&b"durable"[..]));
}

#[test]
fn detects_on_disk_corruption() {
    let e = env();
    let blk = e.dm.extend(REL, FENCE).unwrap();
    let mut page = heap_page(b"payload", 5);
    e.dm.write_page(REL, blk, &mut page, FENCE).unwrap();

    // Flip one byte behind the disk manager's back.
    page[PAGE_SIZE - 3] ^= 0x01;
    e.backend
        .write_at(FileKey::Relation(REL), blk.byte_offset(), &page, FENCE)
        .unwrap();
    let mut buf = page_buf();
    assert!(matches!(
        e.dm.read_page(REL, blk, &mut buf),
        Err(StorageError::ChecksumMismatch { block: 0, .. })
    ));
}

#[test]
fn detects_misplaced_page() {
    let e = env();
    e.dm.extend(REL, FENCE).unwrap();
    e.dm.extend(REL, FENCE).unwrap();
    let mut page = heap_page(b"block zero", 1);
    e.dm.write_page(REL, BlockNumber(0), &mut page, FENCE)
        .unwrap();

    // Copy block 0's bytes (with its checksum) into block 1.
    e.backend
        .write_at(
            FileKey::Relation(REL),
            BlockNumber(1).byte_offset(),
            &page,
            FENCE,
        )
        .unwrap();
    let mut buf = page_buf();
    assert!(matches!(
        e.dm.read_page(REL, BlockNumber(1), &mut buf),
        Err(StorageError::ChecksumMismatch { block: 1, .. })
    ));
}

#[test]
fn rejects_bad_header_with_valid_checksum() {
    let e = env();
    let blk = e.dm.extend(REL, FENCE).unwrap();
    let mut page = page_buf();
    page[0] = 1; // non-zero, but no valid header (version/kind are 0)
    set_checksum(&mut page, blk).unwrap();
    e.backend
        .write_at(FileKey::Relation(REL), blk.byte_offset(), &page, FENCE)
        .unwrap();
    let mut buf = page_buf();
    assert!(matches!(
        e.dm.read_page(REL, blk, &mut buf),
        Err(StorageError::CorruptPage(_))
    ));
}

#[test]
fn zero_page_is_written_without_checksum() {
    let e = env();
    let blk = e.dm.extend(REL, FENCE).unwrap();
    let mut zero = page_buf();
    e.dm.write_page(REL, blk, &mut zero, FENCE).unwrap();
    assert!(is_new_page(&zero));
    let mut buf = page_buf();
    e.dm.read_page(REL, blk, &mut buf).unwrap();
    assert!(is_new_page(&buf));
}

#[test]
fn rejects_partial_page_file() {
    let e = env();
    let half = AlignedBuf::new(IO_ALIGN).unwrap();
    e.backend
        .write_at(FileKey::Relation(REL), 0, &half, FENCE)
        .unwrap();
    assert!(matches!(
        e.dm.nblocks(REL),
        Err(StorageError::InvalidFileLength { len, .. }) if len == IO_ALIGN as u64
    ));
}

#[test]
fn rejects_wrong_buffer_length() {
    let e = env();
    e.dm.extend(REL, FENCE).unwrap();
    let mut small = AlignedBuf::new(IO_ALIGN).unwrap();
    assert!(matches!(
        e.dm.read_page(REL, BlockNumber(0), &mut small),
        Err(StorageError::InvalidPageLength { .. })
    ));
    assert!(matches!(
        e.dm.write_page(REL, BlockNumber(0), &mut small, FENCE),
        Err(StorageError::InvalidPageLength { .. })
    ));
}

#[test]
fn reading_beyond_end_fails() {
    let e = env();
    let mut buf = page_buf();
    assert!(matches!(
        e.dm.read_page(REL, BlockNumber(0), &mut buf),
        Err(StorageError::ShortRead { .. })
    ));
}

#[test]
fn concurrent_extend_yields_unique_blocks() {
    let e = env();
    let dm = &e.dm;
    let mut blocks: Vec<u32> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                s.spawn(|| {
                    (0..25)
                        .map(|_| dm.extend(REL, FENCE).unwrap().0)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });
    blocks.sort_unstable();
    assert_eq!(blocks, (0..100).collect::<Vec<_>>());
    assert_eq!(dm.nblocks(REL).unwrap(), 100);
}

#[test]
fn remove_relation() {
    let e = env();
    e.dm.extend(REL, FENCE).unwrap();
    e.dm.remove_relation(REL, FENCE).unwrap();
    assert!(e.dm.nblocks(REL).is_err());
}
