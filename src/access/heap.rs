//! Heap file: unordered tuples in slotted pages, accessed through the
//! buffer pool.
//!
//! Inserts go to a cached target block (the last block that had room) and
//! extend the relation when it is full. There is no free space map yet, so
//! space freed by vacuum in earlier blocks is not reused until one is added.
//! All-zero pages (extended but never initialized, e.g. after a crash) are
//! initialized on first insert, like PostgreSQL's `PageIsNew` handling.
//!
//! This step has no visibility checks or row locks: concurrent delete or
//! update of the same tuple is prevented by the transaction layer later.
//! Here, deleting or updating a tuple whose xmax is already set is an error.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::tuple::{self, MAX_TUPLE_SIZE, Tid, TupleError, TupleHeader};
use crate::buffer::{BufferError, BufferPool, BufferTag, PageMut};
use crate::storage::StorageError;
use crate::storage::backend::RelFileId;
use crate::storage::page::{BlockNumber, PageKind, is_new_page};
use crate::storage::slotted::{SlotId, SlottedPage};
use crate::txn::{CommandId, Xid};

/// Marker for "no target block yet".
const NO_TARGET: u32 = u32::MAX;

/// Errors from heap operations.
#[derive(Debug, thiserror::Error)]
pub enum HeapError {
    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Tuple(#[from] TupleError),

    #[error("tuple ({}, {}) not found", tid.blk.0, tid.slot.0)]
    TupleNotFound { tid: Tid },

    #[error("tuple ({}, {}) already deleted or updated by xid {}", tid.blk.0, tid.slot.0, xmax.0)]
    AlreadyDeleted { tid: Tid, xmax: Xid },
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, HeapError>;

/// A heap relation.
#[derive(Debug)]
pub struct HeapFile {
    rel: RelFileId,
    pool: Arc<BufferPool>,
    target: AtomicU32,
}

impl HeapFile {
    /// Opens an existing relation file.
    pub fn open(rel: RelFileId, pool: Arc<BufferPool>) -> Result<Self> {
        let n = pool.nblocks(rel)?;
        let target = if n == 0 { NO_TARGET } else { n - 1 };
        Ok(Self {
            rel,
            pool,
            target: AtomicU32::new(target),
        })
    }

    pub fn rel(&self) -> RelFileId {
        self.rel
    }

    /// Inserts a formed tuple and sets its ctid to its own location.
    pub fn insert(&self, tuple: &[u8]) -> Result<Tid> {
        check_size(tuple)?;
        let target = self.target.load(Ordering::Relaxed);
        if target != NO_TARGET {
            let pin = self.pool.fetch(self.tag(BlockNumber(target)))?;
            let mut page = pin.write();
            if let Some(tid) = place(&mut page, BlockNumber(target), tuple)? {
                return Ok(tid);
            }
        }
        // Target full (or none): append a block. Concurrent inserters may
        // each extend; that only costs a little space.
        let (blk, pin) = self.pool.extend(self.rel)?;
        let mut page = pin.write();
        let tid = place(&mut page, blk, tuple)?.ok_or(TupleError::TooLarge {
            len: tuple.len(),
            max: MAX_TUPLE_SIZE,
        })?;
        self.advance_target(blk);
        Ok(tid)
    }

    /// Copies the tuple at `tid`, or `None` if the slot holds no tuple.
    pub fn fetch(&self, tid: Tid) -> Result<Option<Vec<u8>>> {
        let pin = self.pool.fetch(self.tag(tid.blk))?;
        let page = pin.read();
        if is_new_page(&page) {
            return Ok(None);
        }
        let p = SlottedPage::open(&page[..])?;
        Ok(p.get_item(tid.slot).map(<[u8]>::to_vec))
    }

    /// Marks the tuple deleted by `xmax`.
    pub fn delete(&self, tid: Tid, xmax: Xid, cmax: CommandId) -> Result<()> {
        let pin = self.pool.fetch(self.tag(tid.blk))?;
        let mut page = pin.write();
        stamp_xmax(&mut page, tid, xmax, cmax, None)?;
        page.mark_dirty();
        Ok(())
    }

    /// Inserts `new_tuple` as the next version of `tid`, marking the old one
    /// deleted by `xid` with its ctid pointing at the new version.
    ///
    /// The new version goes on the same page when it fits (one page lock
    /// covers both changes); otherwise it is inserted elsewhere first and the
    /// old version is stamped afterwards.
    pub fn update(&self, tid: Tid, new_tuple: &[u8], xid: Xid, cid: CommandId) -> Result<Tid> {
        check_size(new_tuple)?;
        {
            let pin = self.pool.fetch(self.tag(tid.blk))?;
            let mut page = pin.write();
            check_updatable(&page, tid)?;
            if let Some(new_tid) = place(&mut page, tid.blk, new_tuple)? {
                stamp_xmax(&mut page, tid, xid, cid, Some(new_tid))?;
                return Ok(new_tid);
            }
        }
        let new_tid = self.insert(new_tuple)?;
        let pin = self.pool.fetch(self.tag(tid.blk))?;
        let mut page = pin.write();
        stamp_xmax(&mut page, tid, xid, cid, Some(new_tid))?;
        page.mark_dirty();
        Ok(new_tid)
    }

    /// Sequential scan over the blocks that exist now.
    pub fn scan(&self) -> Result<HeapScan<'_>> {
        Ok(HeapScan {
            heap: self,
            nblocks: self.pool.nblocks(self.rel)?,
            next_blk: 0,
            buffered: Vec::new().into_iter(),
        })
    }

    /// Moves the target block forward to `blk` (never backward).
    fn advance_target(&self, blk: BlockNumber) {
        let _ = self
            .target
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                (cur == NO_TARGET || cur < blk.0).then_some(blk.0)
            });
    }

    fn tag(&self, blk: BlockNumber) -> BufferTag {
        BufferTag { rel: self.rel, blk }
    }

    /// Copies the tuples of one page under its shared lock.
    fn read_page(&self, blk: BlockNumber) -> Result<Vec<(Tid, Vec<u8>)>> {
        let pin = self.pool.fetch(self.tag(blk))?;
        let page = pin.read();
        if is_new_page(&page) {
            return Ok(Vec::new());
        }
        let p = SlottedPage::open(&page[..])?;
        Ok((0..p.slot_count())
            .filter_map(|s| {
                let slot = SlotId(s);
                p.get_item(slot).map(|t| (Tid { blk, slot }, t.to_vec()))
            })
            .collect())
    }
}

/// Iterator over all tuples of a heap, page at a time.
#[derive(Debug)]
pub struct HeapScan<'a> {
    heap: &'a HeapFile,
    nblocks: u32,
    next_blk: u32,
    buffered: std::vec::IntoIter<(Tid, Vec<u8>)>,
}

impl Iterator for HeapScan<'_> {
    type Item = Result<(Tid, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(t) = self.buffered.next() {
                return Some(Ok(t));
            }
            if self.next_blk >= self.nblocks {
                return None;
            }
            let blk = BlockNumber(self.next_blk);
            self.next_blk += 1;
            match self.heap.read_page(blk) {
                Ok(tuples) => self.buffered = tuples.into_iter(),
                Err(e) => {
                    self.next_blk = self.nblocks;
                    return Some(Err(e));
                }
            }
        }
    }
}

fn check_size(tuple: &[u8]) -> Result<()> {
    if tuple.len() > MAX_TUPLE_SIZE {
        return Err(TupleError::TooLarge {
            len: tuple.len(),
            max: MAX_TUPLE_SIZE,
        }
        .into());
    }
    TupleHeader::decode(tuple)?;
    Ok(())
}

/// Adds `tuple` to the locked page `blk` (initializing an all-zero page),
/// sets its ctid to itself and marks the page dirty. `None` if it does not
/// fit.
fn place(page: &mut PageMut<'_>, blk: BlockNumber, tuple: &[u8]) -> Result<Option<Tid>> {
    if is_new_page(page) {
        SlottedPage::init(&mut page[..], PageKind::Heap, 0)?;
        page.mark_dirty();
    }
    let mut p = SlottedPage::open(&mut page[..])?;
    let Some(slot) = p.add_item(tuple)? else {
        return Ok(None);
    };
    let tid = Tid { blk, slot };
    if let Some(item) = p.get_item_mut(slot) {
        tuple::set_ctid(item, tid)?;
    }
    page.mark_dirty();
    Ok(Some(tid))
}

/// Fails unless `tid` holds a tuple with no xmax.
fn check_updatable(page: &[u8], tid: Tid) -> Result<()> {
    if is_new_page(page) {
        return Err(HeapError::TupleNotFound { tid });
    }
    let p = SlottedPage::open(page)?;
    let item = p
        .get_item(tid.slot)
        .ok_or(HeapError::TupleNotFound { tid })?;
    let h = TupleHeader::decode(item)?;
    if h.xmax.is_valid() {
        return Err(HeapError::AlreadyDeleted { tid, xmax: h.xmax });
    }
    Ok(())
}

/// Sets xmax/cmax (and ctid, for updates) on the tuple at `tid`.
fn stamp_xmax(
    page: &mut PageMut<'_>,
    tid: Tid,
    xmax: Xid,
    cmax: CommandId,
    new_tid: Option<Tid>,
) -> Result<()> {
    check_updatable(page, tid)?;
    let mut p = SlottedPage::open(&mut page[..])?;
    let item = p
        .get_item_mut(tid.slot)
        .ok_or(HeapError::TupleNotFound { tid })?;
    tuple::set_xmax(item, xmax, cmax)?;
    if let Some(n) = new_tid {
        tuple::set_ctid(item, n)?;
    }
    page.mark_dirty();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tuple::{TupleDesc, deform_tuple, form_tuple};
    use crate::buffer::SingleModeFlush;
    use crate::storage::backend::FenceToken;
    use crate::storage::disk::DiskManager;
    use crate::storage::mem::MemBackend;
    use crate::types::{DataType, Value};
    use std::collections::HashSet;

    const REL: RelFileId = RelFileId(1);
    const FENCE: FenceToken = FenceToken::new(1);

    fn setup(frames: usize) -> (Arc<BufferPool>, HeapFile) {
        let mem = Arc::new(MemBackend::default());
        let disk = Arc::new(DiskManager::new(mem));
        disk.create_relation(REL, FENCE).unwrap();
        let pool =
            Arc::new(BufferPool::new(frames, disk, Arc::new(SingleModeFlush::new(FENCE))).unwrap());
        let heap = HeapFile::open(REL, pool.clone()).unwrap();
        (pool, heap)
    }

    fn desc() -> TupleDesc {
        TupleDesc::new(vec![DataType::Int8, DataType::Text]).unwrap()
    }

    fn row(id: i64, text: &str) -> Vec<u8> {
        form_tuple(
            &desc(),
            &[Value::Int8(id), Value::Text(text.into())],
            Xid(1),
            CommandId(0),
        )
        .unwrap()
    }

    fn id_of(t: &[u8]) -> i64 {
        match deform_tuple(&desc(), t).unwrap()[0] {
            Value::Int8(i) => i,
            ref v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn insert_fetch_roundtrip() {
        let (_pool, heap) = setup(4);
        let tid = heap.insert(&row(7, "seven")).unwrap();
        assert_eq!(tid.blk, BlockNumber(0));
        let t = heap.fetch(tid).unwrap().unwrap();
        assert_eq!(
            deform_tuple(&desc(), &t).unwrap(),
            vec![Value::Int8(7), Value::Text("seven".into())]
        );
        assert_eq!(TupleHeader::decode(&t).unwrap().ctid, tid);
        assert_eq!(
            heap.fetch(Tid {
                blk: tid.blk,
                slot: SlotId(9)
            })
            .unwrap(),
            None
        );
    }

    #[test]
    fn inserts_spill_to_new_pages_and_scan_sees_all() {
        let (pool, heap) = setup(4);
        let text = "x".repeat(500);
        let tids: Vec<Tid> = (0..100)
            .map(|i| heap.insert(&row(i, &text)).unwrap())
            .collect();
        // 100 tuples of ~550 bytes fill pages of 8KB: about 14 per page.
        let nblocks = pool.nblocks(REL).unwrap();
        assert!((7..=8).contains(&nblocks), "nblocks = {nblocks}");
        assert!(tids.windows(2).all(|w| w[0] < w[1]), "appended in order");

        let scanned: Vec<(Tid, i64)> = heap
            .scan()
            .unwrap()
            .map(|r| r.map(|(tid, t)| (tid, id_of(&t))))
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(scanned.len(), 100);
        for (i, (tid, id)) in scanned.iter().enumerate() {
            assert_eq!((*tid, *id), (tids[i], i as i64));
        }
    }

    #[test]
    fn scan_empty_relation() {
        let (_pool, heap) = setup(2);
        assert_eq!(heap.scan().unwrap().count(), 0);
    }

    #[test]
    fn largest_tuple_gets_own_page() {
        let (_pool, heap) = setup(2);
        heap.insert(&row(1, "small")).unwrap();
        let d = TupleDesc::new(vec![DataType::Text]).unwrap();
        let n = MAX_TUPLE_SIZE - tuple::TUPLE_HEADER_SIZE - 4;
        let big = form_tuple(&d, &[Value::Text("z".repeat(n))], Xid(1), CommandId(0)).unwrap();
        let tid = heap.insert(&big).unwrap();
        assert_eq!(
            tid,
            Tid {
                blk: BlockNumber(1),
                slot: SlotId(0)
            }
        );
    }

    #[test]
    fn delete_sets_xmax_once() {
        let (_pool, heap) = setup(2);
        let tid = heap.insert(&row(1, "a")).unwrap();
        heap.delete(tid, Xid(5), CommandId(2)).unwrap();
        let h = TupleHeader::decode(&heap.fetch(tid).unwrap().unwrap()).unwrap();
        assert_eq!((h.xmax, h.cmax), (Xid(5), CommandId(2)));
        assert!(matches!(
            heap.delete(tid, Xid(6), CommandId(0)),
            Err(HeapError::AlreadyDeleted { xmax: Xid(5), .. })
        ));
        let missing = Tid {
            blk: BlockNumber(0),
            slot: SlotId(3),
        };
        assert!(matches!(
            heap.delete(missing, Xid(6), CommandId(0)),
            Err(HeapError::TupleNotFound { .. })
        ));
    }

    #[test]
    fn update_on_same_page_links_versions() {
        let (_pool, heap) = setup(2);
        let old = heap.insert(&row(1, "v1")).unwrap();
        let new = heap
            .update(old, &row(1, "v2"), Xid(9), CommandId(1))
            .unwrap();
        assert_eq!(new.blk, old.blk);
        let oh = TupleHeader::decode(&heap.fetch(old).unwrap().unwrap()).unwrap();
        assert_eq!((oh.xmax, oh.ctid), (Xid(9), new));
        let nt = heap.fetch(new).unwrap().unwrap();
        assert_eq!(TupleHeader::decode(&nt).unwrap().ctid, new);
        assert_eq!(
            deform_tuple(&desc(), &nt).unwrap()[1],
            Value::Text("v2".into())
        );
        assert!(matches!(
            heap.update(old, &row(1, "v3"), Xid(10), CommandId(0)),
            Err(HeapError::AlreadyDeleted { .. })
        ));
    }

    #[test]
    fn update_moves_to_other_page_when_full() {
        let (_pool, heap) = setup(4);
        let big = "b".repeat(3000);
        let old = heap.insert(&row(1, &big)).unwrap();
        heap.insert(&row(2, &big)).unwrap(); // page 0 now nearly full
        let new = heap
            .update(old, &row(1, &big), Xid(3), CommandId(0))
            .unwrap();
        assert_ne!(new.blk, old.blk);
        let oh = TupleHeader::decode(&heap.fetch(old).unwrap().unwrap()).unwrap();
        assert_eq!((oh.xmax, oh.ctid), (Xid(3), new));
    }

    #[test]
    fn uninitialized_page_is_initialized_on_insert() {
        let (pool, _heap) = setup(2);
        drop(pool.extend(REL).unwrap()); // zero page, never initialized
        let heap = HeapFile::open(REL, pool.clone()).unwrap();
        assert_eq!(heap.scan().unwrap().count(), 0);
        let tid = heap.insert(&row(1, "a")).unwrap();
        assert_eq!(tid.blk, BlockNumber(0));
        assert_eq!(heap.scan().unwrap().count(), 1);
    }

    #[test]
    fn concurrent_inserts_are_all_visible_to_scan() {
        let (pool, heap) = setup(8);
        let text = "c".repeat(200);
        std::thread::scope(|s| {
            for t in 0..4i64 {
                let (heap, text) = (&heap, &text);
                s.spawn(move || {
                    for i in 0..500 {
                        heap.insert(&row(t * 1000 + i, text)).unwrap();
                    }
                });
            }
        });
        let ids: HashSet<i64> = heap.scan().unwrap().map(|r| id_of(&r.unwrap().1)).collect();
        assert_eq!(ids.len(), 2000);
        assert!((0..4).all(|t| (0..500).all(|i| ids.contains(&(t * 1000 + i)))));
        pool.flush_all().unwrap();
    }

    #[test]
    fn rejects_malformed_tuple() {
        let (_pool, heap) = setup(2);
        assert!(matches!(
            heap.insert(&[1, 2, 3]),
            Err(HeapError::Tuple(TupleError::Corrupt(_)))
        ));
    }
}
