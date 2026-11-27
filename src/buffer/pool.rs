//! Buffer pool core: frames, mapping table, pin/unpin, and page loading.
//!
//! Frames not yet holding any page are kept on a free list. Replacement of
//! resident pages (clock-sweep) is added on top of this.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex, RwLock};

use super::guard::PinGuard;
use super::{BufferError, Result};
use crate::storage::AlignedBuf;
use crate::storage::backend::RelFileId;
use crate::storage::disk::DiskManager;
use crate::storage::page::{BlockNumber, PAGE_SIZE};

/// Number of mapping table partitions (PostgreSQL uses 128).
pub const NUM_PARTITIONS: usize = 16;

/// Upper bound of a frame's usage count (PostgreSQL's BM_MAX_USAGE_COUNT).
pub const MAX_USAGE_COUNT: u8 = 5;

/// Identity of a page: relation file and block number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferTag {
    pub rel: RelFileId,
    pub blk: BlockNumber,
}

/// Index of a frame in the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FrameId(usize);

/// Per-frame bookkeeping, protected by the frame header lock.
#[derive(Debug, Default)]
pub(super) struct FrameHeader {
    /// Page held by the frame; `None` while free.
    tag: Option<BufferTag>,
    pin_count: u32,
    usage_count: u8,
    /// Modified since last written.
    pub(super) dirty: bool,
    /// Page bytes are loaded and usable.
    valid: bool,
    /// A thread is reading the page in; others wait on `io_done`.
    io_in_progress: bool,
}

/// One buffer slot.
#[derive(Debug)]
pub(super) struct Frame {
    pub(super) content: RwLock<AlignedBuf>,
    pub(super) hdr: Mutex<FrameHeader>,
    io_done: Condvar,
}

/// Fixed-size cache of pages over a [`DiskManager`].
#[derive(Debug)]
pub struct BufferPool {
    frames: Box<[Frame]>,
    table: [RwLock<HashMap<BufferTag, FrameId>>; NUM_PARTITIONS],
    free_list: Mutex<Vec<FrameId>>,
    disk: Arc<DiskManager>,
}

impl BufferPool {
    /// Allocates a pool of `nframes` pages.
    pub fn new(nframes: usize, disk: Arc<DiskManager>) -> Result<Self> {
        if nframes == 0 {
            return Err(BufferError::InvalidPoolSize);
        }
        let frames = (0..nframes)
            .map(|_| {
                Ok(Frame {
                    content: RwLock::new(AlignedBuf::new(PAGE_SIZE)?),
                    hdr: Mutex::new(FrameHeader::default()),
                    io_done: Condvar::new(),
                })
            })
            .collect::<Result<Box<[Frame]>>>()?;
        // Pop order: frame 0 first.
        let free_list = (0..nframes).rev().map(FrameId).collect();
        Ok(Self {
            frames,
            table: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            free_list: Mutex::new(free_list),
            disk,
        })
    }

    /// Number of frames.
    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Pins the page `tag`, reading it from disk if it is not resident.
    pub fn fetch(&self, tag: BufferTag) -> Result<PinGuard<'_>> {
        loop {
            if let Some(fid) = self.pin_resident(tag) {
                if self.wait_until_loaded(fid, tag) {
                    return Ok(PinGuard::new(self, fid, tag));
                }
                // The loader failed; retry from scratch.
                self.unpin(fid);
                continue;
            }

            let fid = self.allocate_frame()?;
            if !self.install(tag, fid) {
                // Another thread mapped `tag` first; use its frame.
                self.unpin(fid);
                continue;
            }
            self.load(tag, fid)?;
            return Ok(PinGuard::new(self, fid, tag));
        }
    }

    /// Pin count of a resident page (diagnostics and tests).
    pub fn pin_count(&self, tag: BufferTag) -> Option<u32> {
        self.with_resident_header(tag, |h| h.pin_count)
    }

    /// Dirty flag of a resident page (diagnostics and tests).
    pub fn is_dirty(&self, tag: BufferTag) -> Option<bool> {
        self.with_resident_header(tag, |h| h.dirty)
    }

    pub(super) fn frame(&self, fid: FrameId) -> &Frame {
        &self.frames[fid.0]
    }

    /// Releases one pin. A frame that holds no page goes back to the free
    /// list when its last pin is released.
    pub(super) fn unpin(&self, fid: FrameId) {
        let mut h = self.frame(fid).hdr.lock();
        debug_assert!(h.pin_count > 0, "unpin of an unpinned frame");
        h.pin_count = h.pin_count.saturating_sub(1);
        let reusable = h.pin_count == 0 && h.tag.is_none();
        drop(h);
        if reusable {
            self.free_list.lock().push(fid);
        }
    }

    fn partition(&self, tag: BufferTag) -> &RwLock<HashMap<BufferTag, FrameId>> {
        let mut hasher = DefaultHasher::new();
        tag.hash(&mut hasher);
        &self.table[(hasher.finish() as usize) % NUM_PARTITIONS]
    }

    /// If `tag` is mapped, pins its frame and bumps the usage count.
    /// The partition lock is held while pinning so the frame cannot be
    /// remapped in between.
    fn pin_resident(&self, tag: BufferTag) -> Option<FrameId> {
        let part = self.partition(tag).read();
        let fid = *part.get(&tag)?;
        let mut h = self.frame(fid).hdr.lock();
        h.pin_count += 1;
        h.usage_count = (h.usage_count + 1).min(MAX_USAGE_COUNT);
        Some(fid)
    }

    /// Waits for an in-progress read of a pinned frame. Returns whether the
    /// frame now validly holds `tag`.
    fn wait_until_loaded(&self, fid: FrameId, tag: BufferTag) -> bool {
        let frame = self.frame(fid);
        let mut h = frame.hdr.lock();
        while h.io_in_progress {
            frame.io_done.wait(&mut h);
        }
        h.valid && h.tag == Some(tag)
    }

    /// Takes a frame that holds no page and returns it pinned.
    fn allocate_frame(&self) -> Result<FrameId> {
        let fid = self.free_list.lock().pop().ok_or(BufferError::Exhausted)?;
        self.frame(fid).hdr.lock().pin_count = 1;
        Ok(fid)
    }

    /// Maps `tag` to the pinned, unmapped frame `fid` and marks it as being
    /// read. Returns false if `tag` is already mapped.
    fn install(&self, tag: BufferTag, fid: FrameId) -> bool {
        let mut part = self.partition(tag).write();
        if part.contains_key(&tag) {
            return false;
        }
        part.insert(tag, fid);
        let mut h = self.frame(fid).hdr.lock();
        h.tag = Some(tag);
        h.valid = false;
        h.dirty = false;
        h.io_in_progress = true;
        h.usage_count = 1;
        true
    }

    /// Reads `tag` into the installed frame `fid`. On failure, unmaps the
    /// frame, wakes waiters, and drops the caller's pin.
    fn load(&self, tag: BufferTag, fid: FrameId) -> Result<()> {
        let frame = self.frame(fid);
        let res = {
            let mut page = frame.content.write();
            self.disk.read_page(tag.rel, tag.blk, &mut page)
        };
        match res {
            Ok(()) => {
                let mut h = frame.hdr.lock();
                h.valid = true;
                h.io_in_progress = false;
                drop(h);
                frame.io_done.notify_all();
                Ok(())
            }
            Err(e) => {
                {
                    let mut part = self.partition(tag).write();
                    if part.get(&tag) == Some(&fid) {
                        part.remove(&tag);
                    }
                    let mut h = frame.hdr.lock();
                    h.tag = None;
                    h.valid = false;
                    h.io_in_progress = false;
                    h.usage_count = 0;
                }
                frame.io_done.notify_all();
                self.unpin(fid);
                Err(e.into())
            }
        }
    }

    fn with_resident_header<T>(
        &self,
        tag: BufferTag,
        f: impl FnOnce(&FrameHeader) -> T,
    ) -> Option<T> {
        let part = self.partition(tag).read();
        let fid = *part.get(&tag)?;
        let h = self.frame(fid).hdr.lock();
        Some(f(&h))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::{FenceToken, FileKey};
    use crate::storage::mem::MemBackend;
    use crate::storage::page::PageKind;
    use crate::storage::slotted::{SlotId, SlottedPage};
    use crate::storage::{StorageError, page};
    use std::sync::Barrier;

    const REL: RelFileId = RelFileId(1);
    const FENCE: FenceToken = FenceToken::new(1);

    fn tag(blk: u32) -> BufferTag {
        BufferTag {
            rel: REL,
            blk: BlockNumber(blk),
        }
    }

    /// Relation with `nblocks` heap pages; block `b` holds one item `[b; 16]`.
    fn setup(nframes: usize, nblocks: u32) -> (Arc<MemBackend>, BufferPool) {
        let mem = Arc::new(MemBackend::default());
        let disk = Arc::new(DiskManager::new(mem.clone()));
        disk.create_relation(REL, FENCE).unwrap();
        for b in 0..nblocks {
            let blk = disk.extend(REL, FENCE).unwrap();
            let mut p =
                SlottedPage::init(AlignedBuf::new(PAGE_SIZE).unwrap(), PageKind::Heap, 0).unwrap();
            p.add_item(&[b as u8; 16]).unwrap().unwrap();
            let mut buf = p.into_inner();
            disk.write_page(REL, blk, &mut buf, FENCE).unwrap();
        }
        let pool = BufferPool::new(nframes, disk).unwrap();
        (mem, pool)
    }

    fn item(page: &[u8]) -> Vec<u8> {
        SlottedPage::open(page)
            .unwrap()
            .get_item(SlotId(0))
            .unwrap()
            .to_vec()
    }

    #[test]
    fn rejects_empty_pool() {
        let mem = Arc::new(MemBackend::default());
        let disk = Arc::new(DiskManager::new(mem));
        assert!(matches!(
            BufferPool::new(0, disk),
            Err(BufferError::InvalidPoolSize)
        ));
    }

    #[test]
    fn miss_then_hit() {
        let (mem, pool) = setup(4, 3);
        let reads = mem.reads();
        {
            let pin = pool.fetch(tag(2)).unwrap();
            assert_eq!(item(&pin.read()), vec![2u8; 16]);
        }
        assert_eq!(mem.reads(), reads + 1);
        {
            let pin = pool.fetch(tag(2)).unwrap();
            assert_eq!(item(&pin.read()), vec![2u8; 16]);
            assert_eq!(pin.tag(), tag(2));
        }
        assert_eq!(mem.reads(), reads + 1, "second fetch must hit");
    }

    #[test]
    fn pin_counts_follow_guards() {
        let (_mem, pool) = setup(4, 1);
        assert_eq!(pool.pin_count(tag(0)), None);
        let a = pool.fetch(tag(0)).unwrap();
        let b = pool.fetch(tag(0)).unwrap();
        assert_eq!(pool.pin_count(tag(0)), Some(2));
        drop(a);
        assert_eq!(pool.pin_count(tag(0)), Some(1));
        drop(b);
        assert_eq!(pool.pin_count(tag(0)), Some(0));
    }

    #[test]
    fn write_and_mark_dirty() {
        let (_mem, pool) = setup(4, 1);
        let pin = pool.fetch(tag(0)).unwrap();
        {
            let mut w = pin.write();
            let mut p = SlottedPage::open(&mut w[..]).unwrap();
            p.get_item_mut(SlotId(0)).unwrap()[0] = 0xAA;
            assert_eq!(pool.is_dirty(tag(0)), Some(false));
            w.mark_dirty();
        }
        assert_eq!(pool.is_dirty(tag(0)), Some(true));
        assert_eq!(item(&pin.read())[0], 0xAA);
        // Shared locks can be held together.
        let (r1, r2) = (pin.read(), pin.read());
        assert_eq!(r1[..], r2[..]);
    }

    #[test]
    fn exhausted_when_all_frames_pinned() {
        let (_mem, pool) = setup(2, 3);
        let _a = pool.fetch(tag(0)).unwrap();
        let _b = pool.fetch(tag(1)).unwrap();
        assert!(matches!(pool.fetch(tag(2)), Err(BufferError::Exhausted)));
        // Resident pages are still reachable.
        assert!(pool.fetch(tag(0)).is_ok());
    }

    #[test]
    fn failed_read_releases_frame() {
        let (mem, pool) = setup(1, 2);
        let file = FileKey::Relation(REL);
        let off = BlockNumber(0).byte_offset() as usize + page::PAGE_SIZE - 1;
        mem.corrupt(file, off);
        assert!(matches!(
            pool.fetch(tag(0)),
            Err(BufferError::Storage(StorageError::ChecksumMismatch { .. }))
        ));
        assert_eq!(pool.pin_count(tag(0)), None);
        // The only frame is free again.
        assert_eq!(item(&pool.fetch(tag(1)).unwrap().read()), vec![1u8; 16]);
    }

    #[test]
    fn missing_block_is_an_error() {
        let (_mem, pool) = setup(2, 1);
        assert!(matches!(
            pool.fetch(tag(5)),
            Err(BufferError::Storage(StorageError::ShortRead { .. }))
        ));
        assert!(pool.fetch(tag(0)).is_ok());
    }

    #[test]
    fn concurrent_fetch_reads_once() {
        let (mem, pool) = setup(4, 1);
        let reads = mem.reads();
        let n = 8;
        let barrier = Barrier::new(n);
        std::thread::scope(|s| {
            for _ in 0..n {
                s.spawn(|| {
                    barrier.wait();
                    let pin = pool.fetch(tag(0)).unwrap();
                    assert_eq!(item(&pin.read()), vec![0u8; 16]);
                });
            }
        });
        assert_eq!(mem.reads(), reads + 1);
        assert_eq!(pool.pin_count(tag(0)), Some(0));
    }

    #[test]
    fn concurrent_fetch_of_many_pages() {
        // Without replacement, threads racing to load the same page each take
        // a free frame before one loses the install; keep one spare frame per
        // thread so that transient use never exhausts the free list.
        let (_mem, pool) = setup(16 + 4, 16);
        std::thread::scope(|s| {
            for t in 0..4u32 {
                let pool = &pool;
                s.spawn(move || {
                    for i in 0..200u32 {
                        let b = (i * 7 + t) % 16;
                        let pin = pool
                            .fetch(tag(b))
                            .unwrap_or_else(|e| panic!("fetch {b}: {e:?}"));
                        assert_eq!(item(&pin.read()), vec![b as u8; 16]);
                    }
                });
            }
        });
        for b in 0..16 {
            assert_eq!(pool.pin_count(tag(b)), Some(0));
        }
    }
}
