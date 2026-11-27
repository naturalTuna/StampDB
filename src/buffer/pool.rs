//! Buffer pool core: frames, mapping table, pin/unpin, page loading,
//! clock-sweep replacement, and the flush path.
//!
//! Frames that have never held a page (or lost it after a failed read) are
//! kept on a free list. When it is empty, clock-sweep picks a victim: an
//! unpinned frame whose usage count has decayed to zero.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Condvar, Mutex, RwLock};

use super::control::FlushControl;
use super::guard::PinGuard;
use super::{BufferError, Result};
use crate::storage::AlignedBuf;
use crate::storage::backend::RelFileId;
use crate::storage::disk::DiskManager;
use crate::storage::page::{BlockNumber, PAGE_SIZE, page_lsn};

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

/// Result of a flush request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// The page was written.
    Written,
    /// Nothing to write (clean or not resident).
    Clean,
    /// [`FlushControl::may_flush`] refused; the page stays dirty.
    Deferred,
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

/// Outcome of trying to map a new tag onto a pinned frame.
enum Replace {
    Installed,
    /// The new tag was mapped by another thread meanwhile.
    AlreadyPresent,
    /// The victim was pinned or dirtied again meanwhile.
    VictimBusy,
}

type Partition = HashMap<BufferTag, FrameId>;

/// Fixed-size cache of pages over a [`DiskManager`].
#[derive(Debug)]
pub struct BufferPool {
    frames: Box<[Frame]>,
    table: [RwLock<Partition>; NUM_PARTITIONS],
    free_list: Mutex<Vec<FrameId>>,
    clock_hand: AtomicUsize,
    disk: Arc<DiskManager>,
    control: Arc<dyn FlushControl>,
}

impl BufferPool {
    /// Allocates a pool of `nframes` pages.
    pub fn new(
        nframes: usize,
        disk: Arc<DiskManager>,
        control: Arc<dyn FlushControl>,
    ) -> Result<Self> {
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
            clock_hand: AtomicUsize::new(0),
            disk,
            control,
        })
    }

    /// Number of frames.
    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Pins the page `tag`, reading it from disk if it is not resident.
    pub fn fetch(&self, tag: BufferTag) -> Result<PinGuard<'_>> {
        loop {
            if let Some(fid) = self.pin_resident(tag, true) {
                if self.wait_until_loaded(fid, tag) {
                    return Ok(PinGuard::new(self, fid, tag));
                }
                // The loader failed; retry from scratch.
                self.unpin(fid);
                continue;
            }

            let (fid, old) = self.get_victim()?;
            match self.replace(fid, old, tag) {
                Replace::Installed => {}
                Replace::AlreadyPresent | Replace::VictimBusy => {
                    self.unpin(fid);
                    continue;
                }
            }
            self.load(tag, fid)?;
            return Ok(PinGuard::new(self, fid, tag));
        }
    }

    /// Writes the page if it is resident and dirty. Not durable until the
    /// relation is synced.
    ///
    /// Must not be called while this thread holds the page's content lock.
    pub fn flush_page(&self, tag: BufferTag) -> Result<FlushOutcome> {
        let Some(fid) = self.pin_resident(tag, false) else {
            return Ok(FlushOutcome::Clean);
        };
        let res = self.flush_frame(fid);
        self.unpin(fid);
        res
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

    fn partition_index(&self, tag: BufferTag) -> usize {
        let mut hasher = DefaultHasher::new();
        tag.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_PARTITIONS
    }

    fn partition(&self, tag: BufferTag) -> &RwLock<Partition> {
        &self.table[self.partition_index(tag)]
    }

    /// If `tag` is mapped, pins its frame (bumping the usage count if
    /// `touch`). The partition lock is held while pinning so the frame
    /// cannot be remapped in between.
    fn pin_resident(&self, tag: BufferTag, touch: bool) -> Option<FrameId> {
        let part = self.partition(tag).read();
        let fid = *part.get(&tag)?;
        let mut h = self.frame(fid).hdr.lock();
        h.pin_count += 1;
        if touch {
            h.usage_count = (h.usage_count + 1).min(MAX_USAGE_COUNT);
        }
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

    /// Returns a pinned frame that can take a new page, along with the tag
    /// it currently holds (`None` for a free frame). A dirty victim has been
    /// written back already.
    fn get_victim(&self) -> Result<(FrameId, Option<BufferTag>)> {
        if let Some(fid) = self.free_list.lock().pop() {
            self.frame(fid).hdr.lock().pin_count = 1;
            return Ok((fid, None));
        }

        // Clock-sweep. Enough steps for every unpinned frame's usage count
        // to decay from the maximum to zero, plus one more round.
        let n = self.frames.len();
        for _ in 0..n * (usize::from(MAX_USAGE_COUNT) + 2) {
            let fid = FrameId(self.clock_hand.fetch_add(1, Ordering::Relaxed) % n);
            let mut h = self.frame(fid).hdr.lock();
            // Untagged unpinned frames are on the free list; leave them there.
            if h.pin_count > 0 || h.io_in_progress || h.tag.is_none() {
                continue;
            }
            if h.usage_count > 0 {
                h.usage_count -= 1;
                continue;
            }
            h.pin_count = 1;
            let (tag, dirty) = (h.tag, h.dirty);
            drop(h);

            if dirty {
                match self.flush_frame(fid) {
                    Ok(FlushOutcome::Written | FlushOutcome::Clean) => {}
                    Ok(FlushOutcome::Deferred) => {
                        self.unpin(fid);
                        continue;
                    }
                    Err(e) => {
                        self.unpin(fid);
                        return Err(e);
                    }
                }
            }
            return Ok((fid, tag));
        }
        Err(BufferError::Exhausted)
    }

    /// Maps `new` onto the pinned frame `fid`, unmapping `old` if given.
    ///
    /// Both partitions are locked (ascending order) so the switch is atomic:
    /// if `new` turns out to be present already, the victim keeps its page.
    fn replace(&self, fid: FrameId, old: Option<BufferTag>, new: BufferTag) -> Replace {
        let pn = self.partition_index(new);
        let po = old.map_or(pn, |t| self.partition_index(t));
        let (lo, hi) = (pn.min(po), pn.max(po));
        let mut g_lo = self.table[lo].write();
        let mut g_hi = (hi != lo).then(|| self.table[hi].write());

        if pick(&mut g_lo, g_hi.as_deref_mut(), hi, pn).contains_key(&new) {
            return Replace::AlreadyPresent;
        }
        let mut h = self.frame(fid).hdr.lock();
        if let Some(old) = old {
            if h.pin_count != 1 || h.dirty || h.tag != Some(old) {
                return Replace::VictimBusy;
            }
            pick(&mut g_lo, g_hi.as_deref_mut(), hi, po).remove(&old);
        }
        pick(&mut g_lo, g_hi.as_deref_mut(), hi, pn).insert(new, fid);
        h.tag = Some(new);
        h.valid = false;
        h.dirty = false;
        h.io_in_progress = true;
        h.usage_count = 1;
        Replace::Installed
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

    /// Writes a pinned frame back if it is dirty.
    ///
    /// The page is copied under the shared content lock and the checksum is
    /// stamped on the copy: hint bits may change under a shared lock, so
    /// checksumming the live page could write a torn checksum (PostgreSQL's
    /// `PageSetChecksumCopy`). `dirty` is cleared before the copy, so any
    /// change made after it marks the page dirty again.
    fn flush_frame(&self, fid: FrameId) -> Result<FlushOutcome> {
        let frame = self.frame(fid);
        let page = frame.content.read();
        let tag = {
            let h = frame.hdr.lock();
            match h.tag {
                Some(t) if h.valid && h.dirty => t,
                _ => return Ok(FlushOutcome::Clean),
            }
        };
        if !self.control.may_flush(tag, page_lsn(&page)) {
            return Ok(FlushOutcome::Deferred);
        }
        // WAL step: flush WAL up to the page LSN here before writing.
        let mut copy = AlignedBuf::new(PAGE_SIZE)?;
        frame.hdr.lock().dirty = false;
        copy.copy_from_slice(&page);
        drop(page);

        let fence = self.control.fence_token();
        if let Err(e) = self.disk.write_page(tag.rel, tag.blk, &mut copy, fence) {
            frame.hdr.lock().dirty = true;
            return Err(e.into());
        }
        Ok(FlushOutcome::Written)
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

/// Selects the partition `idx` among the (one or two) locked partitions.
fn pick<'g>(
    lo: &'g mut Partition,
    hi: Option<&'g mut Partition>,
    hi_idx: usize,
    idx: usize,
) -> &'g mut Partition {
    match hi {
        Some(g) if idx == hi_idx => g,
        _ => lo,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::control::SingleModeFlush;
    use crate::storage::backend::{FenceToken, FileKey};
    use crate::storage::checksum::verify_checksum;
    use crate::storage::mem::MemBackend;
    use crate::storage::page::{Lsn, PageKind};
    use crate::storage::slotted::{SlotId, SlottedPage};
    use crate::storage::{StorageError, page};
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;

    const REL: RelFileId = RelFileId(1);
    const FENCE: FenceToken = FenceToken::new(1);

    fn tag(blk: u32) -> BufferTag {
        BufferTag {
            rel: REL,
            blk: BlockNumber(blk),
        }
    }

    /// Flush control whose permission can be toggled.
    #[derive(Debug)]
    struct Gate {
        allow: AtomicBool,
        fence: FenceToken,
    }

    impl FlushControl for Gate {
        fn may_flush(&self, _tag: BufferTag, _lsn: Lsn) -> bool {
            self.allow.load(Ordering::SeqCst)
        }
        fn fence_token(&self) -> FenceToken {
            self.fence
        }
    }

    /// Relation with `nblocks` heap pages; block `b` holds one item `[b; 16]`.
    fn setup_with(
        nframes: usize,
        nblocks: u32,
        control: Arc<dyn FlushControl>,
    ) -> (Arc<MemBackend>, Arc<DiskManager>, BufferPool) {
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
        let pool = BufferPool::new(nframes, disk.clone(), control).unwrap();
        (mem, disk, pool)
    }

    fn setup(nframes: usize, nblocks: u32) -> (Arc<MemBackend>, BufferPool) {
        let (mem, _disk, pool) =
            setup_with(nframes, nblocks, Arc::new(SingleModeFlush::new(FENCE)));
        (mem, pool)
    }

    fn gate(allow: bool, fence: u64) -> Arc<Gate> {
        Arc::new(Gate {
            allow: AtomicBool::new(allow),
            fence: FenceToken::new(fence),
        })
    }

    fn item(page: &[u8]) -> Vec<u8> {
        SlottedPage::open(page)
            .unwrap()
            .get_item(SlotId(0))
            .unwrap()
            .to_vec()
    }

    /// Overwrites the first byte of the page's item and marks it dirty.
    fn modify(pool: &BufferPool, blk: u32, byte: u8) {
        let pin = pool.fetch(tag(blk)).unwrap();
        let mut w = pin.write();
        SlottedPage::open(&mut w[..])
            .unwrap()
            .get_item_mut(SlotId(0))
            .unwrap()[0] = byte;
        w.mark_dirty();
    }

    fn on_disk(mem: &MemBackend, blk: u32) -> Vec<u8> {
        mem.raw(
            FileKey::Relation(REL),
            BlockNumber(blk).byte_offset() as usize,
            PAGE_SIZE,
        )
    }

    #[test]
    fn rejects_empty_pool() {
        let mem = Arc::new(MemBackend::default());
        let disk = Arc::new(DiskManager::new(mem));
        assert!(matches!(
            BufferPool::new(0, disk, Arc::new(SingleModeFlush::new(FENCE))),
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
    fn evicts_unpinned_pages() {
        let (mem, pool) = setup(2, 5);
        let reads = mem.reads();
        for b in 0..5 {
            assert_eq!(item(&pool.fetch(tag(b)).unwrap().read()), vec![b as u8; 16]);
        }
        assert_eq!(mem.reads(), reads + 5);
        assert_eq!(pool.pin_count(tag(0)), None, "block 0 was evicted");
        assert_eq!(item(&pool.fetch(tag(0)).unwrap().read()), vec![0u8; 16]);
        assert_eq!(mem.reads(), reads + 6);
    }

    #[test]
    fn hot_page_survives_sweep() {
        let (_mem, pool) = setup(3, 6);
        for _ in 0..MAX_USAGE_COUNT {
            drop(pool.fetch(tag(0)).unwrap());
        }
        // Fewer misses than it takes to decay a usage count of 5.
        for b in 1..5 {
            drop(pool.fetch(tag(b)).unwrap());
        }
        assert!(pool.pin_count(tag(0)).is_some(), "hot page stays resident");
    }

    #[test]
    fn pinned_page_is_never_evicted() {
        let (_mem, pool) = setup(2, 6);
        let pinned = pool.fetch(tag(0)).unwrap();
        for b in 1..6 {
            drop(pool.fetch(tag(b)).unwrap());
        }
        assert_eq!(pool.pin_count(tag(0)), Some(1));
        assert_eq!(item(&pinned.read()), vec![0u8; 16]);
    }

    #[test]
    fn dirty_victim_is_written_back() {
        let (mem, pool) = setup(1, 2);
        modify(&pool, 0, 0xAB);
        drop(pool.fetch(tag(1)).unwrap()); // evicts block 0
        assert_eq!(pool.pin_count(tag(0)), None);
        let disk_page = on_disk(&mem, 0);
        verify_checksum(&disk_page, BlockNumber(0)).unwrap();
        assert_eq!(item(&disk_page)[0], 0xAB);
        assert_eq!(item(&pool.fetch(tag(0)).unwrap().read())[0], 0xAB);
    }

    #[test]
    fn flush_page_writes_copy_with_checksum() {
        let (mem, pool) = setup(2, 1);
        modify(&pool, 0, 0x5A);
        let pin = pool.fetch(tag(0)).unwrap();
        let live_checksum = pin.read()[8..12].to_vec();

        assert_eq!(pool.flush_page(tag(0)).unwrap(), FlushOutcome::Written);
        assert_eq!(pool.is_dirty(tag(0)), Some(false));
        assert_eq!(pool.flush_page(tag(0)).unwrap(), FlushOutcome::Clean);
        assert_eq!(pool.flush_page(tag(9)).unwrap(), FlushOutcome::Clean);

        let disk_page = on_disk(&mem, 0);
        verify_checksum(&disk_page, BlockNumber(0)).unwrap();
        assert_eq!(item(&disk_page)[0], 0x5A);
        // The checksum was stamped on a copy, not on the cached page.
        assert_eq!(pin.read()[8..12], live_checksum[..]);
        assert_ne!(disk_page[8..12], live_checksum[..]);
    }

    #[test]
    fn flush_passes_fence_token() {
        let (mem, _disk, pool) = setup_with(1, 2, gate(true, 42));
        modify(&pool, 0, 1);
        pool.flush_page(tag(0)).unwrap();
        modify(&pool, 0, 2);
        drop(pool.fetch(tag(1)).unwrap()); // eviction write
        let page_writes: Vec<_> = mem.fences().into_iter().skip(1 + 2 * 2).collect();
        assert_eq!(page_writes, vec![FenceToken::new(42); 2]);
    }

    #[test]
    fn deferred_flush_blocks_eviction() {
        let g = gate(false, 1);
        let (mem, _disk, pool) = setup_with(1, 2, g.clone());
        modify(&pool, 0, 0xEE);
        let writes = mem.writes();

        assert_eq!(pool.flush_page(tag(0)).unwrap(), FlushOutcome::Deferred);
        assert_eq!(pool.is_dirty(tag(0)), Some(true));
        assert!(matches!(pool.fetch(tag(1)), Err(BufferError::Exhausted)));
        assert_eq!(mem.writes(), writes);

        g.allow.store(true, Ordering::SeqCst);
        drop(pool.fetch(tag(1)).unwrap());
        assert_eq!(item(&on_disk(&mem, 0))[0], 0xEE);
    }

    #[test]
    fn failed_write_keeps_page_dirty() {
        let (_mem, disk, pool) = setup_with(2, 1, Arc::new(SingleModeFlush::new(FENCE)));
        modify(&pool, 0, 3);
        disk.remove_relation(REL, FENCE).unwrap();
        assert!(pool.flush_page(tag(0)).is_err());
        assert_eq!(pool.is_dirty(tag(0)), Some(true));
    }

    #[test]
    fn concurrent_fetch_reads_once() {
        // Each racing miss pins one frame before the install race is decided,
        // so the pool needs at least as many frames as concurrent fetchers.
        let n = 8;
        let (mem, pool) = setup(n, 1);
        let reads = mem.reads();
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
    fn concurrent_fetch_with_eviction() {
        let (_mem, pool) = setup(8, 32);
        std::thread::scope(|s| {
            for t in 0..4u32 {
                let pool = &pool;
                s.spawn(move || {
                    for i in 0..500u32 {
                        let b = (i * 13 + t * 5) % 32;
                        let pin = pool
                            .fetch(tag(b))
                            .unwrap_or_else(|e| panic!("fetch {b}: {e:?}"));
                        assert_eq!(item(&pin.read()), vec![b as u8; 16]);
                    }
                });
            }
        });
    }
}
