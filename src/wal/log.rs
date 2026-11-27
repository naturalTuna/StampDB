//! WAL files: inserting records, flushing them durably, and reading them back.
//!
//! An LSN is a byte position in the WAL stream; segment `n` holds positions
//! `[n * segment_size, (n + 1) * segment_size)`. The first record starts at
//! [`FIRST_LSN`] so that LSN 0 can mean "none".
//!
//! Writes use Direct I/O in 4KB blocks. The last, partially filled block is
//! padded with zeros and written again by the next flush (DESIGN: Direct
//! I/O details). Segments are created zero-filled and never recycled, so the
//! end of WAL is the first position where no valid record (length, CRC,
//! `prev_lsn` chain) starts.
//!
//! Concurrency: one insert lock assigns positions and appends to an
//! in-memory buffer; one flush lock serializes writers, so a thread that
//! waited for the flush lock often finds its LSN already durable (group
//! commit). Write or fsync failures abort the process (DESIGN r6).

use std::sync::Arc;

use parking_lot::Mutex;

use super::record::{self, BlockRef, Decoded, HEADER_SIZE, Record, RmgrId};
use super::{Result, WalError, fatal, is_not_found};
use crate::storage::backend::{FenceSource, FileKey, RelFileId, StorageBackend};
use crate::storage::page::{BlockNumber, Lsn, PAGE_SIZE, is_new_page, page_lsn};
use crate::storage::{AlignedBuf, IO_ALIGN};

/// Default segment size (same as PostgreSQL).
pub const DEFAULT_SEGMENT_SIZE: u64 = 16 << 20;

/// Position of the first record.
pub const FIRST_LSN: Lsn = Lsn(32);

/// `info` values of the Xlog resource manager.
pub mod xlog_info {
    /// A checkpoint; main data is the redo LSN (u64 LE).
    pub const CHECKPOINT: u8 = 0;
    /// A full page image taken because only hint bits changed.
    pub const FPI_FOR_HINT: u8 = 1;
}

const BLOCK: u64 = IO_ALIGN as u64;
const ZERO_CHUNK: usize = 1 << 20;

/// WAL layout parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalConfig {
    pub timeline: u32,
    pub segment_size: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            timeline: 1,
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }
}

impl WalConfig {
    fn validate(&self) -> Result<()> {
        if self.segment_size < 2 * BLOCK || !self.segment_size.is_multiple_of(BLOCK) {
            return Err(WalError::InvalidConfig(
                "segment size must be a multiple of 4096 and at least 8192",
            ));
        }
        Ok(())
    }

    fn key(&self, segno: u64) -> FileKey {
        FileKey::Wal {
            timeline: self.timeline,
            segno,
        }
    }
}

/// State protected by the insert lock.
#[derive(Debug)]
struct InsertState {
    /// Where the next record starts.
    next: Lsn,
    /// Start of the last inserted record.
    prev: Lsn,
    /// Redo point of the latest checkpoint. A page whose LSN is not above
    /// it gets a full page image on its next logged change (full page
    /// writes). Changed only under the insert lock, so the decision made
    /// in `insert_fpw` cannot race with a checkpoint starting.
    redo: Lsn,
    /// LSN of `buf[0]`; always block-aligned.
    buf_start: u64,
    /// Bytes from `buf_start` to `next` not yet known durable (the first
    /// block may be durable already but gets rewritten).
    buf: Vec<u8>,
}

/// State protected by the flush lock.
#[derive(Debug)]
struct FlushState {
    flushed: Lsn,
    /// Segments below this number are known to exist.
    created_upto: u64,
}

/// The write side of the WAL.
#[derive(Debug)]
pub struct Wal {
    backend: Arc<dyn StorageBackend>,
    fence: Arc<dyn FenceSource>,
    cfg: WalConfig,
    insert: Mutex<InsertState>,
    flush: Mutex<FlushState>,
}

impl Wal {
    /// Starts a new, empty log (initdb).
    pub fn create(
        backend: Arc<dyn StorageBackend>,
        fence: Arc<dyn FenceSource>,
        cfg: WalConfig,
    ) -> Result<Self> {
        cfg.validate()?;
        Ok(Self::with_state(
            backend,
            fence,
            cfg,
            InsertState {
                next: FIRST_LSN,
                prev: Lsn::INVALID,
                redo: FIRST_LSN,
                buf_start: 0,
                buf: vec![0u8; FIRST_LSN.0 as usize],
            },
            FIRST_LSN,
        ))
    }

    /// Continues an existing log after its last valid record: `end` is where
    /// the next record goes and `prev` is the start of the last record (both
    /// as found by [`WalReader`]); `redo` is the latest checkpoint's redo
    /// point.
    pub fn open_at(
        backend: Arc<dyn StorageBackend>,
        fence: Arc<dyn FenceSource>,
        cfg: WalConfig,
        end: Lsn,
        prev: Lsn,
        redo: Lsn,
    ) -> Result<Self> {
        cfg.validate()?;
        let buf_start = end.0 - end.0 % BLOCK;
        let mut buf = Vec::new();
        if buf_start < end.0 {
            // Keep the valid head of the partial block: it is rewritten.
            let segno = buf_start / cfg.segment_size;
            let mut block = AlignedBuf::new(IO_ALIGN)?;
            backend.read_at(cfg.key(segno), buf_start % cfg.segment_size, &mut block)?;
            buf.extend_from_slice(&block[..(end.0 - buf_start) as usize]);
        }
        Ok(Self::with_state(
            backend,
            fence,
            cfg,
            InsertState {
                next: end,
                prev,
                redo,
                buf_start,
                buf,
            },
            end,
        ))
    }

    fn with_state(
        backend: Arc<dyn StorageBackend>,
        fence: Arc<dyn FenceSource>,
        cfg: WalConfig,
        insert: InsertState,
        flushed: Lsn,
    ) -> Self {
        Self {
            backend,
            fence,
            cfg,
            insert: Mutex::new(insert),
            flush: Mutex::new(FlushState {
                flushed,
                created_upto: 0,
            }),
        }
    }

    pub fn config(&self) -> WalConfig {
        self.cfg
    }

    /// Appends a record. Returns its start and end LSN; the end is what a
    /// modified page's LSN is set to. Not durable until [`Wal::flush_to`].
    pub fn insert(&self, rec: &Record) -> Result<(Lsn, Lsn)> {
        let mut g = self.insert.lock();
        Self::append(&mut g, rec)
    }

    /// Appends a record, adding a full page image for each block whose page
    /// has not been logged since the redo point (and is not rebuilt by redo
    /// anyway). `pages[i]` is the current, already modified, content of
    /// `rec.blocks[i]`.
    pub fn insert_fpw(&self, mut rec: Record, pages: &[&[u8]]) -> Result<(Lsn, Lsn)> {
        if pages.len() != rec.blocks.len() {
            return Err(WalError::InvalidConfig("one page per block reference"));
        }
        let mut g = self.insert.lock();
        for (b, page) in rec.blocks.iter_mut().zip(pages) {
            if !b.will_init && needs_image(page, g.redo) {
                b.image = Some(page.to_vec());
            }
        }
        Self::append(&mut g, &rec)
    }

    /// Logs a full page image for a hint-bit-only change if the page has
    /// not been logged since the redo point (DESIGN: hint bit policy).
    /// Returns the record's end LSN, to become the page LSN, or `None` if
    /// no image was needed.
    pub fn log_hint_fpi(
        &self,
        rel: RelFileId,
        blk: BlockNumber,
        page: &[u8],
    ) -> Result<Option<Lsn>> {
        let mut g = self.insert.lock();
        if !needs_image(page, g.redo) {
            return Ok(None);
        }
        let mut rec = Record::new(RmgrId::Xlog, xlog_info::FPI_FOR_HINT);
        let mut b = BlockRef::new(rel, blk);
        b.image = Some(page.to_vec());
        rec.blocks.push(b);
        Self::append(&mut g, &rec).map(|(_, end)| Some(end))
    }

    fn append(g: &mut InsertState, rec: &Record) -> Result<(Lsn, Lsn)> {
        let bytes = rec.encode(g.prev)?;
        let start = g.next;
        let end = Lsn(start.0 + bytes.len() as u64);
        g.buf.extend_from_slice(&bytes);
        g.prev = start;
        g.next = end;
        Ok((start, end))
    }

    /// Starts a checkpoint: the current insert position becomes the redo
    /// point, so every page changed from now on is logged with a full image
    /// first. Returns the redo point.
    pub fn begin_checkpoint(&self) -> Lsn {
        let mut g = self.insert.lock();
        g.redo = g.next;
        g.redo
    }

    /// Redo point of the latest (possibly in-progress) checkpoint.
    pub fn redo_ptr(&self) -> Lsn {
        self.insert.lock().redo
    }

    /// Where the next record will start.
    pub fn insert_lsn(&self) -> Lsn {
        self.insert.lock().next
    }

    /// Everything before this LSN is durable.
    pub fn flushed_lsn(&self) -> Lsn {
        self.flush.lock().flushed
    }

    /// Makes the log durable up to at least `lsn` (clamped to what has been
    /// inserted). Aborts the process if writing or syncing fails.
    pub fn flush_to(&self, lsn: Lsn) {
        let mut f = self.flush.lock();
        if f.flushed >= lsn {
            return;
        }
        let (start, bytes) = {
            let g = self.insert.lock();
            (g.buf_start, g.buf.clone())
        };
        let end = start + bytes.len() as u64;
        if end <= f.flushed.0 {
            return;
        }
        let touched = match self.write_range(&mut f, start, &bytes) {
            Ok(t) => t,
            Err(e) => fatal("WAL write failed", &e),
        };
        for segno in touched {
            if let Err(e) = self.backend.sync(self.cfg.key(segno)) {
                fatal("WAL fsync failed", &e);
            }
        }
        f.flushed = Lsn(end);

        // Drop fully written blocks; the partial last block stays buffered.
        let keep_from = end - end % BLOCK;
        let mut g = self.insert.lock();
        let cut = (keep_from - g.buf_start) as usize;
        g.buf.drain(..cut);
        g.buf_start = keep_from;
    }

    /// Flushes everything inserted so far.
    pub fn flush_all(&self) {
        let lsn = self.insert_lsn();
        self.flush_to(lsn);
    }

    /// Writes `bytes` (starting at block-aligned `start`) padded to whole
    /// blocks. Returns the segments written.
    fn write_range(&self, f: &mut FlushState, start: u64, bytes: &[u8]) -> Result<Vec<u64>> {
        let seg = self.cfg.segment_size;
        let end = start + (bytes.len() as u64).next_multiple_of(BLOCK);
        let fence = self.fence.fence_token();
        let mut touched = Vec::new();
        let mut pos = start;
        while pos < end {
            let segno = pos / seg;
            let off = pos % seg;
            let n = (seg - off).min(end - pos);
            self.ensure_segment(f, segno)?;
            let mut buf = AlignedBuf::new(n as usize)?;
            let from = (pos - start) as usize;
            let avail = bytes.len().saturating_sub(from).min(n as usize);
            buf[..avail].copy_from_slice(&bytes[from..from + avail]);
            self.backend
                .write_at(self.cfg.key(segno), off, &buf, fence)?;
            touched.push(segno);
            pos += n;
        }
        Ok(touched)
    }

    /// Creates segment `segno` zero-filled if it does not exist yet.
    fn ensure_segment(&self, f: &mut FlushState, segno: u64) -> Result<()> {
        if segno < f.created_upto {
            return Ok(());
        }
        let key = self.cfg.key(segno);
        let seg = self.cfg.segment_size;
        let have = match self.backend.file_len(key) {
            Ok(len) => len,
            Err(e) if is_not_found(&e) => {
                self.backend.create(key, self.fence.fence_token())?;
                0
            }
            Err(e) => return Err(e.into()),
        };
        if have < seg {
            let zeros = AlignedBuf::new(ZERO_CHUNK.min(seg as usize))?;
            let mut pos = have - have % BLOCK;
            while pos < seg {
                let n = (seg - pos).min(zeros.len() as u64) as usize;
                self.backend
                    .write_at(key, pos, &zeros[..n], self.fence.fence_token())?;
                pos += n as u64;
            }
            if let Err(e) = self.backend.sync(key) {
                fatal("WAL segment fsync failed", &e);
            }
        }
        f.created_upto = segno + 1;
        Ok(())
    }
}

/// Whether a page must be logged with a full image: it has a header and
/// its LSN is not past the redo point. (An all-zero page has no content to
/// protect from torn writes.)
fn needs_image(page: &[u8], redo: Lsn) -> bool {
    page.len() == PAGE_SIZE && !is_new_page(page) && page_lsn(page) <= redo
}

/// Sequential reader of WAL records.
#[derive(Debug)]
pub struct WalReader {
    backend: Arc<dyn StorageBackend>,
    cfg: WalConfig,
    next: Lsn,
    prev: Lsn,
    /// Whether to check the next record's `prev_lsn` against `prev`.
    check_prev: bool,
    cache: Option<(u64, AlignedBuf)>,
}

impl WalReader {
    /// Starts reading at `start`. If `prev` is given, the first record's
    /// `prev_lsn` must match it.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        cfg: WalConfig,
        start: Lsn,
        prev: Option<Lsn>,
    ) -> Result<Self> {
        cfg.validate()?;
        Ok(Self {
            backend,
            cfg,
            next: start,
            prev: prev.unwrap_or(Lsn::INVALID),
            check_prev: prev.is_some(),
            cache: None,
        })
    }

    /// The next record as `(start, end, record)`, or `None` at the end of
    /// valid WAL.
    pub fn next_record(&mut self) -> Result<Option<(Lsn, Lsn, Decoded)>> {
        let Some(header) = self.read(self.next.0, HEADER_SIZE)? else {
            return Ok(None);
        };
        let Some(len) = record::peek_len(&header) else {
            return Ok(None);
        };
        let Some(bytes) = self.read(self.next.0, len)? else {
            return Ok(None);
        };
        let Ok(d) = record::decode(&bytes) else {
            return Ok(None);
        };
        if self.check_prev && d.prev_lsn != self.prev {
            return Ok(None);
        }
        let start = self.next;
        self.next = Lsn(start.0 + len as u64);
        self.prev = start;
        self.check_prev = true;
        Ok(Some((start, self.next, d)))
    }

    /// After `next_record` returned `None`: where valid WAL ends.
    pub fn end_lsn(&self) -> Lsn {
        self.next
    }

    /// Start of the last record returned.
    pub fn last_start(&self) -> Lsn {
        self.prev
    }

    /// Reads `len` bytes at `lsn`, or `None` if a segment is missing.
    fn read(&mut self, lsn: u64, len: usize) -> Result<Option<Vec<u8>>> {
        let seg = self.cfg.segment_size;
        let mut out = Vec::with_capacity(len);
        let mut pos = lsn;
        while out.len() < len {
            let segno = pos / seg;
            if self.cache.as_ref().map(|c| c.0) != Some(segno) {
                let mut buf = AlignedBuf::new(seg as usize)?;
                match self.backend.read_at(self.cfg.key(segno), 0, &mut buf) {
                    Ok(()) => {}
                    Err(e) if is_not_found(&e) => return Ok(None),
                    Err(crate::storage::StorageError::ShortRead { .. }) => return Ok(None),
                    Err(e) => return Err(e.into()),
                }
                self.cache = Some((segno, buf));
            }
            let Some((_, data)) = &self.cache else {
                return Ok(None);
            };
            let off = (pos % seg) as usize;
            let n = (len - out.len()).min(seg as usize - off);
            out.extend_from_slice(&data[off..off + n]);
            pos += n as u64;
        }
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::RelFileId;
    use crate::storage::backend::{FenceToken, FixedFence};
    use crate::storage::mem::MemBackend;
    use crate::storage::page::BlockNumber;
    use crate::wal::record::{BlockRef, RmgrId};

    const SEG: u64 = 16 * 1024;

    fn cfg() -> WalConfig {
        WalConfig {
            timeline: 1,
            segment_size: SEG,
        }
    }

    fn fence() -> Arc<dyn FenceSource> {
        Arc::new(FixedFence(FenceToken::new(5)))
    }

    fn rec(tag: u8, len: usize) -> Record {
        let mut r = Record::new(RmgrId::Heap, tag);
        r.main = vec![tag; len];
        r
    }

    fn read_all(mem: &Arc<MemBackend>) -> (Vec<(Lsn, Record)>, WalReader) {
        let mut rd = WalReader::new(mem.clone(), cfg(), FIRST_LSN, Some(Lsn::INVALID)).unwrap();
        let mut out = Vec::new();
        while let Some((start, _, d)) = rd.next_record().unwrap() {
            out.push((start, d.record));
        }
        (out, rd)
    }

    #[test]
    fn insert_flush_read_back() {
        let mem = Arc::new(MemBackend::default());
        let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
        let mut expected = Vec::new();
        for i in 0..20u8 {
            let r = rec(i, 100 + usize::from(i));
            let (start, end) = wal.insert(&r).unwrap();
            assert_eq!(start, expected.last().map_or(FIRST_LSN, |(_, e, _)| *e));
            assert!(end > start);
            expected.push((start, end, r));
        }
        assert_eq!(wal.flushed_lsn(), FIRST_LSN);
        wal.flush_all();
        assert_eq!(wal.flushed_lsn(), wal.insert_lsn());
        let (got, rd) = read_all(&mem);
        assert_eq!(got.len(), 20);
        for ((s, r), (es, _, er)) in got.iter().zip(&expected) {
            assert_eq!((s, r), (es, er));
        }
        assert_eq!(rd.end_lsn(), wal.insert_lsn());
        assert!(mem.fences().iter().all(|&f| f == FenceToken::new(5)));
    }

    #[test]
    fn unflushed_records_are_not_durable() {
        let mem = Arc::new(MemBackend::default());
        let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
        let (_, end) = wal.insert(&rec(1, 10)).unwrap();
        wal.flush_to(end);
        wal.insert(&rec(2, 10)).unwrap();
        assert_eq!(read_all(&mem).0.len(), 1);
    }

    #[test]
    fn partial_block_is_rewritten_not_clobbered() {
        let mem = Arc::new(MemBackend::default());
        let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
        for i in 0..50u8 {
            let (_, end) = wal.insert(&rec(i, 30)).unwrap();
            wal.flush_to(end); // many flushes within the same 4KB block
        }
        let (got, _) = read_all(&mem);
        assert_eq!(got.len(), 50);
        assert!(got.iter().enumerate().all(|(i, (_, r))| r.info == i as u8));
    }

    #[test]
    fn records_cross_segment_boundaries() {
        let mem = Arc::new(MemBackend::default());
        let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
        for i in 0..10u8 {
            wal.insert(&rec(i, 7000)).unwrap(); // ~4 segments in total
        }
        wal.flush_all();
        assert!(wal.insert_lsn().0 / SEG >= 4);
        let (got, _) = read_all(&mem);
        assert_eq!(got.len(), 10);
        assert!(got.iter().all(|(_, r)| r.main.len() == 7000));
    }

    #[test]
    fn reopen_continues_after_last_record() {
        let mem = Arc::new(MemBackend::default());
        {
            let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
            for i in 0..5u8 {
                wal.insert(&rec(i, 50)).unwrap();
            }
            wal.flush_all();
            wal.insert(&rec(99, 50)).unwrap(); // lost: never flushed
        }
        let (got, rd) = read_all(&mem);
        assert_eq!(got.len(), 5);
        let wal = Wal::open_at(
            mem.clone(),
            fence(),
            cfg(),
            rd.end_lsn(),
            rd.last_start(),
            FIRST_LSN,
        )
        .unwrap();
        for i in 5..8u8 {
            wal.insert(&rec(i, 50)).unwrap();
        }
        wal.flush_all();
        let (got, _) = read_all(&mem);
        let infos: Vec<u8> = got.iter().map(|(_, r)| r.info).collect();
        assert_eq!(infos, (0..8).collect::<Vec<u8>>());
    }

    #[test]
    fn stops_at_corruption_and_broken_chain() {
        let mem = Arc::new(MemBackend::default());
        let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
        let mut starts = Vec::new();
        for i in 0..5u8 {
            starts.push(wal.insert(&rec(i, 40)).unwrap().0);
        }
        wal.flush_all();
        mem.corrupt(cfg().key(0), starts[3].0 as usize + HEADER_SIZE + 1);
        let (got, rd) = read_all(&mem);
        assert_eq!(got.len(), 3);
        assert_eq!(rd.end_lsn(), starts[3]);

        // A valid record whose prev does not chain is not accepted.
        let mut rd = WalReader::new(mem.clone(), cfg(), starts[1], Some(starts[2])).unwrap();
        assert!(rd.next_record().unwrap().is_none());
        // Without a prev check, reading can start mid-log (redo point).
        let mut rd = WalReader::new(mem.clone(), cfg(), starts[1], None).unwrap();
        assert_eq!(rd.next_record().unwrap().unwrap().2.record.info, 1);
    }

    #[test]
    fn empty_log_and_missing_segment() {
        let mem = Arc::new(MemBackend::default());
        let (got, rd) = read_all(&mem);
        assert!(got.is_empty());
        assert_eq!(rd.end_lsn(), FIRST_LSN);
    }

    #[test]
    fn concurrent_inserts_and_flushes() {
        let mem = Arc::new(MemBackend::default());
        let wal = Wal::create(mem.clone(), fence(), cfg()).unwrap();
        std::thread::scope(|s| {
            for t in 0..4u8 {
                let wal = &wal;
                s.spawn(move || {
                    for i in 0..100u16 {
                        let mut r = rec(t, 20 + usize::from(i % 50));
                        r.blocks.push(BlockRef::new(
                            RelFileId(u32::from(t)),
                            BlockNumber(u32::from(i)),
                        ));
                        let (_, end) = wal.insert(&r).unwrap();
                        if i % 7 == 0 {
                            wal.flush_to(end);
                            assert!(wal.flushed_lsn() >= end);
                        }
                    }
                });
            }
        });
        wal.flush_all();
        let (got, _) = read_all(&mem);
        assert_eq!(got.len(), 400);
        for t in 0..4u8 {
            let blks: Vec<u32> = got
                .iter()
                .filter(|(_, r)| r.info == t)
                .map(|(_, r)| r.blocks[0].blk.0)
                .collect();
            assert_eq!(
                blks,
                (0..100).collect::<Vec<u32>>(),
                "per-thread order kept"
            );
        }
    }

    #[test]
    fn rejects_bad_config() {
        let mem = Arc::new(MemBackend::default());
        let bad = WalConfig {
            timeline: 1,
            segment_size: 5000,
        };
        assert!(Wal::create(mem, fence(), bad).is_err());
    }
}
