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
//!
//! # WAL
//!
//! When the pool has a WAL, every change is logged while the page lock is
//! held, and the page LSN is set to the record's end (a failure there aborts
//! the process: the page is already modified). Each block reference carries
//! everything needed to redo that page alone ([`redo`]), so pages can be
//! replayed independently. An update that moves to another page locks both
//! pages in block order and logs one record, so it is atomic after a crash.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::tuple::infomask;
use super::tuple::{self, MAX_TUPLE_SIZE, Tid, TupleError, TupleHeader};
use super::visibility::{Me, satisfies_mvcc};
use crate::buffer::{BufferError, BufferPool, BufferTag, PageMut};
use crate::storage::StorageError;
use crate::storage::backend::RelFileId;
use crate::storage::page::{BlockNumber, PageKind, is_new_page};
use crate::storage::slotted::{SlotId, SlottedPage};
use crate::txn::{
    CommandId, Isolation, Snapshot, Transaction, TransactionError, TransactionManager, TxnError,
    Xid, XidStatus,
};
use crate::wal::log_and_stamp;
use crate::wal::record::{BlockRef, Record, RmgrId};

/// Marker for "no target block yet".
const NO_TARGET: u32 = u32::MAX;

/// `info` values of heap WAL records.
pub mod info {
    /// Block 0: `slot u16 | tuple`.
    pub const INSERT: u8 = 0;
    /// Block 0: `slot u16 | xmax u64 | cmax u32`.
    pub const DELETE: u8 = 1;
    /// Block 0 (old): `slot u16 | xmax u64 | cmax u32 | new blk u32 | new
    /// slot u16`; block 1 (new): `slot u16 | tuple`.
    pub const UPDATE: u8 = 2;
    /// Block 0: `old slot u16 | xmax u64 | cmax u32 | new slot u16 | tuple`.
    pub const UPDATE_SAME: u8 = 3;
    /// Block 0: slots newly marked dead (u16 each); the page is compacted.
    pub const PRUNE: u8 = 4;
}

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

    #[error("heap redo failed: {0}")]
    Redo(&'static str),

    #[error(transparent)]
    Transaction(#[from] TransactionError),

    #[error(transparent)]
    Txn(#[from] TxnError),
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
            if let Some(p) = place(&mut page, BlockNumber(target), tuple)? {
                self.log_insert(&mut page, &p);
                return Ok(p.tid);
            }
        }
        // Target full (or none): append a block. Concurrent inserters may
        // each extend; that only costs a little space.
        let (blk, pin) = self.pool.extend(self.rel)?;
        let mut page = pin.write();
        let p = place(&mut page, blk, tuple)?.ok_or(too_large(tuple))?;
        self.log_insert(&mut page, &p);
        self.advance_target(blk);
        Ok(p.tid)
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

    /// Marks the tuple deleted by `xmax` (no visibility checks; an existing
    /// xmax is an error). See [`HeapFile::delete_mvcc`] for transactions.
    pub fn delete(&self, tid: Tid, xmax: Xid, cmax: CommandId) -> Result<()> {
        match self.delete_checked(tid, xmax, cmax, &|page| {
            check_updatable(page, tid).map(|()| None)
        })? {
            TmResult::Ok(_) => Ok(()),
            _ => Err(HeapError::Redo("unexpected update result")),
        }
    }

    /// Deletes the tuple at `tid` for `txn` if it may (PostgreSQL's
    /// heap_delete with HeapTupleSatisfiesUpdate). Does not wait; see
    /// [`HeapFile::delete_waiting`].
    pub fn delete_mvcc(&self, tid: Tid, txn: &mut Transaction) -> Result<TmResult> {
        let xid = txn.write_xid()?;
        let cid = txn.command_id();
        let tm = txn.tm();
        self.delete_checked(tid, xid, cid, &|page| {
            satisfies_update(page, tid, xid, cid, tm)
        })
    }

    fn delete_checked(
        &self,
        tid: Tid,
        xmax: Xid,
        cmax: CommandId,
        check: &dyn Fn(&[u8]) -> Result<Option<TmResult>>,
    ) -> Result<TmResult> {
        let pin = self.pool.fetch(self.tag(tid.blk))?;
        let mut page = pin.write();
        if let Some(r) = check(&page)? {
            return Ok(r);
        }
        stamp_xmax(&mut page, tid, xmax, cmax, None)?;
        let mut b = BlockRef::new(self.rel, tid.blk);
        b.data = xmax_data(tid.slot, xmax, cmax);
        let mut rec = Record::new(RmgrId::Heap, info::DELETE);
        rec.xid = xmax;
        self.log1(rec, b, &mut page);
        Ok(TmResult::Ok(tid))
    }

    /// Inserts `new_tuple` as the next version of `tid`, marking the old one
    /// deleted by `xid` with its ctid pointing at the new version (no
    /// visibility checks; an existing xmax is an error). See
    /// [`HeapFile::update_mvcc`] for transactions.
    pub fn update(&self, tid: Tid, new_tuple: &[u8], xid: Xid, cid: CommandId) -> Result<Tid> {
        match self.update_checked(tid, new_tuple, xid, cid, &|page| {
            check_updatable(page, tid).map(|()| None)
        })? {
            TmResult::Ok(t) => Ok(t),
            _ => Err(HeapError::Redo("unexpected update result")),
        }
    }

    /// Updates the tuple at `tid` for `txn` if it may. `new_tuple` must be
    /// formed with `txn`'s xid and command id. Does not wait; see
    /// [`HeapFile::update_waiting`].
    pub fn update_mvcc(
        &self,
        tid: Tid,
        new_tuple: &[u8],
        txn: &mut Transaction,
    ) -> Result<TmResult> {
        let xid = txn.write_xid()?;
        let cid = txn.command_id();
        let tm = txn.tm();
        self.update_checked(tid, new_tuple, xid, cid, &|page| {
            satisfies_update(page, tid, xid, cid, tm)
        })
    }

    /// The new version goes on the same page when it fits. Otherwise the old
    /// page and a target page are locked together in block order and both
    /// changes are logged in one record. `check` decides, under the old
    /// page's lock, whether the update may proceed.
    fn update_checked(
        &self,
        tid: Tid,
        new_tuple: &[u8],
        xid: Xid,
        cid: CommandId,
        check: &dyn Fn(&[u8]) -> Result<Option<TmResult>>,
    ) -> Result<TmResult> {
        check_size(new_tuple)?;
        let old_pin = self.pool.fetch(self.tag(tid.blk))?;
        {
            let mut page = old_pin.write();
            if let Some(r) = check(&page)? {
                return Ok(r);
            }
            if let Some(p) = place(&mut page, tid.blk, new_tuple)? {
                stamp_xmax(&mut page, tid, xid, cid, Some(p.tid))?;
                let mut b = BlockRef::new(self.rel, tid.blk);
                b.data = xmax_data(tid.slot, xid, cid);
                b.data.extend_from_slice(&p.tid.slot.0.to_le_bytes());
                b.data.extend_from_slice(&p.stored);
                let mut rec = Record::new(RmgrId::Heap, info::UPDATE_SAME);
                rec.xid = xid;
                self.log1(rec, b, &mut page);
                return Ok(TmResult::Ok(p.tid));
            }
        }

        // Try the cached target block first, then a fresh block.
        let target = self.target.load(Ordering::Relaxed);
        if target != NO_TARGET && target != tid.blk.0 {
            let tblk = BlockNumber(target);
            let tpin = self.pool.fetch(self.tag(tblk))?;
            let (mut old, mut new) = lock_pair(&old_pin, tid.blk, &tpin, tblk);
            match self.update_across(&mut old, &mut new, tid, tblk, new_tuple, xid, cid, check)? {
                Across::Done(r) => return Ok(r),
                Across::Full => {}
            }
        }
        let (nblk, npin) = self.pool.extend(self.rel)?;
        // A fresh block is always above the old one: lock old first.
        let mut old = old_pin.write();
        let mut new = npin.write();
        let r =
            match self.update_across(&mut old, &mut new, tid, nblk, new_tuple, xid, cid, check)? {
                Across::Done(r) => r,
                Across::Full => return Err(too_large(new_tuple)),
            };
        self.advance_target(nblk);
        Ok(r)
    }

    /// Update with both pages locked (re-checking the old version, which
    /// may have changed while it was unlocked).
    #[allow(clippy::too_many_arguments)]
    fn update_across(
        &self,
        old: &mut PageMut<'_>,
        new: &mut PageMut<'_>,
        tid: Tid,
        nblk: BlockNumber,
        new_tuple: &[u8],
        xid: Xid,
        cid: CommandId,
        check: &dyn Fn(&[u8]) -> Result<Option<TmResult>>,
    ) -> Result<Across> {
        if let Some(r) = check(old)? {
            return Ok(Across::Done(r));
        }
        let Some(p) = place(new, nblk, new_tuple)? else {
            return Ok(Across::Full);
        };
        stamp_xmax(old, tid, xid, cid, Some(p.tid))?;
        let mut ob = BlockRef::new(self.rel, tid.blk);
        ob.data = xmax_data(tid.slot, xid, cid);
        ob.data.extend_from_slice(&nblk.0.to_le_bytes());
        ob.data.extend_from_slice(&p.tid.slot.0.to_le_bytes());
        let nb = insert_block(self.rel, &p);
        let mut rec = Record::new(RmgrId::Heap, info::UPDATE);
        rec.xid = xid;
        rec.blocks = vec![ob, nb];
        self.log(rec, &mut [&mut old[..], &mut new[..]]);
        Ok(Across::Done(TmResult::Ok(p.tid)))
    }

    /// [`HeapFile::delete_mvcc`] with the waiting and isolation rules: waits
    /// for a transaction modifying the row and retries. A row changed by a
    /// transaction that committed after `txn`'s snapshot is a serialization
    /// failure under Snapshot Isolation; under Read Committed the result is
    /// returned so the caller can re-check the newest version.
    pub fn delete_waiting(&self, tid: Tid, txn: &mut Transaction) -> Result<TmResult> {
        loop {
            let r = self.delete_mvcc(tid, txn)?;
            if let Some(r) = resolve(r, txn)? {
                return Ok(r);
            }
        }
    }

    /// [`HeapFile::update_mvcc`] with the waiting and isolation rules of
    /// [`HeapFile::delete_waiting`].
    pub fn update_waiting(
        &self,
        tid: Tid,
        new_tuple: &[u8],
        txn: &mut Transaction,
    ) -> Result<TmResult> {
        loop {
            let r = self.update_mvcc(tid, new_tuple, txn)?;
            if let Some(r) = resolve(r, txn)? {
                return Ok(r);
            }
        }
    }

    /// Copies the tuple at `tid` if it is visible to `snap`.
    pub fn fetch_visible(
        &self,
        tid: Tid,
        snap: &Snapshot,
        me: &Me,
        tm: &dyn TransactionManager,
    ) -> Result<Option<Vec<u8>>> {
        let Some(t) = self.fetch(tid)? else {
            return Ok(None);
        };
        let v = satisfies_mvcc(&TupleHeader::decode(&t)?, snap, me, tm)?;
        Ok(v.visible.then_some(t))
    }

    /// Follows the update chain from `tid` to the newest version that is
    /// not deleted by a committed transaction (Read Committed re-check;
    /// PostgreSQL's EvalPlanQual fetch). `None` if the row was deleted.
    pub fn latest_version(&self, mut tid: Tid, tm: &dyn TransactionManager) -> Result<Option<Tid>> {
        loop {
            let Some(t) = self.fetch(tid)? else {
                return Ok(None);
            };
            let h = TupleHeader::decode(&t)?;
            if !h.xmax.is_valid() || tm.status(h.xmax)? != XidStatus::Committed {
                return Ok(Some(tid));
            }
            if h.ctid == tid {
                return Ok(None); // deleted
            }
            // The next version must be the one this updater inserted.
            match self.fetch(h.ctid)? {
                Some(n) if TupleHeader::decode(&n)?.xmin == h.xmax => tid = h.ctid,
                _ => return Ok(None),
            }
        }
    }

    /// Number of blocks in the heap now.
    pub fn nblocks(&self) -> Result<u32> {
        Ok(self.pool.nblocks(self.rel)?)
    }

    /// Prunes block `blk`: tuples dead to everyone (by `horizon`) become
    /// DEAD line pointers and the page is compacted; slot numbers stay, so
    /// index entries still point at the dead slots until index vacuum.
    /// Returns every DEAD slot on the page (including ones left by an
    /// earlier, interrupted vacuum). Logged as one PRUNE record.
    pub fn prune_page(
        &self,
        blk: BlockNumber,
        horizon: Xid,
        tm: &dyn TransactionManager,
    ) -> Result<Vec<Tid>> {
        let pin = self.pool.fetch(self.tag(blk))?;
        let mut page = pin.write();
        if is_new_page(&page) {
            return Ok(Vec::new());
        }
        let mut newly_dead = Vec::new();
        {
            let p = SlottedPage::open(&page[..])?;
            for s in 0..p.slot_count() {
                let slot = SlotId(s);
                if let Some(t) = p.get_item(slot)
                    && satisfies_vacuum(&TupleHeader::decode(t)?, horizon, tm)? == VacuumState::Dead
                {
                    newly_dead.push(slot);
                }
            }
        }
        if !newly_dead.is_empty() {
            let mut p = SlottedPage::open(&mut page[..])?;
            for &slot in &newly_dead {
                p.mark_dead(slot)?;
            }
            p.compact();
            page.mark_dirty();
            let mut b = BlockRef::new(self.rel, blk);
            b.data = newly_dead.iter().flat_map(|s| s.0.to_le_bytes()).collect();
            self.log1(Record::new(RmgrId::Heap, info::PRUNE), b, &mut page);
        }
        let p = SlottedPage::open(&page[..])?;
        Ok((0..p.slot_count())
            .map(SlotId)
            .filter(|&s| {
                p.item_id(s)
                    .is_some_and(|id| id.state == crate::storage::slotted::ItemState::Dead)
            })
            .map(|slot| Tid { blk, slot })
            .collect())
    }

    /// Sequential scan returning only versions visible to `snap`, setting
    /// hint bits opportunistically page by page.
    pub fn scan_mvcc<'a>(
        &'a self,
        snap: Snapshot,
        me: Me,
        tm: &'a dyn TransactionManager,
    ) -> Result<HeapMvccScan<'a>> {
        Ok(HeapMvccScan {
            heap: self,
            snap,
            me,
            tm,
            nblocks: self.pool.nblocks(self.rel)?,
            next_blk: 0,
            buffered: Vec::new().into_iter(),
        })
    }

    /// Visible tuples of one page. Suggested hint bits are applied if the
    /// exclusive lock is free, to tuples whose xmin/xmax are unchanged.
    fn read_page_mvcc(
        &self,
        blk: BlockNumber,
        snap: &Snapshot,
        me: &Me,
        tm: &dyn TransactionManager,
    ) -> Result<Vec<(Tid, Vec<u8>)>> {
        let pin = self.pool.fetch(self.tag(blk))?;
        let mut out = Vec::new();
        let mut hints: Vec<(SlotId, Xid, Xid, u16)> = Vec::new();
        {
            let page = pin.read();
            if is_new_page(&page) {
                return Ok(out);
            }
            let p = SlottedPage::open(&page[..])?;
            for s in 0..p.slot_count() {
                let slot = SlotId(s);
                let Some(t) = p.get_item(slot) else { continue };
                let h = TupleHeader::decode(t)?;
                let v = satisfies_mvcc(&h, snap, me, tm)?;
                if v.hints != 0 && h.infomask & v.hints != v.hints {
                    hints.push((slot, h.xmin, h.xmax, v.hints));
                }
                if v.visible {
                    out.push((Tid { blk, slot }, t.to_vec()));
                }
            }
        }
        if !hints.is_empty()
            && let Some(mut page) = pin.try_write()
        {
            let mut changed = false;
            let mut p = SlottedPage::open(&mut page[..])?;
            for (slot, xmin, xmax, bits) in hints {
                if let Some(item) = p.get_item_mut(slot) {
                    let h = TupleHeader::decode(item)?;
                    if h.xmin == xmin && h.xmax == xmax {
                        tuple::set_hint_bits(item, bits)?;
                        changed = true;
                    }
                }
            }
            if changed {
                page.mark_dirty_hint();
            }
        }
        Ok(out)
    }

    /// Sets hint bits on the tuple at `tid` if the page's exclusive lock is
    /// free right now. Returns whether the bits are now set; `false` means
    /// the attempt was skipped (hint bits are optional, so this is safe).
    ///
    /// The caller is responsible for the hint bit policy, e.g. setting
    /// `XMIN_COMMITTED` only after the commit record is flushed.
    pub fn set_hint_bits(&self, tid: Tid, bits: u16) -> Result<bool> {
        let pin = self.pool.fetch(self.tag(tid.blk))?;
        let Some(mut page) = pin.try_write() else {
            return Ok(false);
        };
        if is_new_page(&page) {
            return Err(HeapError::TupleNotFound { tid });
        }
        let mut p = SlottedPage::open(&mut page[..])?;
        let item = p
            .get_item_mut(tid.slot)
            .ok_or(HeapError::TupleNotFound { tid })?;
        let before = TupleHeader::decode(item)?.infomask;
        tuple::set_hint_bits(item, bits)?;
        if before | bits != before {
            page.mark_dirty_hint();
        }
        Ok(true)
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

    fn log_insert(&self, page: &mut PageMut<'_>, p: &Placed) {
        let b = insert_block(self.rel, p);
        let mut rec = Record::new(RmgrId::Heap, info::INSERT);
        // The inserter's xid, so recovery never hands it out again.
        rec.xid = TupleHeader::decode(&p.stored)
            .map(|h| h.xmin)
            .unwrap_or(Xid::INVALID);
        self.log1(rec, b, page);
    }

    fn log1(&self, mut rec: Record, block: BlockRef, page: &mut PageMut<'_>) {
        rec.blocks = vec![block];
        self.log(rec, &mut [&mut page[..]]);
    }

    fn log(&self, rec: Record, pages: &mut [&mut [u8]]) {
        log_and_stamp(self.pool.wal(), rec, pages);
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

/// State of a tuple version for vacuum (PostgreSQL's HTSV_Result).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumState {
    /// Visible to someone and not deleted by a committed transaction.
    Live,
    /// Invisible to every current and future snapshot: removable.
    Dead,
    /// Deleted by a committed transaction some snapshot may predate.
    RecentlyDead,
    InsertInProgress,
    DeleteInProgress,
}

/// Classifies a tuple version against the vacuum `horizon` (the oldest
/// xmin any snapshot may hold; PostgreSQL's HeapTupleSatisfiesVacuum).
pub fn satisfies_vacuum(
    h: &TupleHeader,
    horizon: Xid,
    tm: &dyn TransactionManager,
) -> Result<VacuumState> {
    let mask = h.infomask;
    if mask & infomask::XMIN_INVALID != 0 {
        return Ok(VacuumState::Dead);
    }
    if mask & infomask::XMIN_COMMITTED == 0 {
        match tm.status(h.xmin)? {
            XidStatus::InProgress => return Ok(VacuumState::InsertInProgress),
            XidStatus::Aborted => return Ok(VacuumState::Dead),
            XidStatus::Committed => {}
        }
    }
    if !h.xmax.is_valid() || mask & infomask::XMAX_INVALID != 0 {
        return Ok(VacuumState::Live);
    }
    let status = if mask & infomask::XMAX_COMMITTED != 0 {
        XidStatus::Committed
    } else {
        tm.status(h.xmax)?
    };
    Ok(match status {
        XidStatus::InProgress => VacuumState::DeleteInProgress,
        XidStatus::Aborted => VacuumState::Live,
        XidStatus::Committed if h.xmax < horizon => VacuumState::Dead,
        XidStatus::Committed => VacuumState::RecentlyDead,
    })
}

/// Result of trying to delete or update a tuple (PostgreSQL's TM_Result).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TmResult {
    /// Done. For an update, the new version's TID; for a delete, `tid`.
    Ok(Tid),
    /// Not visible to the current command (inserted by it, by an aborted or
    /// still running transaction, or deleted by an earlier command of ours).
    Invisible,
    /// Already deleted or updated by the current command.
    SelfModified,
    /// Another running transaction is deleting or updating it.
    BeingModified(Xid),
    /// Updated by committed transaction `xmax`; the newer version is `ctid`.
    Updated { ctid: Tid, xmax: Xid },
    /// Deleted by committed transaction `xmax`.
    Deleted { xmax: Xid },
}

/// Outcome of an update with both pages locked.
enum Across {
    Done(TmResult),
    /// The target page has no room.
    Full,
}

/// Whether `txn` (xid, cid) may delete or update the version at `tid`
/// (PostgreSQL's HeapTupleSatisfiesUpdate). `None` means it may.
fn satisfies_update(
    page: &[u8],
    tid: Tid,
    xid: Xid,
    cid: CommandId,
    tm: &dyn TransactionManager,
) -> Result<Option<TmResult>> {
    if is_new_page(page) {
        return Err(HeapError::TupleNotFound { tid });
    }
    let p = SlottedPage::open(page)?;
    let item = p
        .get_item(tid.slot)
        .ok_or(HeapError::TupleNotFound { tid })?;
    let h = TupleHeader::decode(item)?;
    let mask = h.infomask;

    if h.xmin == xid {
        if h.cmin >= cid {
            return Ok(Some(TmResult::Invisible));
        }
    } else if mask & infomask::XMIN_INVALID != 0
        || (mask & infomask::XMIN_COMMITTED == 0 && tm.status(h.xmin)? != XidStatus::Committed)
    {
        return Ok(Some(TmResult::Invisible));
    }

    if !h.xmax.is_valid() || mask & infomask::XMAX_INVALID != 0 {
        return Ok(None);
    }
    if h.xmax == xid {
        return Ok(Some(if h.cmax >= cid {
            TmResult::SelfModified
        } else {
            TmResult::Invisible
        }));
    }
    let status = if mask & infomask::XMAX_COMMITTED != 0 {
        XidStatus::Committed
    } else {
        tm.status(h.xmax)?
    };
    Ok(match status {
        XidStatus::InProgress => Some(TmResult::BeingModified(h.xmax)),
        XidStatus::Aborted => None, // the old xmax is overwritten
        XidStatus::Committed if h.ctid != tid => Some(TmResult::Updated {
            ctid: h.ctid,
            xmax: h.xmax,
        }),
        XidStatus::Committed => Some(TmResult::Deleted { xmax: h.xmax }),
    })
}

/// Applies the waiting and isolation rules to a DML result: `None` means
/// "waited for the other transaction, retry".
fn resolve(r: TmResult, txn: &mut Transaction) -> Result<Option<TmResult>> {
    match r {
        TmResult::BeingModified(x) => {
            txn.wait_for(x)?;
            Ok(None)
        }
        TmResult::Updated { xmax, .. } | TmResult::Deleted { xmax }
            if txn.isolation() == Isolation::SnapshotIsolation
                && txn.snapshot().is_running(xmax) =>
        {
            Err(TransactionError::SerializationFailure.into())
        }
        other => Ok(Some(other)),
    }
}

/// Locks two different pages of one relation in block order.
fn lock_pair<'a>(
    a_pin: &'a crate::buffer::PinGuard<'_>,
    a_blk: BlockNumber,
    b_pin: &'a crate::buffer::PinGuard<'_>,
    b_blk: BlockNumber,
) -> (PageMut<'a>, PageMut<'a>) {
    if a_blk < b_blk {
        let a = a_pin.write();
        (a, b_pin.write())
    } else {
        let b = b_pin.write();
        (a_pin.write(), b)
    }
}

/// Applies block `idx` of a heap record to `page`. The caller has checked
/// that the page LSN is below the record's and sets the page LSN afterwards.
pub fn redo(info: u8, idx: usize, block: &BlockRef, page: &mut [u8]) -> Result<()> {
    if block.will_init {
        SlottedPage::init(&mut *page, PageKind::Heap, 0)?;
    }
    let d = &block.data;
    match (info, idx) {
        (info::INSERT, 0) | (info::UPDATE, 1) => redo_add(
            page,
            u16_at(d, 0)?,
            d.get(2..).ok_or(HeapError::Redo("short data"))?,
        ),
        (info::DELETE, 0) => redo_xmax(page, d, None),
        (info::PRUNE, 0) => {
            let mut p = SlottedPage::open(page)?;
            for c in d.chunks_exact(2) {
                p.mark_dead(SlotId(u16::from_le_bytes([c[0], c[1]])))?;
            }
            p.compact();
            Ok(())
        }
        (info::UPDATE, 0) => {
            let new = Tid {
                blk: BlockNumber(u32_at(d, 14)?),
                slot: SlotId(u16_at(d, 18)?),
            };
            redo_xmax(page, d, Some(new))
        }
        (info::UPDATE_SAME, 0) => {
            let new_slot = u16_at(d, 14)?;
            redo_add(
                page,
                new_slot,
                d.get(16..).ok_or(HeapError::Redo("short data"))?,
            )?;
            let new = Tid {
                blk: block.blk,
                slot: SlotId(new_slot),
            };
            redo_xmax(page, d, Some(new))
        }
        _ => Err(HeapError::Redo("unknown record")),
    }
}

/// Re-adds a tuple; it must land in the slot it had originally.
fn redo_add(page: &mut [u8], slot: u16, tuple: &[u8]) -> Result<()> {
    let mut p = SlottedPage::open(page)?;
    match p.add_item(tuple)? {
        Some(s) if s.0 == slot => Ok(()),
        _ => Err(HeapError::Redo("tuple did not land in its original slot")),
    }
}

fn redo_xmax(page: &mut [u8], d: &[u8], new: Option<Tid>) -> Result<()> {
    let slot = SlotId(u16_at(d, 0)?);
    let xmax = Xid(u64_at(d, 2)?);
    let cmax = CommandId(u32_at(d, 10)?);
    let mut p = SlottedPage::open(page)?;
    let item = p
        .get_item_mut(slot)
        .ok_or(HeapError::Redo("missing tuple"))?;
    tuple::set_xmax(item, xmax, cmax)?;
    if let Some(n) = new {
        tuple::set_ctid(item, n)?;
    }
    Ok(())
}

fn u16_at(d: &[u8], at: usize) -> Result<u16> {
    let b = d.get(at..at + 2).ok_or(HeapError::Redo("short data"))?;
    Ok(u16::from_le_bytes([b[0], b[1]]))
}

fn u32_at(d: &[u8], at: usize) -> Result<u32> {
    let b = d.get(at..at + 4).ok_or(HeapError::Redo("short data"))?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn u64_at(d: &[u8], at: usize) -> Result<u64> {
    let b = d.get(at..at + 8).ok_or(HeapError::Redo("short data"))?;
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    Ok(u64::from_le_bytes(a))
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

/// Iterator over the tuples visible to a snapshot, page at a time.
pub struct HeapMvccScan<'a> {
    heap: &'a HeapFile,
    snap: Snapshot,
    me: Me,
    tm: &'a dyn TransactionManager,
    nblocks: u32,
    next_blk: u32,
    buffered: std::vec::IntoIter<(Tid, Vec<u8>)>,
}

impl Iterator for HeapMvccScan<'_> {
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
            match self.heap.read_page_mvcc(blk, &self.snap, &self.me, self.tm) {
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
        return Err(too_large(tuple));
    }
    TupleHeader::decode(tuple)?;
    Ok(())
}

fn too_large(tuple: &[u8]) -> HeapError {
    TupleError::TooLarge {
        len: tuple.len(),
        max: MAX_TUPLE_SIZE,
    }
    .into()
}

/// A tuple just added to a page.
struct Placed {
    tid: Tid,
    /// The page was all-zero and initialized by this insert.
    initialized: bool,
    /// The tuple as stored (ctid set).
    stored: Vec<u8>,
}

fn insert_block(rel: RelFileId, p: &Placed) -> BlockRef {
    let mut b = BlockRef::new(rel, p.tid.blk);
    b.will_init = p.initialized;
    b.data = p.tid.slot.0.to_le_bytes().to_vec();
    b.data.extend_from_slice(&p.stored);
    b
}

fn xmax_data(slot: SlotId, xmax: Xid, cmax: CommandId) -> Vec<u8> {
    let mut d = Vec::with_capacity(14);
    d.extend_from_slice(&slot.0.to_le_bytes());
    d.extend_from_slice(&xmax.0.to_le_bytes());
    d.extend_from_slice(&cmax.0.to_le_bytes());
    d
}

/// Adds `tuple` to the locked page `blk` (initializing an all-zero page),
/// sets its ctid to itself and marks the page dirty. `None` if it does not
/// fit.
fn place(page: &mut PageMut<'_>, blk: BlockNumber, tuple: &[u8]) -> Result<Option<Placed>> {
    let initialized = is_new_page(page);
    if initialized {
        SlottedPage::init(&mut page[..], PageKind::Heap, 0)?;
        page.mark_dirty();
    }
    let mut p = SlottedPage::open(&mut page[..])?;
    let Some(slot) = p.add_item(tuple)? else {
        return Ok(None);
    };
    let tid = Tid { blk, slot };
    let item = p
        .get_item_mut(slot)
        .ok_or(HeapError::Redo("added tuple missing"))?;
    tuple::set_ctid(item, tid)?;
    let stored = item.to_vec();
    page.mark_dirty();
    Ok(Some(Placed {
        tid,
        initialized,
        stored,
    }))
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
        // Each insert holds at most one pin at a time; 4 frames of slack.
        const THREADS: i64 = 4;
        let (pool, heap) = setup(THREADS as usize + 4);
        let text = "c".repeat(200);
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let (heap, text) = (&heap, &text);
                s.spawn(move || {
                    for i in 0..500 {
                        match heap.insert(&row(t * 1000 + i, text)) {
                            Ok(_) => {}
                            Err(HeapError::Buffer(BufferError::Exhausted)) => {
                                panic!("Exhausted with a pool sized for {THREADS} threads")
                            }
                            Err(e) => panic!("insert: {e}"),
                        }
                    }
                });
            }
        });
        let ids: HashSet<i64> = heap.scan().unwrap().map(|r| id_of(&r.unwrap().1)).collect();
        assert_eq!(ids.len(), 2000);
        assert!((0..THREADS).all(|t| (0..500).all(|i| ids.contains(&(t * 1000 + i)))));
        pool.flush_all().unwrap();
    }

    #[test]
    fn hint_bits_set_when_lock_is_free() {
        use crate::access::tuple::infomask;
        let (pool, heap) = setup(2);
        let tid = heap.insert(&row(1, "h")).unwrap();
        pool.flush_all().unwrap();
        let tag = BufferTag {
            rel: REL,
            blk: tid.blk,
        };

        assert!(heap.set_hint_bits(tid, infomask::XMIN_COMMITTED).unwrap());
        let h = TupleHeader::decode(&heap.fetch(tid).unwrap().unwrap()).unwrap();
        assert_ne!(h.infomask & infomask::XMIN_COMMITTED, 0);
        assert_eq!(pool.is_dirty(tag), Some(true));

        // Setting bits that are already set does not dirty the page.
        pool.flush_all().unwrap();
        assert!(heap.set_hint_bits(tid, infomask::XMIN_COMMITTED).unwrap());
        assert_eq!(pool.is_dirty(tag), Some(false));
    }

    #[test]
    fn hint_bits_skipped_under_shared_lock() {
        use crate::access::tuple::infomask;
        let (pool, heap) = setup(2);
        let tid = heap.insert(&row(1, "h")).unwrap();
        let tag = BufferTag {
            rel: REL,
            blk: tid.blk,
        };
        let pin = pool.fetch(tag).unwrap();
        let reader = pin.read();
        assert!(pin.try_write().is_none());
        assert!(!heap.set_hint_bits(tid, infomask::XMAX_INVALID).unwrap());
        drop(reader);

        assert!(heap.set_hint_bits(tid, infomask::XMIN_INVALID).unwrap());
        assert!(matches!(
            heap.set_hint_bits(tid, infomask::HAS_NULL),
            Err(HeapError::Tuple(TupleError::Corrupt(_)))
        ));
        let missing = Tid {
            blk: tid.blk,
            slot: SlotId(5),
        };
        assert!(matches!(
            heap.set_hint_bits(missing, infomask::XMIN_COMMITTED),
            Err(HeapError::TupleNotFound { .. })
        ));
    }

    // ---- WAL ----

    use crate::access::tuple::infomask;
    use crate::storage::backend::{FileKey, FixedFence};
    use crate::storage::page::{Lsn, PAGE_SIZE, page_lsn, set_page_lsn};
    use crate::wal::log::{FIRST_LSN, xlog_info};
    use crate::wal::{Wal, WalConfig, WalReader};
    use std::collections::HashMap;

    fn wal_cfg() -> WalConfig {
        WalConfig {
            timeline: 1,
            segment_size: 64 * 1024,
        }
    }

    fn setup_wal(frames: usize) -> (Arc<MemBackend>, Arc<BufferPool>, HeapFile) {
        let mem = Arc::new(MemBackend::default());
        let disk = Arc::new(DiskManager::new(mem.clone()));
        disk.create_relation(REL, FENCE).unwrap();
        let wal =
            Arc::new(Wal::create(mem.clone(), Arc::new(FixedFence(FENCE)), wal_cfg()).unwrap());
        let pool = Arc::new(
            BufferPool::new(frames, disk, Arc::new(SingleModeFlush::new(FENCE)))
                .unwrap()
                .with_wal(wal),
        );
        let heap = HeapFile::open(REL, pool.clone()).unwrap();
        (mem, pool, heap)
    }

    fn records(mem: &Arc<MemBackend>) -> Vec<(Lsn, crate::wal::record::Record)> {
        let mut rd = WalReader::new(mem.clone(), wal_cfg(), FIRST_LSN, Some(Lsn::INVALID)).unwrap();
        let mut out = Vec::new();
        while let Some((_, end, d)) = rd.next_record().unwrap() {
            out.push((end, d.record));
        }
        out
    }

    /// Minimal redo loop over all WAL, starting from empty pages.
    fn replay(mem: &Arc<MemBackend>) -> HashMap<u32, Vec<u8>> {
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
        for (end, rec) in records(mem) {
            for (i, b) in rec.blocks.iter().enumerate() {
                let page = pages.entry(b.blk.0).or_insert_with(|| vec![0u8; PAGE_SIZE]);
                if let Some(img) = &b.image {
                    page.copy_from_slice(img);
                } else if page_lsn(page) < end {
                    assert_eq!(rec.rmgr, RmgrId::Heap);
                    redo(rec.info, i, b, page).unwrap();
                } else {
                    continue;
                }
                set_page_lsn(page, end);
            }
        }
        pages
    }

    fn live_page(pool: &BufferPool, blk: u32) -> Vec<u8> {
        let pin = pool
            .fetch(BufferTag {
                rel: REL,
                blk: BlockNumber(blk),
            })
            .unwrap();
        let mut p = pin.read().to_vec();
        p[8..12].fill(0); // checksum is stamped on write copies only
        p
    }

    #[test]
    fn wal_replay_reproduces_pages() {
        let (mem, pool, heap) = setup_wal(64);
        let big = "u".repeat(900);
        let mut tids = Vec::new();
        for i in 0..60 {
            tids.push(heap.insert(&row(i, &big)).unwrap());
        }
        for (n, &t) in tids.iter().enumerate().step_by(5) {
            heap.delete(t, Xid(100 + n as u64), CommandId(1)).unwrap();
        }
        let live = |t: Tid| {
            heap.fetch(t)
                .unwrap()
                .is_some_and(|b| !TupleHeader::decode(&b).unwrap().xmax.is_valid())
        };
        // Small new versions stay on the page; large ones move elsewhere.
        for &t in tids.iter().skip(1).step_by(7) {
            if live(t) {
                heap.update(t, &row(7, "short"), Xid(200), CommandId(2))
                    .unwrap();
            }
        }
        for &t in tids.iter().skip(3).step_by(11) {
            if live(t) {
                heap.update(t, &row(8, &"w".repeat(3000)), Xid(300), CommandId(3))
                    .unwrap();
            }
        }
        pool.wal().unwrap().flush_all();

        let replayed = replay(&mem);
        let n = pool.nblocks(REL).unwrap();
        assert!(n > 5);
        for blk in 0..n {
            let mut want = replayed
                .get(&blk)
                .cloned()
                .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
            want[8..12].fill(0);
            assert_eq!(
                live_page(&pool, blk),
                want,
                "block {blk} differs after replay"
            );
        }
    }

    #[test]
    fn cross_page_update_is_one_record() {
        let (mem, pool, heap) = setup_wal(8);
        let fill = "f".repeat(2500);
        let t = heap.insert(&row(1, &fill)).unwrap();
        heap.insert(&row(2, &fill)).unwrap();
        heap.insert(&row(3, &fill)).unwrap(); // page 0 nearly full
        let new = heap
            .update(t, &row(1, &fill), Xid(9), CommandId(0))
            .unwrap();
        assert_ne!(new.blk, t.blk);
        pool.wal().unwrap().flush_all();
        let recs = records(&mem);
        let (_, upd) = recs.iter().find(|(_, r)| r.info == info::UPDATE).unwrap();
        assert_eq!(upd.blocks.len(), 2);
        assert_eq!((upd.blocks[0].blk, upd.blocks[1].blk), (t.blk, new.blk));
        assert!(upd.blocks[1].will_init, "fresh page is rebuilt by redo");
    }

    #[test]
    fn full_page_image_on_first_change_after_checkpoint() {
        let (mem, pool, heap) = setup_wal(8);
        let a = heap.insert(&row(1, "a")).unwrap();
        let b = heap.insert(&row(2, "b")).unwrap();
        let wal = pool.wal().unwrap().clone();
        wal.begin_checkpoint();
        heap.delete(a, Xid(5), CommandId(0)).unwrap();
        heap.delete(b, Xid(5), CommandId(0)).unwrap();
        wal.flush_all();
        let dels: Vec<_> = records(&mem)
            .into_iter()
            .filter(|(_, r)| r.rmgr == RmgrId::Heap && r.info == info::DELETE)
            .collect();
        assert_eq!(dels.len(), 2);
        assert!(
            dels[0].1.blocks[0].image.is_some(),
            "first change after redo point"
        );
        assert!(
            dels[1].1.blocks[0].image.is_none(),
            "page already logged since"
        );
        // The first insert initialized the page: no image needed there.
        let first = &records(&mem)[0].1;
        assert!(first.blocks[0].will_init && first.blocks[0].image.is_none());
    }

    #[test]
    fn hint_bits_log_one_image_per_checkpoint() {
        let (mem, pool, heap) = setup_wal(8);
        let t = heap.insert(&row(1, "h")).unwrap();
        let wal = pool.wal().unwrap().clone();
        wal.begin_checkpoint();
        let before = page_lsn(&live_page(&pool, t.blk.0));
        assert!(heap.set_hint_bits(t, infomask::XMIN_COMMITTED).unwrap());
        let after = page_lsn(&live_page(&pool, t.blk.0));
        assert!(after > before, "hint image advanced the page LSN");
        assert!(heap.set_hint_bits(t, infomask::XMAX_INVALID).unwrap());
        wal.flush_all();
        let fpis = records(&mem)
            .into_iter()
            .filter(|(_, r)| r.rmgr == RmgrId::Xlog && r.info == xlog_info::FPI_FOR_HINT)
            .count();
        assert_eq!(fpis, 1);
    }

    #[test]
    fn page_write_flushes_wal_first() {
        let (mem, pool, heap) = setup_wal(8);
        let t = heap.insert(&row(1, "x")).unwrap();
        let wal = pool.wal().unwrap().clone();
        let lsn = page_lsn(&live_page(&pool, t.blk.0));
        assert!(wal.flushed_lsn() < lsn);
        pool.flush_all().unwrap();
        assert!(wal.flushed_lsn() >= lsn);
        let on_disk = mem.raw(FileKey::Relation(REL), 0, PAGE_SIZE);
        assert_eq!(page_lsn(&on_disk), lsn);
    }

    // ---- MVCC ----

    mod mvcc {
        use super::super::*;
        use crate::access::tuple::{TupleDesc, deform_tuple, form_tuple, infomask};
        use crate::buffer::SingleModeFlush;
        use crate::lock::{LockManager, WaitPolicy};
        use crate::storage::backend::{FenceToken, FixedFence};
        use crate::storage::mem::MemBackend;
        use crate::types::{DataType, Value};
        use crate::wal::{Engine, WalConfig, initdb};
        use std::collections::BTreeSet;
        use std::time::Duration;

        const FENCE: FenceToken = FenceToken::new(1);
        const REL: RelFileId = RelFileId(16384);

        fn cfg() -> WalConfig {
            WalConfig {
                timeline: 1,
                segment_size: 256 * 1024,
            }
        }

        struct Env {
            mem: Arc<MemBackend>,
            engine: Engine,
            locks: Arc<LockManager>,
            heap: HeapFile,
        }

        fn start(mem: &Arc<MemBackend>) -> Env {
            let engine = Engine::start(
                mem.clone(),
                Arc::new(SingleModeFlush::new(FENCE)),
                cfg(),
                64,
            )
            .unwrap();
            let heap = HeapFile::open(REL, engine.pool().clone()).unwrap();
            Env {
                mem: mem.clone(),
                engine,
                locks: Arc::new(LockManager::new()),
                heap,
            }
        }

        fn env() -> Env {
            let mem = Arc::new(MemBackend::default());
            initdb(mem.clone(), Arc::new(FixedFence(FENCE)), cfg()).unwrap();
            let engine = Engine::start(
                mem.clone(),
                Arc::new(SingleModeFlush::new(FENCE)),
                cfg(),
                64,
            )
            .unwrap();
            engine.create_relation(REL).unwrap();
            drop(engine);
            start(&mem)
        }

        impl Env {
            fn begin(&self, iso: Isolation) -> Transaction {
                let tm: Arc<dyn TransactionManager> = self.engine.txn().clone();
                Transaction::begin(
                    tm,
                    self.locks.clone(),
                    iso,
                    WaitPolicy {
                        lock_timeout: Some(Duration::from_secs(5)),
                        deadlock_timeout: Duration::from_millis(100),
                    },
                )
            }

            fn insert(&self, txn: &mut Transaction, id: i64) -> Tid {
                let xid = txn.write_xid().unwrap();
                self.heap.insert(&row(id, xid, txn.command_id())).unwrap()
            }

            /// Ids visible to `txn`'s current statement snapshot.
            fn ids(&self, txn: &mut Transaction) -> BTreeSet<i64> {
                let snap = txn.snapshot();
                let me = Me {
                    xid: txn.xid(),
                    cid: txn.command_id(),
                };
                self.heap
                    .scan_mvcc(snap, me, txn.tm())
                    .unwrap()
                    .map(|r| id_of(&r.unwrap().1))
                    .collect()
            }

            fn infomask(&self, tid: Tid) -> u16 {
                TupleHeader::decode(&self.heap.fetch(tid).unwrap().unwrap())
                    .unwrap()
                    .infomask
            }
        }

        fn desc() -> TupleDesc {
            TupleDesc::new(vec![DataType::Int8]).unwrap()
        }

        fn row(id: i64, xid: Xid, cid: CommandId) -> Vec<u8> {
            form_tuple(&desc(), &[Value::Int8(id)], xid, cid).unwrap()
        }

        fn id_of(t: &[u8]) -> i64 {
            match deform_tuple(&desc(), t).unwrap()[0] {
                Value::Int8(i) => i,
                _ => panic!("bad row"),
            }
        }

        fn set(ids: &[i64]) -> BTreeSet<i64> {
            ids.iter().copied().collect()
        }

        /// A committed row.
        fn committed_row(e: &Env, id: i64) -> Tid {
            let mut t = e.begin(Isolation::ReadCommitted);
            let tid = e.insert(&mut t, id);
            t.commit().unwrap();
            tid
        }

        #[test]
        fn insert_visibility_by_snapshot_and_command() {
            let e = env();
            let mut t1 = e.begin(Isolation::ReadCommitted);
            let mut rc = e.begin(Isolation::ReadCommitted);
            let mut si = e.begin(Isolation::SnapshotIsolation);
            assert!(e.ids(&mut si).is_empty()); // SI snapshot taken now
            e.insert(&mut t1, 1);
            assert!(e.ids(&mut t1).is_empty(), "current command's insert");
            t1.next_command();
            assert_eq!(e.ids(&mut t1), set(&[1]));
            rc.statement_snapshot();
            assert!(e.ids(&mut rc).is_empty(), "uncommitted");
            t1.commit().unwrap();
            assert!(e.ids(&mut rc).is_empty(), "same statement, same snapshot");
            rc.statement_snapshot();
            assert_eq!(e.ids(&mut rc), set(&[1]), "new statement sees the commit");
            si.statement_snapshot();
            assert!(e.ids(&mut si).is_empty(), "SI keeps its first snapshot");
        }

        #[test]
        fn aborted_rows_stay_invisible_and_get_hints() {
            let e = env();
            let mut t1 = e.begin(Isolation::ReadCommitted);
            let a = e.insert(&mut t1, 1);
            t1.abort().unwrap();
            let c = committed_row(&e, 2);
            let mut r = e.begin(Isolation::ReadCommitted);
            assert_eq!(e.ids(&mut r), set(&[2]));
            assert_ne!(e.infomask(a) & infomask::XMIN_INVALID, 0);
            assert_ne!(e.infomask(c) & infomask::XMIN_COMMITTED, 0);
        }

        #[test]
        fn concurrent_delete_waits_for_the_other_transaction() {
            for commit_first in [true, false] {
                let e = env();
                let tid = committed_row(&e, 1);
                let mut t1 = e.begin(Isolation::ReadCommitted);
                assert_eq!(e.heap.delete_mvcc(tid, &mut t1).unwrap(), TmResult::Ok(tid));
                let x1 = t1.xid().unwrap();
                let mut t2 = e.begin(Isolation::ReadCommitted);
                assert_eq!(
                    e.heap.delete_mvcc(tid, &mut t2).unwrap(),
                    TmResult::BeingModified(x1)
                );
                let r = std::thread::scope(|s| {
                    let h = s.spawn(|| e.heap.delete_waiting(tid, &mut t2).unwrap());
                    std::thread::sleep(Duration::from_millis(50));
                    if commit_first {
                        t1.commit().unwrap();
                    } else {
                        t1.abort().unwrap();
                    }
                    h.join().unwrap()
                });
                if commit_first {
                    assert_eq!(r, TmResult::Deleted { xmax: x1 });
                } else {
                    assert_eq!(r, TmResult::Ok(tid), "aborted xmax is overwritten");
                }
            }
        }

        #[test]
        fn snapshot_isolation_first_updater_wins() {
            let e = env();
            let tid = committed_row(&e, 1);
            let mut si = e.begin(Isolation::SnapshotIsolation);
            assert_eq!(e.ids(&mut si), set(&[1]));
            let mut t1 = e.begin(Isolation::ReadCommitted);
            let x1 = t1.write_xid().unwrap();
            e.heap
                .update_mvcc(tid, &row(10, x1, t1.command_id()), &mut t1)
                .unwrap();
            t1.commit().unwrap();
            let xs = si.write_xid().unwrap();
            let err = e
                .heap
                .update_waiting(tid, &row(20, xs, si.command_id()), &mut si)
                .unwrap_err();
            assert!(matches!(
                err,
                HeapError::Transaction(TransactionError::SerializationFailure)
            ));
        }

        #[test]
        fn read_committed_follows_update_chain() {
            let e = env();
            let tid = committed_row(&e, 1);
            let mut t1 = e.begin(Isolation::ReadCommitted);
            let x1 = t1.write_xid().unwrap();
            let TmResult::Ok(v2) = e
                .heap
                .update_mvcc(tid, &row(2, x1, t1.command_id()), &mut t1)
                .unwrap()
            else {
                panic!("update failed");
            };
            t1.commit().unwrap();

            let mut t2 = e.begin(Isolation::ReadCommitted);
            let x2 = t2.write_xid().unwrap();
            let r = e
                .heap
                .update_waiting(tid, &row(3, x2, t2.command_id()), &mut t2)
                .unwrap();
            assert_eq!(r, TmResult::Updated { ctid: v2, xmax: x1 });
            let latest = e.heap.latest_version(tid, t2.tm()).unwrap();
            assert_eq!(latest, Some(v2));
            let r = e
                .heap
                .update_waiting(v2, &row(3, x2, t2.command_id()), &mut t2)
                .unwrap();
            assert!(matches!(r, TmResult::Ok(_)));
            t2.commit().unwrap();
            let mut r = e.begin(Isolation::ReadCommitted);
            assert_eq!(e.ids(&mut r), set(&[3]));
        }

        #[test]
        fn own_modifications_by_command() {
            let e = env();
            let tid = committed_row(&e, 1);
            let mut t = e.begin(Isolation::ReadCommitted);
            assert_eq!(e.heap.delete_mvcc(tid, &mut t).unwrap(), TmResult::Ok(tid));
            assert_eq!(
                e.heap.delete_mvcc(tid, &mut t).unwrap(),
                TmResult::SelfModified
            );
            t.next_command();
            assert_eq!(
                e.heap.delete_mvcc(tid, &mut t).unwrap(),
                TmResult::Invisible
            );
            assert!(e.ids(&mut t).is_empty());
        }

        #[test]
        fn snapshots_hold_the_vacuum_horizon() {
            let e = env();
            let tm = e.engine.txn().clone();
            let mut si = e.begin(Isolation::SnapshotIsolation);
            let mut rc = e.begin(Isolation::ReadCommitted);
            si.statement_snapshot();
            rc.statement_snapshot();
            let held = tm.oldest_xmin();
            for i in 0..3 {
                committed_row(&e, i);
            }
            rc.statement_snapshot();
            si.statement_snapshot();
            assert_eq!(tm.oldest_xmin(), held, "SI keeps its first snapshot's xmin");
            si.commit().unwrap();
            rc.statement_snapshot();
            let advanced = tm.oldest_xmin();
            assert!(advanced > held, "RC moves its xmin with each statement");
            rc.commit().unwrap();
            assert_eq!(tm.oldest_xmin(), tm.next_xid(), "nothing holds it back");
        }

        fn header_at(e: &Env, tid: Tid) -> TupleHeader {
            TupleHeader::decode(&e.heap.fetch(tid).unwrap().unwrap()).unwrap()
        }

        #[test]
        fn vacuum_classification() {
            let e = env();
            let tm = e.engine.txn().clone();
            let live = committed_row(&e, 1);
            let deleted = committed_row(&e, 2);
            let recent = committed_row(&e, 3);
            let mut ab = e.begin(Isolation::ReadCommitted);
            let aborted = e.insert(&mut ab, 4);
            ab.abort().unwrap();
            let mut d = e.begin(Isolation::ReadCommitted);
            e.heap.delete_mvcc(deleted, &mut d).unwrap();
            d.commit().unwrap();
            let mut holder = e.begin(Isolation::SnapshotIsolation);
            holder.statement_snapshot(); // sees row 3 as live from here on
            let mut d = e.begin(Isolation::ReadCommitted);
            e.heap.delete_mvcc(recent, &mut d).unwrap();
            d.commit().unwrap();
            let mut running = e.begin(Isolation::ReadCommitted);
            let ins = e.insert(&mut running, 5);
            let mut del = e.begin(Isolation::ReadCommitted);
            let pending = committed_row(&e, 6);
            e.heap.delete_mvcc(pending, &mut del).unwrap();

            let h = tm.oldest_xmin();
            let st = |tid| satisfies_vacuum(&header_at(&e, tid), h, tm.as_ref()).unwrap();
            assert_eq!(st(live), VacuumState::Live);
            assert_eq!(st(deleted), VacuumState::Dead);
            assert_eq!(st(recent), VacuumState::RecentlyDead);
            assert_eq!(st(aborted), VacuumState::Dead);
            assert_eq!(st(ins), VacuumState::InsertInProgress);
            assert_eq!(st(pending), VacuumState::DeleteInProgress);
            holder.commit().unwrap();
            running.commit().unwrap();
            del.commit().unwrap();
            let h = tm.oldest_xmin();
            let st = |tid| satisfies_vacuum(&header_at(&e, tid), h, tm.as_ref()).unwrap();
            assert_eq!(
                st(recent),
                VacuumState::Dead,
                "no snapshot holds it any more"
            );
        }

        #[test]
        fn prune_frees_space_and_survives_crash() {
            let e = env();
            let tm = e.engine.txn().clone();
            let tids: Vec<Tid> = (0..40).map(|i| committed_row(&e, i)).collect();
            let mut d = e.begin(Isolation::ReadCommitted);
            for &t in tids.iter().step_by(2) {
                e.heap.delete_mvcc(t, &mut d).unwrap();
            }
            d.commit().unwrap();
            let blk = tids[0].blk;
            let free = |e: &Env| {
                let pin = e.engine.pool().fetch(BufferTag { rel: REL, blk }).unwrap();
                let page = pin.read();
                SlottedPage::open(&page[..]).unwrap().free_space()
            };
            let before = free(&e);
            let dead = e
                .heap
                .prune_page(blk, tm.oldest_xmin(), tm.as_ref())
                .unwrap();
            assert_eq!(dead.len(), 20);
            assert!(free(&e) > before);
            assert!(dead.iter().all(|t| e.heap.fetch(*t).unwrap().is_none()));
            let mut r = e.begin(Isolation::ReadCommitted);
            let want: BTreeSet<i64> = (0..40).filter(|i| i % 2 == 1).collect();
            assert_eq!(e.ids(&mut r), want);
            drop(r);
            // Pruning again finds the same DEAD slots and logs nothing new.
            assert_eq!(
                e.heap
                    .prune_page(blk, tm.oldest_xmin(), tm.as_ref())
                    .unwrap(),
                dead
            );

            e.engine.wal().flush_all();
            let mem = e.mem.clone();
            drop(e);
            let e = start(&mem);
            let mut r = e.begin(Isolation::ReadCommitted);
            assert_eq!(e.ids(&mut r), want);
            let tm = e.engine.txn().clone();
            assert_eq!(
                e.heap
                    .prune_page(blk, tm.oldest_xmin(), tm.as_ref())
                    .unwrap(),
                dead
            );
        }

        #[test]
        fn uncommitted_rows_are_invisible_after_crash() {
            let e = env();
            committed_row(&e, 1);
            let mut open = e.begin(Isolation::ReadCommitted);
            e.insert(&mut open, 2);
            let open_xid = open.xid().unwrap();
            let mem = e.mem.clone();
            committed_row(&e, 3); // flushes WAL past the uncommitted insert
            std::mem::forget(open); // crash: no abort record
            drop(e);
            let e = start(&mem);
            let mut r = e.begin(Isolation::ReadCommitted);
            assert_eq!(e.ids(&mut r), set(&[1, 3]));
            assert!(
                e.engine.txn().next_xid() > open_xid,
                "xid that wrote a row is not reused"
            );
        }
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
