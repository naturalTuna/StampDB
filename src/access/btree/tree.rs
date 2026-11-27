//! B+Tree operations with Lehman-Yao concurrency.
//!
//! * Descent holds one content lock at a time. If the target is not below a
//!   page's high key, the page has split since its parent was read: release
//!   it and follow the right-link ("move right").
//! * Insert remembers the parents it passed. A split builds the left and new
//!   right pages under both locks and marks the left page INCOMPLETE_SPLIT.
//!   It then locks the parent (moving right in the parent level if it split
//!   too), releases the right page, and inserts the downlink; that insert
//!   clears the flag on the still-locked left page in the same WAL record.
//!   Locks are taken bottom-up and left-to-right only, so writers cannot
//!   deadlock (as in PostgreSQL's nbtree).
//! * A split of the root creates a new root and updates the metapage while
//!   still holding the old root's halves.
//! * After a crash between a split and its downlink, the left page keeps
//!   INCOMPLETE_SPLIT. Searches are unaffected (right-links), and an insert
//!   that locks such a page, or passes it while looking for a parent,
//!   inserts the missing downlink first (PostgreSQL's `_bt_finish_split`).
//! * Scans copy one leaf at a time and remember its right-link, so a later
//!   split of that leaf only moves entries that were already copied.
//!
//! Pins: an insert keeps every page of its split path pinned until it
//! returns, up to about two per level plus the metapage.
//!
//! WAL: every change is logged while its pages are locked (see [`info`]);
//! pages built from scratch by a split, a new root, or index creation are
//! logged as full images. [`redo`] applies the other block references.

use std::ops::Bound;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use super::key::{TID_LEN, decode_tid, encode_tid};
use super::page::{
    CHILD_LEN, MAX_ITEM_SIZE, Meta, Node, Opaque, P_NONE, USABLE, build_meta, build_page, flags,
    internal_item, item_cost,
};
use super::{BTreeError, Result};
use crate::access::tuple::Tid;
use crate::buffer::{BufferPool, BufferTag, PageMut, PinGuard};
use crate::storage::backend::RelFileId;
use crate::storage::page::BlockNumber;
use crate::txn::Xid;
use crate::wal::log_and_stamp;
use crate::wal::record::{BlockRef, Record, RmgrId};

const META_BLOCK: u32 = 0;

/// `info` values of B+Tree WAL records.
pub mod info {
    /// Block 0: `slot u16 | item`. Optional block 1: the child whose split
    /// this downlink completes (clear INCOMPLETE_SPLIT).
    pub const INSERT: u8 = 0;
    /// Block 0: `slot u16`.
    pub const DELETE: u8 = 1;
    /// Block 0 left, block 1 right (images). Main data: one flags byte;
    /// then, if flagged, the old right sibling (`prev u32`) and the child
    /// whose split this split's downlink completes (clear the flag).
    pub const SPLIT: u8 = 2;
    /// Block 0 new root (image), block 1 old root (clear ROOT and
    /// INCOMPLETE_SPLIT), block 2 metapage (`root u32 | level u32`).
    pub const NEWROOT: u8 = 3;
    /// Block 0 metapage, block 1 root leaf (images).
    pub const CREATE: u8 = 4;
    /// Block 0: slots removed by vacuum, u16 each, in removal order.
    pub const BULK_DELETE: u8 = 5;
    /// Block 0 parent: `slot u16`; its downlink takes over the next item's
    /// child, which is removed. Block 1: the leaf, now HALF_DEAD.
    pub const MARK_HALFDEAD: u8 = 6;
    /// Main data: has-left flag. Blocks: [left sibling (`next u32`)], the
    /// leaf (DELETED), the right sibling (`prev u32`).
    pub const UNLINK: u8 = 7;

    /// SPLIT main-data flag: an old right sibling block follows.
    pub const SPLIT_HAS_SIBLING: u8 = 0x01;
    /// SPLIT main-data flag: a child block follows.
    pub const SPLIT_HAS_CHILD: u8 = 0x02;
}

/// What the table says about the row an existing entry with the same key
/// points to (for unique checks; PostgreSQL checks with a dirty snapshot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueCheck {
    /// The row exists (committed, or written by the inserting transaction):
    /// the new key would be a duplicate.
    Live,
    /// The row is dead or was never committed: not a conflict.
    Ignore,
    /// A transaction that has not finished inserted or deleted the row:
    /// wait for it, then check again.
    Wait(Xid),
}

/// Result of [`BTree::insert_unique`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Duplicate,
    /// Nothing was inserted; wait for this transaction, then retry.
    Wait(Xid),
}

/// Progress of a unique insert along the chain of pages with the key.
enum Chain {
    Done(InsertOutcome),
    /// The entry belongs to a page further left.
    NotYet,
}

/// Next step of a descent.
enum Step {
    Done,
    Right(u32),
    Down(u32),
}

/// A B+Tree index stored in one relation file.
#[derive(Debug)]
pub struct BTree {
    rel: RelFileId,
    pool: Arc<BufferPool>,
    /// Test hook: return right after logging a split, as if the process
    /// died before the downlink was inserted.
    #[cfg(test)]
    stop_after_split: AtomicBool,
    /// Test hook: stop page deletion after the first phase.
    #[cfg(test)]
    stop_after_halfdead: AtomicBool,
}

impl BTree {
    fn new(rel: RelFileId, pool: Arc<BufferPool>) -> Self {
        Self {
            rel,
            pool,
            #[cfg(test)]
            stop_after_split: AtomicBool::new(false),
            #[cfg(test)]
            stop_after_halfdead: AtomicBool::new(false),
        }
    }

    /// Initializes an index in an empty relation file: metapage at block 0
    /// and an empty root leaf.
    pub fn create(rel: RelFileId, pool: Arc<BufferPool>) -> Result<Self> {
        if pool.nblocks(rel)? != 0 {
            return Err(BTreeError::Corrupt("relation is not empty"));
        }
        let (mblk, mpin) = pool.extend(rel)?;
        let (rblk, rpin) = pool.extend(rel)?;
        if mblk.0 != META_BLOCK {
            return Err(BTreeError::Corrupt("metapage is not block 0"));
        }
        {
            let mut r = rpin.write();
            let op = Opaque {
                prev: P_NONE,
                next: P_NONE,
                level: 0,
                flags: flags::LEAF | flags::ROOT,
            };
            build_page(&mut r, op, None, &[])?;
            r.mark_dirty();
            let mut m = mpin.write();
            build_meta(
                &mut m,
                &Meta {
                    root: rblk.0,
                    level: 0,
                },
            )?;
            m.mark_dirty();
            let mut rec = Record::new(RmgrId::Btree, info::CREATE);
            rec.blocks = vec![image_block(rel, mblk.0, &m), image_block(rel, rblk.0, &r)];
            log_and_stamp(pool.wal(), rec, &mut [&mut m[..], &mut r[..]]);
        }
        drop((rpin, mpin));
        Ok(Self::new(rel, pool))
    }

    /// Opens an existing index.
    pub fn open(rel: RelFileId, pool: Arc<BufferPool>) -> Result<Self> {
        let tree = Self::new(rel, pool);
        tree.meta()?;
        Ok(tree)
    }

    /// Number of levels (1 = the root is a leaf).
    pub fn height(&self) -> Result<u32> {
        Ok(self.meta()?.level + 1)
    }

    /// Inserts the entry `key || tid`. `key` is a memcomparable key.
    pub fn insert(&self, key: &[u8], tid: Tid) -> Result<()> {
        let entry = entry_key(key, tid)?;
        'restart: loop {
            let (mut pin, mut blk, mut stack) = self.descend(&entry)?;
            loop {
                let mut page = pin.write();
                let (incomplete, move_right, next) = {
                    let node = Node::open(&page[..])?;
                    (
                        node.opaque().flags & flags::INCOMPLETE_SPLIT != 0,
                        node.must_move_right(&entry)?,
                        node.opaque().next,
                    )
                };
                if incomplete {
                    self.finish_split(blk, &mut page, 0, stack.clone())?;
                    continue 'restart;
                }
                if move_right {
                    drop(page);
                    blk = next;
                    pin = self.fetch(next)?;
                    continue;
                }
                let pos = {
                    let node = Node::open(&page[..])?;
                    let pos = node.lower_bound(&entry)?;
                    if pos < node.slot_count() && node.key_at(pos)? == entry.as_slice() {
                        return Err(BTreeError::Duplicate);
                    }
                    pos
                };
                return self.insert_item(&pin, page, blk, pos, entry, &mut stack, None);
            }
        }
    }

    /// Inserts `key || tid` unless another entry with the same key points
    /// at a row `check` reports live (PostgreSQL's _bt_check_unique).
    ///
    /// The search uses the key alone to find the first leaf that can hold
    /// it and walks right, locking page after page (left to right) while
    /// equal keys continue; the entry is inserted into the right one of
    /// those pages while they are all still locked, so a concurrent insert
    /// of the same key must see it. On `Wait`, nothing was inserted and no
    /// lock is held.
    pub fn insert_unique(
        &self,
        key: &[u8],
        tid: Tid,
        check: &dyn Fn(Tid) -> Result<UniqueCheck>,
    ) -> Result<InsertOutcome> {
        let entry = entry_key(key, tid)?;
        'restart: loop {
            let (mut pin, mut blk, stack) = self.descend(key)?;
            loop {
                let mut page = pin.write();
                let (incomplete, move_right, next) = {
                    let node = Node::open(&page[..])?;
                    (
                        node.opaque().flags & flags::INCOMPLETE_SPLIT != 0,
                        node.must_move_right(key)?,
                        node.opaque().next,
                    )
                };
                if incomplete {
                    self.finish_split(blk, &mut page, 0, stack.clone())?;
                    continue 'restart;
                }
                if move_right {
                    drop(page);
                    blk = next;
                    pin = self.fetch(next)?;
                    continue;
                }
                return match self.unique_chain(&pin, page, blk, None, key, &entry, &stack, check)? {
                    Chain::Done(r) => Ok(r),
                    Chain::NotYet => Err(BTreeError::Corrupt("no page accepted the unique entry")),
                };
            }
        }
    }

    /// One page of the unique-key chain: checks this page's entries with
    /// `key`, continues right (still holding this page) while the key may
    /// continue, then inserts here if the entry belongs to this page.
    /// `low` is the previous page's high key (this page's lower bound).
    #[allow(clippy::too_many_arguments)]
    fn unique_chain(
        &self,
        pin: &PinGuard<'_>,
        page: PageMut<'_>,
        blk: u32,
        low: Option<Vec<u8>>,
        key: &[u8],
        entry: &[u8],
        stack: &[u32],
        check: &dyn Fn(Tid) -> Result<UniqueCheck>,
    ) -> Result<Chain> {
        let (ignored, high, next, more) = {
            let node = Node::open(&page[..])?;
            let op = node.opaque();
            let ignored = op.flags & (flags::HALF_DEAD | flags::DELETED) != 0;
            let mut ended = false;
            if !ignored {
                for s in node.lower_bound(key)?..node.slot_count() {
                    let e = node.key_at(s)?;
                    let cut = e
                        .len()
                        .checked_sub(TID_LEN)
                        .ok_or(BTreeError::Corrupt("leaf entry shorter than tid"))?;
                    if &e[..cut] != key {
                        ended = true;
                        break;
                    }
                    if e == entry {
                        return Err(BTreeError::Duplicate);
                    }
                    match check(decode_tid(&e[cut..])?)? {
                        UniqueCheck::Live => return Ok(Chain::Done(InsertOutcome::Duplicate)),
                        UniqueCheck::Wait(x) => return Ok(Chain::Done(InsertOutcome::Wait(x))),
                        UniqueCheck::Ignore => {}
                    }
                }
            }
            let high = node.high_key()?.map(<[u8]>::to_vec);
            let key_continues = high
                .as_ref()
                .is_some_and(|h| h.len() >= TID_LEN && &h[..h.len() - TID_LEN] == key);
            let more = !op.is_rightmost() && (ignored || (!ended && key_continues));
            (ignored, high, op.next, more)
        };
        if more {
            let rpin = self.fetch(next)?;
            let rpage = rpin.write();
            let rlow = if ignored { low.clone() } else { high.clone() };
            if let Chain::Done(r) =
                self.unique_chain(&rpin, rpage, next, rlow, key, entry, stack, check)?
            {
                return Ok(Chain::Done(r));
            }
        }
        let belongs = !ignored
            && low.as_ref().is_none_or(|l| entry >= l.as_slice())
            && high.as_ref().is_none_or(|h| entry < h.as_slice());
        if !belongs {
            return Ok(Chain::NotYet);
        }
        let pos = Node::open(&page[..])?.lower_bound(entry)?;
        let mut stack = stack.to_vec();
        self.insert_item(pin, page, blk, pos, entry.to_vec(), &mut stack, None)?;
        Ok(Chain::Done(InsertOutcome::Inserted))
    }

    /// Removes the entry `key || tid`. Returns whether it existed.
    /// Pages are never merged; empty pages are left for vacuum.
    pub fn delete(&self, key: &[u8], tid: Tid) -> Result<bool> {
        let entry = entry_key(key, tid)?;
        let (mut pin, mut blk, _) = self.descend(&entry)?;
        loop {
            let mut page = pin.write();
            let mut node = Node::open(&mut page[..])?;
            if node.must_move_right(&entry)? {
                let next = node.opaque().next;
                drop(page);
                blk = next;
                pin = self.fetch(next)?;
                continue;
            }
            let pos = node.lower_bound(&entry)?;
            if pos < node.slot_count() && node.key_at(pos)? == entry.as_slice() {
                node.remove_at(pos)?;
                page.mark_dirty();
                let mut b = BlockRef::new(self.rel, BlockNumber(blk));
                b.data = pos.to_le_bytes().to_vec();
                let mut rec = Record::new(RmgrId::Btree, info::DELETE);
                rec.blocks.push(b);
                self.log(rec, &mut [&mut page[..]]);
                return Ok(true);
            }
            return Ok(false);
        }
    }

    /// Removes every leaf entry whose TID satisfies `dead` (vacuum's index
    /// cleanup; PostgreSQL's btbulkdelete). Returns how many were removed.
    ///
    /// Leaves are visited in block order up to the end of the file, re-read
    /// until it stops growing: a concurrent split only moves entries to a
    /// newly appended block (deleted pages are never reused), so no entry
    /// can move to a block already passed.
    pub fn bulk_delete(&self, dead: &dyn Fn(Tid) -> bool) -> Result<usize> {
        let mut removed = 0;
        let mut blk = META_BLOCK + 1;
        loop {
            let n = self.pool.nblocks(self.rel)?;
            if blk >= n {
                return Ok(removed);
            }
            while blk < n {
                removed += self.bulk_delete_page(blk, dead)?;
                blk += 1;
            }
        }
    }

    fn bulk_delete_page(&self, blk: u32, dead: &dyn Fn(Tid) -> bool) -> Result<usize> {
        let pin = self.fetch(blk)?;
        let mut page = pin.write();
        if crate::storage::page::is_new_page(&page) {
            return Ok(0);
        }
        let mut node = Node::open(&mut page[..])?;
        let op = node.opaque();
        if !op.is_leaf() || op.flags & (flags::DELETED | flags::HALF_DEAD) != 0 {
            return Ok(0);
        }
        let mut slots = Vec::new();
        for s in node.first_data()..node.slot_count() {
            let entry = node.key_at(s)?;
            let cut = entry
                .len()
                .checked_sub(TID_LEN)
                .ok_or(BTreeError::Corrupt("leaf entry shorter than tid"))?;
            if dead(decode_tid(&entry[cut..])?) {
                slots.push(s);
            }
        }
        // Highest first, so earlier slots keep their numbers.
        slots.reverse();
        for &s in &slots {
            node.remove_at(s)?;
        }
        if slots.is_empty() {
            return Ok(0);
        }
        page.mark_dirty();
        let mut b = BlockRef::new(self.rel, BlockNumber(blk));
        b.data = slots.iter().flat_map(|s| s.to_le_bytes()).collect();
        let mut rec = Record::new(RmgrId::Btree, info::BULK_DELETE);
        rec.blocks.push(b);
        self.log(rec, &mut [&mut page[..]]);
        Ok(slots.len())
    }

    /// Deletes empty leaf pages (after vacuum's bulk delete) in two phases,
    /// as PostgreSQL does, and finishes deletions a crash left half done.
    /// Only a leaf that is not the rightmost of its level and whose parent
    /// also holds its right sibling's downlink is deleted (DESIGN r8);
    /// deleted pages are not reused. Returns how many pages were deleted.
    pub fn delete_empty_pages(&self) -> Result<usize> {
        let mut deleted = 0;
        let n = self.pool.nblocks(self.rel)?;
        for blk in META_BLOCK + 1..n {
            let state = {
                let pin = self.fetch(blk)?;
                let page = pin.read();
                if crate::storage::page::is_new_page(&page) {
                    continue;
                }
                let node = Node::open(&page[..])?;
                let op = node.opaque();
                if !op.is_leaf() || op.flags & flags::DELETED != 0 {
                    continue;
                }
                if op.flags & flags::HALF_DEAD != 0 {
                    PageState::HalfDead
                } else if !op.is_rightmost() && node.slot_count() == node.first_data() {
                    PageState::Empty
                } else {
                    continue;
                }
            };
            if state == PageState::Empty && !self.mark_half_dead(blk)? {
                continue;
            }
            #[cfg(test)]
            if self.stop_after_halfdead.load(Ordering::SeqCst) {
                continue;
            }
            self.unlink(blk)?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Phase 1: takes the empty leaf's key range away from it. Locks the
    /// leaf, then its parent (bottom-up); the parent's downlink to the leaf
    /// is re-pointed at the right sibling, whose own downlink is removed.
    /// Returns false (doing nothing) if the leaf is no longer eligible.
    fn mark_half_dead(&self, blk: u32) -> Result<bool> {
        let pin = self.fetch(blk)?;
        let mut page = pin.write();
        let op = {
            let node = Node::open(&page[..])?;
            let op = node.opaque();
            let eligible = op.is_leaf()
                && op.flags & (flags::HALF_DEAD | flags::DELETED | flags::INCOMPLETE_SPLIT) == 0
                && !op.is_rightmost()
                && node.slot_count() == node.first_data();
            if !eligible {
                return Ok(false);
            }
            op
        };
        // Find the parent by walking the level above.
        let mut pblk = self.leftmost_at(1)?;
        loop {
            let ppin = self.fetch(pblk)?;
            let mut ppage = ppin.write();
            let (slot, next, pcount) = {
                let pnode = Node::open(&ppage[..])?;
                (
                    pnode.find_downlink(blk)?,
                    pnode.opaque().next,
                    pnode.slot_count(),
                )
            };
            let Some(slot) = slot else {
                if next == P_NONE {
                    return Ok(false); // no downlink (unfinished split): skip
                }
                drop(ppage);
                pblk = next;
                continue;
            };
            // The right sibling's downlink must follow in this parent.
            if slot + 1 >= pcount || Node::open(&ppage[..])?.child_at(slot + 1)? != op.next {
                return Ok(false);
            }
            redo_halfdead_parent(&mut ppage, slot)?;
            ppage.mark_dirty();
            set_flags(&mut page, flags::HALF_DEAD)?;
            page.mark_dirty();
            let mut pb = BlockRef::new(self.rel, BlockNumber(pblk));
            pb.data = slot.to_le_bytes().to_vec();
            let mut rec = Record::new(RmgrId::Btree, info::MARK_HALFDEAD);
            rec.blocks = vec![pb, BlockRef::new(self.rel, BlockNumber(blk))];
            self.log(rec, &mut [&mut ppage[..], &mut page[..]]);
            return Ok(true);
        }
    }

    /// Phase 2: unlinks the half-dead leaf from its siblings, locking left
    /// sibling, leaf, and right sibling left to right. The leaf keeps its
    /// right-link so a scan standing on it can still move right.
    fn unlink(&self, blk: u32) -> Result<()> {
        let prev = {
            let pin = self.fetch(blk)?;
            let page = pin.read();
            Node::open(&page[..])?.opaque().prev
        };
        // The left sibling may have split since: move right to the page
        // whose right-link is the leaf.
        let mut lpin = None;
        if prev != P_NONE {
            let mut lblk = prev;
            loop {
                let pin = self.fetch(lblk)?;
                let next = {
                    let page = pin.read();
                    Node::open(&page[..])?.opaque().next
                };
                if next == blk {
                    lpin = Some((lblk, pin));
                    break;
                }
                if next == P_NONE {
                    return Err(BTreeError::Corrupt(
                        "left sibling chain does not reach the page",
                    ));
                }
                lblk = next;
            }
        }
        let mut lpage = lpin.as_ref().map(|(_, p)| p.write());
        if let Some(lp) = lpage.as_ref()
            && Node::open(&lp[..])?.opaque().next != blk
        {
            // Split between our check and the lock: retry from scratch.
            drop(lpage);
            return self.unlink(blk);
        }
        let pin = self.fetch(blk)?;
        let mut page = pin.write();
        let op = Node::open(&page[..])?.opaque();
        if op.flags & flags::HALF_DEAD == 0 {
            return Ok(()); // someone finished it
        }
        let rpin = self.fetch(op.next)?;
        let mut rpage = rpin.write();

        let left_blk = lpin.as_ref().map(|(b, _)| *b).unwrap_or(P_NONE);
        if let Some(lp) = lpage.as_mut() {
            set_next(lp, op.next)?;
            lp.mark_dirty();
        }
        clear_flags(&mut page, flags::HALF_DEAD)?;
        set_flags(&mut page, flags::DELETED)?;
        page.mark_dirty();
        set_prev(&mut rpage, left_blk)?;
        rpage.mark_dirty();

        let mut rec = Record::new(RmgrId::Btree, info::UNLINK);
        let mut rb = BlockRef::new(self.rel, BlockNumber(op.next));
        rb.data = left_blk.to_le_bytes().to_vec();
        match lpage.as_mut() {
            Some(lp) => {
                let mut lb = BlockRef::new(self.rel, BlockNumber(left_blk));
                lb.data = op.next.to_le_bytes().to_vec();
                rec.main = vec![1];
                rec.blocks = vec![lb, BlockRef::new(self.rel, BlockNumber(blk)), rb];
                self.log(rec, &mut [&mut lp[..], &mut page[..], &mut rpage[..]]);
            }
            None => {
                rec.main = vec![0];
                rec.blocks = vec![BlockRef::new(self.rel, BlockNumber(blk)), rb];
                self.log(rec, &mut [&mut page[..], &mut rpage[..]]);
            }
        }
        Ok(())
    }

    /// Entries whose key lies within the bounds, in key order (then TID).
    /// Bounds are memcomparable keys without a TID.
    pub fn scan(&self, lower: Bound<Vec<u8>>, upper: Bound<Vec<u8>>) -> Result<BTreeScan<'_>> {
        Ok(BTreeScan {
            tree: self,
            lower,
            upper,
            state: ScanState::Start,
            buffered: Vec::new().into_iter(),
        })
    }

    fn fetch(&self, blk: u32) -> Result<PinGuard<'_>> {
        Ok(self.pool.fetch(BufferTag {
            rel: self.rel,
            blk: BlockNumber(blk),
        })?)
    }

    fn meta(&self) -> Result<Meta> {
        let pin = self.fetch(META_BLOCK)?;
        let page = pin.read();
        Meta::read(&page)
    }

    fn log(&self, rec: Record, pages: &mut [&mut [u8]]) {
        log_and_stamp(self.pool.wal(), rec, pages);
    }

    /// Descends to the leaf that may contain `target`. Returns it pinned
    /// but unlocked, with the internal pages passed on the way down.
    fn descend(&self, target: &[u8]) -> Result<(PinGuard<'_>, u32, Vec<u32>)> {
        let mut blk = self.meta()?.root;
        let mut pin = self.fetch(blk)?;
        let mut stack = Vec::new();
        loop {
            let step = {
                let page = pin.read();
                let node = Node::open(&page[..])?;
                if node.must_move_right(target)? {
                    Step::Right(node.opaque().next)
                } else if node.opaque().is_leaf() {
                    Step::Done
                } else {
                    Step::Down(node.choose_child(target)?)
                }
            };
            match step {
                Step::Done => return Ok((pin, blk, stack)),
                Step::Right(next) => blk = next,
                Step::Down(child) => {
                    stack.push(blk);
                    blk = child;
                }
            }
            pin = self.fetch(blk)?;
        }
    }

    /// Leftmost page of `level`, found by descending from the root. Used
    /// when a split has no remembered parent because the root split after
    /// this insert started its descent.
    fn leftmost_at(&self, level: u32) -> Result<u32> {
        let meta = self.meta()?;
        if meta.level < level {
            return Err(BTreeError::Corrupt("parent level does not exist"));
        }
        let mut blk = meta.root;
        loop {
            let pin = self.fetch(blk)?;
            let page = pin.read();
            let node = Node::open(&page[..])?;
            if node.opaque().level == level {
                return Ok(blk);
            }
            blk = node.child_at(node.first_data())?;
        }
    }

    /// Inserts `item` at `pos` on the write-locked page `blk`, splitting and
    /// recursing into the parent if needed. `_pin` is the caller's pin of
    /// that page and stays alive for the whole call. `child` is the
    /// locked page whose split this item (a downlink) completes.
    #[allow(clippy::too_many_arguments)]
    fn insert_item(
        &self,
        _pin: &PinGuard<'_>,
        mut page: PageMut<'_>,
        blk: u32,
        pos: u16,
        item: Vec<u8>,
        stack: &mut Vec<u32>,
        child: Option<(u32, &mut [u8])>,
    ) -> Result<()> {
        if Node::open(&mut page[..])?.insert_at(pos, &item)? {
            page.mark_dirty();
            let mut b = BlockRef::new(self.rel, BlockNumber(blk));
            b.data = pos.to_le_bytes().to_vec();
            b.data.extend_from_slice(&item);
            let mut rec = Record::new(RmgrId::Btree, info::INSERT);
            rec.blocks.push(b);
            match child {
                Some((cblk, cpage)) => {
                    clear_flags(cpage, flags::INCOMPLETE_SPLIT)?;
                    rec.blocks.push(BlockRef::new(self.rel, BlockNumber(cblk)));
                    self.log(rec, &mut [&mut page[..], cpage]);
                }
                None => self.log(rec, &mut [&mut page[..]]),
            }
            return Ok(());
        }

        let (rblk, rpin) = self.pool.extend(self.rel)?;
        let mut rpage = rpin.write();
        let (sep, op) = self.split(blk, &mut page, rblk.0, &mut rpage, pos, item, child)?;
        #[cfg(test)]
        if self.stop_after_split.load(Ordering::SeqCst) {
            return Ok(());
        }
        if op.is_root() {
            return self.new_root(blk, &mut page, rblk.0, &sep, op.level);
        }
        self.insert_downlink(blk, &mut page, &sep, rblk.0, op.level, stack, Some(rpage))
    }

    /// Inserts the downlink `sep -> rblk` into the parent of the locked
    /// page `cblk` (at `level`), clearing its INCOMPLETE_SPLIT flag. The
    /// parent is locked before `right` (the new right page) is released.
    #[allow(clippy::too_many_arguments)]
    fn insert_downlink(
        &self,
        cblk: u32,
        child: &mut PageMut<'_>,
        sep: &[u8],
        rblk: u32,
        level: u32,
        stack: &mut Vec<u32>,
        right: Option<PageMut<'_>>,
    ) -> Result<()> {
        let mut pblk = match stack.pop() {
            Some(b) => b,
            None => self.leftmost_at(level + 1)?,
        };
        let mut ppin = self.fetch(pblk)?;
        loop {
            let mut ppage = ppin.write();
            let (slot, incomplete, next) = {
                let pnode = Node::open(&ppage[..])?;
                (
                    pnode.find_downlink(cblk)?,
                    pnode.opaque().flags & flags::INCOMPLETE_SPLIT != 0,
                    pnode.opaque().next,
                )
            };
            // A parent-level page whose own split is unfinished gets its
            // downlink first, before we insert into it or move past it.
            if incomplete {
                self.finish_split(pblk, &mut ppage, level + 1, stack.clone())?;
            }
            match slot {
                Some(slot) => {
                    drop(right);
                    let downlink = internal_item(sep, rblk);
                    let child = Some((cblk, &mut child[..]));
                    return self.insert_item(&ppin, ppage, pblk, slot + 1, downlink, stack, child);
                }
                None => {
                    if next == P_NONE {
                        return Err(BTreeError::Corrupt("downlink to split page not found"));
                    }
                    drop(ppage);
                    pblk = next;
                    ppin = self.fetch(next)?;
                }
            }
        }
    }

    /// Completes the split of the locked page `blk` at `level` whose
    /// downlink is missing (INCOMPLETE_SPLIT after a crash).
    fn finish_split(
        &self,
        blk: u32,
        page: &mut PageMut<'_>,
        level: u32,
        mut stack: Vec<u32>,
    ) -> Result<()> {
        let (sep, rblk, op) = {
            let node = Node::open(&page[..])?;
            let high = node
                .high_key()?
                .ok_or(BTreeError::Corrupt("incomplete split on a rightmost page"))?;
            (high.to_vec(), node.opaque().next, node.opaque())
        };
        page.mark_dirty();
        if op.is_root() {
            return self.new_root(blk, page, rblk, &sep, level);
        }
        self.insert_downlink(blk, page, &sep, rblk, level, &mut stack, None)
    }

    /// Splits the full page `lblk` (adding `item` at `pos`) into itself and
    /// the new page `rblk`, and logs the split. Returns the separator (the
    /// right page's lower bound and the left page's new high key) and the
    /// left page's opaque.
    #[allow(clippy::too_many_arguments)]
    fn split(
        &self,
        lblk: u32,
        lpage: &mut PageMut<'_>,
        rblk: u32,
        rpage: &mut PageMut<'_>,
        pos: u16,
        item: Vec<u8>,
        child: Option<(u32, &mut [u8])>,
    ) -> Result<(Vec<u8>, Opaque)> {
        let (op, high, mut items, first) = {
            let n = Node::open(&lpage[..])?;
            (
                n.opaque(),
                n.high_key()?.map(<[u8]>::to_vec),
                n.data_items()?,
                n.first_data(),
            )
        };
        let idx = usize::from(pos - first);
        let at_end = idx == items.len();
        items.insert(idx, item);
        let k = choose_split(&items, op, at_end, high.as_ref().map(Vec::len))?;

        let mut right = items.split_off(k);
        let sep = if op.is_leaf() {
            right[0].clone()
        } else {
            // The first right item's key moves up; it becomes minus infinity.
            let first = &right[0];
            let cut = first.len() - CHILD_LEN;
            let sep = first[..cut].to_vec();
            right[0] = first[cut..].to_vec();
            sep
        };

        let left_op = Opaque {
            next: rblk,
            flags: op.flags | flags::INCOMPLETE_SPLIT,
            ..op
        };
        let right_op = Opaque {
            prev: lblk,
            next: op.next,
            level: op.level,
            flags: op.flags & !(flags::ROOT | flags::INCOMPLETE_SPLIT),
        };
        build_page(lpage, left_op, Some(&sep), &items)?;
        build_page(rpage, right_op, high.as_deref(), &right)?;
        lpage.mark_dirty();
        rpage.mark_dirty();

        let mut rec = Record::new(RmgrId::Btree, info::SPLIT);
        rec.blocks = vec![
            image_block(self.rel, lblk, lpage),
            image_block(self.rel, rblk, rpage),
        ];
        let mut split_flags = 0u8;

        // Left-to-right lock order: left, new right, old right sibling.
        let spin = if op.next != P_NONE {
            Some(self.fetch(op.next)?)
        } else {
            None
        };
        let mut spage = spin.as_ref().map(|p| p.write());
        if let Some(sp) = spage.as_mut() {
            let mut snode = Node::open(&mut sp[..])?;
            let sop = Opaque {
                prev: rblk,
                ..snode.opaque()
            };
            snode.set_opaque(sop);
            sp.mark_dirty();
            let mut b = BlockRef::new(self.rel, BlockNumber(op.next));
            b.data = rblk.to_le_bytes().to_vec();
            rec.blocks.push(b);
            split_flags |= info::SPLIT_HAS_SIBLING;
        }
        let child_page = match child {
            Some((cblk, cpage)) => {
                clear_flags(cpage, flags::INCOMPLETE_SPLIT)?;
                rec.blocks.push(BlockRef::new(self.rel, BlockNumber(cblk)));
                split_flags |= info::SPLIT_HAS_CHILD;
                Some(cpage)
            }
            None => None,
        };
        rec.main = vec![split_flags];

        let mut pages: Vec<&mut [u8]> = vec![&mut lpage[..], &mut rpage[..]];
        if let Some(sp) = spage.as_mut() {
            pages.push(&mut sp[..]);
        }
        if let Some(c) = child_page {
            pages.push(c);
        }
        self.log(rec, &mut pages);
        Ok((sep, left_op))
    }

    /// Creates a new root above the split old root, points the metapage at
    /// it, and clears the old root's ROOT and INCOMPLETE_SPLIT flags, in one
    /// WAL record. Called with the old root still locked.
    fn new_root(
        &self,
        lblk: u32,
        lpage: &mut PageMut<'_>,
        rblk: u32,
        sep: &[u8],
        level: u32,
    ) -> Result<()> {
        let (nblk, npin) = self.pool.extend(self.rel)?;
        let mut np = npin.write();
        let op = Opaque {
            prev: P_NONE,
            next: P_NONE,
            level: level + 1,
            flags: flags::ROOT,
        };
        let items = [internal_item(&[], lblk), internal_item(sep, rblk)];
        build_page(&mut np, op, None, &items)?;
        np.mark_dirty();

        clear_flags(lpage, flags::ROOT | flags::INCOMPLETE_SPLIT)?;
        lpage.mark_dirty();

        let mpin = self.fetch(META_BLOCK)?;
        let mut m = mpin.write();
        let meta = Meta {
            root: nblk.0,
            level: level + 1,
        };
        meta.write(&mut m)?;
        m.mark_dirty();

        let mut mb = BlockRef::new(self.rel, BlockNumber(META_BLOCK));
        mb.data = [meta.root.to_le_bytes(), meta.level.to_le_bytes()].concat();
        let mut rec = Record::new(RmgrId::Btree, info::NEWROOT);
        rec.blocks = vec![
            image_block(self.rel, nblk.0, &np),
            BlockRef::new(self.rel, BlockNumber(lblk)),
            mb,
        ];
        self.log(rec, &mut [&mut np[..], &mut lpage[..], &mut m[..]]);
        Ok(())
    }
}

/// A block reference carrying a full image of `page` (a page built from
/// scratch, so redo just restores it).
fn image_block(rel: RelFileId, blk: u32, page: &[u8]) -> BlockRef {
    let mut b = BlockRef::new(rel, BlockNumber(blk));
    b.image = Some(page.to_vec());
    b
}

/// Leaf state found by the page deletion scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageState {
    Empty,
    HalfDead,
}

/// Sets `bits` in a node page's flags.
fn set_flags(page: &mut [u8], bits: u16) -> Result<()> {
    let mut node = Node::open(page)?;
    let op = Opaque {
        flags: node.opaque().flags | bits,
        ..node.opaque()
    };
    node.set_opaque(op);
    Ok(())
}

fn set_next(page: &mut [u8], next: u32) -> Result<()> {
    let mut node = Node::open(page)?;
    let op = Opaque {
        next,
        ..node.opaque()
    };
    node.set_opaque(op);
    Ok(())
}

fn set_prev(page: &mut [u8], prev: u32) -> Result<()> {
    let mut node = Node::open(page)?;
    let op = Opaque {
        prev,
        ..node.opaque()
    };
    node.set_opaque(op);
    Ok(())
}

/// Parent side of MARK_HALFDEAD: the downlink at `slot` takes the next
/// item's child, and the next item is removed.
fn redo_halfdead_parent(page: &mut [u8], slot: u16) -> Result<()> {
    let mut node = Node::open(page)?;
    let right = node.child_at(slot + 1)?;
    let item = node.item(slot)?.to_vec();
    let sep = &item[..item.len() - CHILD_LEN];
    let new_item = internal_item(sep, right);
    node.remove_at(slot + 1)?;
    node.remove_at(slot)?;
    if !node.insert_at(slot, &new_item)? {
        return Err(BTreeError::Corrupt("re-pointed downlink does not fit"));
    }
    Ok(())
}

/// Clears `bits` in a node page's flags.
fn clear_flags(page: &mut [u8], bits: u16) -> Result<()> {
    let mut node = Node::open(page)?;
    let op = Opaque {
        flags: node.opaque().flags & !bits,
        ..node.opaque()
    };
    node.set_opaque(op);
    Ok(())
}

/// Applies block `idx` of a B+Tree record that carries no image to `page`.
/// The caller has checked that the page LSN is below the record's and sets
/// the page LSN afterwards.
pub fn redo(rec: &Record, idx: usize, page: &mut [u8]) -> Result<()> {
    let block = rec
        .blocks
        .get(idx)
        .ok_or(BTreeError::Corrupt("redo block index"))?;
    let d = &block.data;
    let slot = || -> Result<u16> {
        d.get(..2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
            .ok_or(BTreeError::Corrupt("redo data too short"))
    };
    let u32_at = |at: usize| -> Result<u32> {
        d.get(at..at + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or(BTreeError::Corrupt("redo data too short"))
    };
    match (rec.info, idx) {
        (info::INSERT, 0) => {
            if !Node::open(page)?.insert_at(slot()?, &d[2..])? {
                return Err(BTreeError::Corrupt("redo insert does not fit"));
            }
            Ok(())
        }
        (info::INSERT, 1) => clear_flags(page, flags::INCOMPLETE_SPLIT),
        (info::DELETE, 0) => Node::open(page)?.remove_at(slot()?),
        (info::BULK_DELETE, 0) => {
            let mut node = Node::open(page)?;
            for c in d.as_chunks::<2>().0 {
                node.remove_at(u16::from_le_bytes(*c))?;
            }
            Ok(())
        }
        (info::SPLIT, i) if i >= 2 => {
            let split_flags = rec.main.first().copied().unwrap_or(0);
            let has_sibling = split_flags & info::SPLIT_HAS_SIBLING != 0;
            if i == 2 && has_sibling {
                let mut node = Node::open(page)?;
                let op = Opaque {
                    prev: u32_at(0)?,
                    ..node.opaque()
                };
                node.set_opaque(op);
                Ok(())
            } else {
                clear_flags(page, flags::INCOMPLETE_SPLIT)
            }
        }
        (info::NEWROOT, 1) => clear_flags(page, flags::ROOT | flags::INCOMPLETE_SPLIT),
        (info::MARK_HALFDEAD, 0) => redo_halfdead_parent(page, slot()?),
        (info::MARK_HALFDEAD, 1) => set_flags(page, flags::HALF_DEAD),
        (info::UNLINK, i) => {
            let has_left = rec.main.first() == Some(&1);
            match (has_left, i) {
                (true, 0) => set_next(page, u32_at(0)?),
                (true, 1) | (false, 0) => {
                    clear_flags(page, flags::HALF_DEAD)?;
                    set_flags(page, flags::DELETED)
                }
                _ => set_prev(page, u32_at(0)?),
            }
        }
        (info::NEWROOT, 2) => Meta {
            root: u32_at(0)?,
            level: u32_at(4)?,
        }
        .write(page),
        _ => Err(BTreeError::Corrupt("record block needs an image")),
    }
}

/// Builds `key || tid`, rejecting entries that are too large.
fn entry_key(key: &[u8], tid: Tid) -> Result<Vec<u8>> {
    let len = key.len() + TID_LEN;
    if len > MAX_ITEM_SIZE {
        return Err(BTreeError::KeyTooLarge {
            len,
            max: MAX_ITEM_SIZE,
        });
    }
    let mut e = Vec::with_capacity(len);
    e.extend_from_slice(key);
    encode_tid(tid, &mut e);
    Ok(e)
}

/// Picks `k` so that `items[..k]` stay left and `items[k..]` move right.
///
/// A rightmost leaf receiving an append is split 90:10 (sequential
/// inserts leave full pages behind); otherwise the split balances bytes.
/// Every candidate must fit with its high key on both sides.
fn choose_split(
    items: &[Vec<u8>],
    op: Opaque,
    at_end: bool,
    old_high: Option<usize>,
) -> Result<usize> {
    let n = items.len();
    if n < 2 {
        return Err(BTreeError::Corrupt("too few items to split"));
    }
    let mut prefix = vec![0usize; n + 1];
    for (i, it) in items.iter().enumerate() {
        prefix[i + 1] = prefix[i] + item_cost(it.len());
    }
    let total = prefix[n];
    let sep_len = |k: usize| {
        if op.is_leaf() {
            items[k].len()
        } else {
            items[k].len() - CHILD_LEN
        }
    };
    let left = |k: usize| prefix[k] + item_cost(sep_len(k));
    let right = |k: usize| {
        let mut r = total - prefix[k] + old_high.map_or(0, item_cost);
        if !op.is_leaf() {
            // The first right item is truncated to a bare child pointer.
            r = r - item_cost(items[k].len()) + item_cost(CHILD_LEN);
        }
        r
    };
    let fits = |k: usize| left(k) <= USABLE && right(k) <= USABLE;

    if op.is_leaf() && op.is_rightmost() && at_end {
        let target = USABLE * 9 / 10;
        if let Some(k) = (1..n).rev().find(|&k| left(k) <= target && fits(k)) {
            return Ok(k);
        }
    }
    (1..n)
        .filter(|&k| fits(k))
        .min_by_key(|&k| left(k).abs_diff(right(k)))
        .ok_or(BTreeError::Corrupt("no valid split point"))
}

#[derive(Debug)]
enum ScanState {
    Start,
    Next(u32),
    Done,
}

/// Iterator over index entries within bounds: `(key, tid)`.
#[derive(Debug)]
pub struct BTreeScan<'a> {
    tree: &'a BTree,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    state: ScanState,
    buffered: std::vec::IntoIter<(Vec<u8>, Tid)>,
}

impl BTreeScan<'_> {
    /// Copies the qualifying entries of the next leaf into `buffered`.
    fn load(&mut self) -> Result<()> {
        let (pin, target) = match self.state {
            ScanState::Start => {
                let target = match &self.lower {
                    Bound::Included(k) | Bound::Excluded(k) => k.clone(),
                    Bound::Unbounded => Vec::new(),
                };
                (self.tree.descend(&target)?.0, Some(target))
            }
            ScanState::Next(blk) => (self.tree.fetch(blk)?, None),
            ScanState::Done => return Ok(()),
        };
        let mut pin = pin;
        loop {
            let page = pin.read();
            let node = Node::open(&page[..])?;
            let start = match &target {
                Some(t) if node.must_move_right(t)? => {
                    let next = node.opaque().next;
                    drop(page);
                    pin = self.tree.fetch(next)?;
                    continue;
                }
                Some(t) => node.lower_bound(t)?,
                None => node.first_data(),
            };
            let mut out = Vec::new();
            let mut finished = false;
            for s in start..node.slot_count() {
                let entry = node.key_at(s)?;
                let cut = entry
                    .len()
                    .checked_sub(TID_LEN)
                    .ok_or(BTreeError::Corrupt("leaf entry shorter than tid"))?;
                let key = &entry[..cut];
                let above_lower = match &self.lower {
                    Bound::Included(k) => key >= k.as_slice(),
                    Bound::Excluded(k) => key > k.as_slice(),
                    Bound::Unbounded => true,
                };
                if !above_lower {
                    continue;
                }
                let below_upper = match &self.upper {
                    Bound::Included(k) => key <= k.as_slice(),
                    Bound::Excluded(k) => key < k.as_slice(),
                    Bound::Unbounded => true,
                };
                if !below_upper {
                    finished = true;
                    break;
                }
                out.push((key.to_vec(), decode_tid(&entry[cut..])?));
            }
            let next = node.opaque().next;
            self.state = if finished || next == P_NONE {
                ScanState::Done
            } else {
                ScanState::Next(next)
            };
            self.buffered = out.into_iter();
            return Ok(());
        }
    }
}

impl Iterator for BTreeScan<'_> {
    type Item = Result<(Vec<u8>, Tid)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(e) = self.buffered.next() {
                return Some(Ok(e));
            }
            if matches!(self.state, ScanState::Done) {
                return None;
            }
            if let Err(e) = self.load() {
                self.state = ScanState::Done;
                return Some(Err(e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::btree::key::encode_key;
    use crate::buffer::SingleModeFlush;
    use crate::storage::backend::FenceToken;
    use crate::storage::disk::DiskManager;
    use crate::storage::mem::MemBackend;
    use crate::storage::slotted::SlotId;
    use crate::types::{DataType, Value};
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    const REL: RelFileId = RelFileId(10);
    const FENCE: FenceToken = FenceToken::new(1);

    fn pool_over(mem: &Arc<MemBackend>, frames: usize) -> Arc<BufferPool> {
        let disk = Arc::new(DiskManager::new(mem.clone()));
        Arc::new(BufferPool::new(frames, disk, Arc::new(SingleModeFlush::new(FENCE))).unwrap())
    }

    fn setup() -> (Arc<MemBackend>, Arc<BufferPool>, BTree) {
        let mem = Arc::new(MemBackend::default());
        let pool = pool_over(&mem, 64);
        DiskManager::new(mem.clone())
            .create_relation(REL, FENCE)
            .unwrap();
        let tree = BTree::create(REL, pool.clone()).unwrap();
        (mem, pool, tree)
    }

    fn tid(n: u32) -> Tid {
        Tid {
            blk: BlockNumber(n / 100),
            slot: SlotId((n % 100) as u16),
        }
    }

    /// Key of (INT8 id, TEXT pad) so that the pad length controls entry size.
    fn key(id: i64, pad: usize) -> Vec<u8> {
        let mut out = Vec::new();
        encode_key(
            &[DataType::Int8, DataType::Text],
            &[Value::Int8(id), Value::Text("p".repeat(pad))],
            &mut out,
        )
        .unwrap();
        out
    }

    fn all(tree: &BTree) -> Vec<(Vec<u8>, Tid)> {
        tree.scan(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap()
    }

    /// Walks every level left to right and checks the structure. Returns the
    /// leaf entries in order.
    fn check_invariants(tree: &BTree) -> Vec<Vec<u8>> {
        let meta = tree.meta().unwrap();
        let mut leaf_entries = Vec::new();
        let mut expected_pages: Option<Vec<u32>> = None;
        for level in (0..=meta.level).rev() {
            let mut blk = tree.leftmost_at(level).unwrap();
            let mut prev = P_NONE;
            let mut lower: Option<Vec<u8>> = None;
            let mut pages_here = Vec::new();
            let mut children = Vec::new();
            loop {
                pages_here.push(blk);
                let pin = tree.fetch(blk).unwrap();
                let page = pin.read();
                let node = Node::open(&page[..]).unwrap();
                let op = node.opaque();
                assert_eq!(op.level, level);
                assert_eq!(op.prev, prev, "prev link of {blk}");
                assert_eq!(
                    op.is_root(),
                    level == meta.level && prev == P_NONE && op.is_rightmost()
                );
                assert_eq!(
                    op.flags & flags::INCOMPLETE_SPLIT,
                    0,
                    "unfinished split at {blk}"
                );
                let high = node.high_key().unwrap().map(<[u8]>::to_vec);
                let mut last: Option<Vec<u8>> = None;
                for s in node.first_data()..node.slot_count() {
                    let k = node.key_at(s).unwrap().to_vec();
                    let minus_inf = level > 0 && s == node.first_data();
                    if minus_inf {
                        assert!(k.is_empty(), "first internal key is minus infinity");
                    } else {
                        if let Some(l) = &last {
                            assert!(l < &k, "keys strictly increasing");
                        }
                        if let Some(lo) = &lower {
                            assert!(&k >= lo, "key below left sibling's high key");
                        }
                        if let Some(h) = &high {
                            assert!(&k < h, "key not below high key");
                        }
                        last = Some(k.clone());
                    }
                    if level == 0 {
                        leaf_entries.push(k);
                    } else {
                        let child = node.child_at(s).unwrap();
                        children.push(child);
                        let cpin = tree.fetch(child).unwrap();
                        let cpage = cpin.read();
                        assert_eq!(Node::open(&cpage[..]).unwrap().opaque().level, level - 1);
                    }
                }
                lower = high;
                if op.is_rightmost() {
                    break;
                }
                prev = blk;
                blk = op.next;
            }
            if let Some(exp) = expected_pages.take() {
                assert_eq!(
                    pages_here, exp,
                    "every page at level {level} has a downlink"
                );
            }
            if level > 0 {
                expected_pages = Some(children);
            }
        }
        leaf_entries
    }

    #[test]
    fn empty_tree() {
        let (_mem, _pool, tree) = setup();
        assert_eq!(tree.height().unwrap(), 1);
        assert!(all(&tree).is_empty());
        assert!(!tree.delete(&key(1, 0), tid(1)).unwrap());
        check_invariants(&tree);
    }

    #[test]
    fn small_inserts_come_back_sorted() {
        let (_mem, _pool, tree) = setup();
        for (i, id) in [5i64, -3, 9, 0, 7].iter().enumerate() {
            tree.insert(&key(*id, 0), tid(i as u32)).unwrap();
        }
        let keys: Vec<Vec<u8>> = all(&tree).into_iter().map(|(k, _)| k).collect();
        let expected: Vec<Vec<u8>> = [-3i64, 0, 5, 7, 9].iter().map(|&i| key(i, 0)).collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn duplicates_ordered_by_tid_and_exact_duplicate_rejected() {
        let (_mem, _pool, tree) = setup();
        for t in [30u32, 10, 20] {
            tree.insert(&key(1, 0), tid(t)).unwrap();
        }
        let tids: Vec<Tid> = all(&tree).into_iter().map(|(_, t)| t).collect();
        assert_eq!(tids, vec![tid(10), tid(20), tid(30)]);
        assert!(matches!(
            tree.insert(&key(1, 0), tid(20)),
            Err(BTreeError::Duplicate)
        ));
    }

    #[test]
    fn delete_entries() {
        let (_mem, _pool, tree) = setup();
        tree.insert(&key(1, 0), tid(1)).unwrap();
        tree.insert(&key(1, 0), tid(2)).unwrap();
        assert!(tree.delete(&key(1, 0), tid(1)).unwrap());
        assert!(!tree.delete(&key(1, 0), tid(1)).unwrap());
        assert_eq!(all(&tree), vec![(key(1, 0), tid(2))]);
    }

    #[test]
    fn rejects_oversized_key() {
        let (_mem, _pool, tree) = setup();
        assert!(matches!(
            tree.insert(&key(1, MAX_ITEM_SIZE), tid(1)),
            Err(BTreeError::KeyTooLarge { .. })
        ));
        // The largest allowed entry works, repeatedly (forcing splits).
        let pad = MAX_ITEM_SIZE - TID_LEN - key(0, 0).len();
        for i in 0..20 {
            tree.insert(&key(i, pad), tid(i as u32)).unwrap();
        }
        assert_eq!(check_invariants(&tree).len(), 20);
    }

    #[test]
    fn random_inserts_split_and_grow() {
        let (_mem, _pool, tree) = setup();
        let mut model = BTreeSet::new();
        let mut x: u64 = 12345;
        for i in 0..2000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let id = (x % 100_000) as i64;
            let e = (key(id, 300), tid(i));
            tree.insert(&e.0, e.1).unwrap();
            model.insert(e);
        }
        assert!(
            tree.height().unwrap() >= 3,
            "height {}",
            tree.height().unwrap()
        );
        assert_eq!(check_invariants(&tree).len(), 2000);
        assert_eq!(all(&tree), model.into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn sequential_inserts_fill_leaves_90_percent() {
        let (_mem, _pool, tree) = setup();
        let n = 3000;
        for i in 0..n {
            tree.insert(&key(i, 0), tid(i as u32)).unwrap();
        }
        check_invariants(&tree);
        let mut leaves = 0;
        let mut blk = tree.leftmost_at(0).unwrap();
        loop {
            leaves += 1;
            let pin = tree.fetch(blk).unwrap();
            let page = pin.read();
            let op = Node::open(&page[..]).unwrap().opaque();
            if op.is_rightmost() {
                break;
            }
            blk = op.next;
        }
        let per_page = USABLE / item_cost(key(0, 0).len() + TID_LEN);
        let ideal = (n as usize).div_ceil(per_page);
        assert!(
            leaves <= ideal * 12 / 10,
            "{leaves} leaves, ideal {ideal} (50:50 would be about {})",
            ideal * 2
        );
    }

    #[test]
    fn survives_flush_and_reopen() {
        let (mem, pool, tree) = setup();
        for i in 0..500 {
            tree.insert(&key(i, 100), tid(i as u32)).unwrap();
        }
        pool.flush_all().unwrap();
        let expected = all(&tree);
        let tree2 = BTree::open(REL, pool_over(&mem, 64)).unwrap();
        assert_eq!(all(&tree2), expected);
        check_invariants(&tree2);
    }

    #[test]
    fn range_bounds() {
        let (_mem, _pool, tree) = setup();
        for i in 0..100 {
            tree.insert(&key(i, 50), tid(i as u32)).unwrap();
            tree.insert(&key(i, 50), tid(1000 + i as u32)).unwrap();
        }
        let ids = |lo: Bound<Vec<u8>>, hi: Bound<Vec<u8>>| -> Vec<Vec<u8>> {
            tree.scan(lo, hi).unwrap().map(|r| r.unwrap().0).collect()
        };
        let k = |i| key(i, 50);
        let r = ids(Bound::Included(k(10)), Bound::Excluded(k(13)));
        assert_eq!(r, vec![k(10), k(10), k(11), k(11), k(12), k(12)]);
        let r = ids(Bound::Excluded(k(97)), Bound::Unbounded);
        assert_eq!(r, vec![k(98), k(98), k(99), k(99)]);
        let r = ids(Bound::Unbounded, Bound::Included(k(0)));
        assert_eq!(r, vec![k(0), k(0)]);
        assert!(ids(Bound::Included(k(50)), Bound::Excluded(k(50))).is_empty());
        assert!(ids(Bound::Excluded(k(99)), Bound::Unbounded).is_empty());
    }

    // ---- concurrency ----

    use crate::buffer::BufferError;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Height the concurrency tests stay within (asserted at the end).
    const MAX_HEIGHT: usize = 4;
    /// An insert pins its split path: about two pages per level, plus the
    /// old right sibling and the metapage/new root at a root split.
    const MAX_PINS_PER_THREAD: usize = 2 * MAX_HEIGHT + 2;
    const SLACK: usize = 4;

    fn concurrent_setup(threads: usize) -> (Arc<BufferPool>, BTree) {
        let mem = Arc::new(MemBackend::default());
        let pool = pool_over(&mem, threads * MAX_PINS_PER_THREAD + SLACK);
        DiskManager::new(mem.clone())
            .create_relation(REL, FENCE)
            .unwrap();
        let tree = BTree::create(REL, pool.clone()).unwrap();
        (pool, tree)
    }

    fn fail_on_exhausted<T>(r: Result<T>, what: &str) -> T {
        match r {
            Ok(v) => v,
            Err(BTreeError::Buffer(BufferError::Exhausted)) => {
                panic!("{what}: Exhausted with a pool sized for the test's concurrency")
            }
            Err(e) => panic!("{what}: {e}"),
        }
    }

    #[test]
    fn concurrent_inserts_and_scans() {
        const THREADS: usize = 4;
        const PER_THREAD: usize = 1500;
        const PAD: usize = 200;
        let (pool, tree) = concurrent_setup(THREADS + 1);
        let progress: Vec<AtomicUsize> = (0..THREADS).map(|_| AtomicUsize::new(0)).collect();
        let done = AtomicBool::new(false);
        // Interleaved ids make every writer hit the right edge of the tree.
        let id = |t: usize, i: usize| (i * THREADS + t) as i64;

        std::thread::scope(|s| {
            for t in 0..THREADS {
                let (tree, progress) = (&tree, &progress);
                s.spawn(move || {
                    for i in 0..PER_THREAD {
                        fail_on_exhausted(
                            tree.insert(&key(id(t, i), PAD), tid(i as u32)),
                            "insert",
                        );
                        progress[t].store(i + 1, Ordering::SeqCst);
                    }
                });
            }
            s.spawn(|| {
                let mut last_len = 0;
                let mut scans = 0;
                while !done.load(Ordering::SeqCst) || scans < 3 {
                    let before: Vec<usize> =
                        progress.iter().map(|p| p.load(Ordering::SeqCst)).collect();
                    let got: Vec<(Vec<u8>, Tid)> = fail_on_exhausted(
                        tree.scan(Bound::Unbounded, Bound::Unbounded)
                            .and_then(|sc| sc.collect::<Result<_>>()),
                        "scan",
                    );
                    assert!(got.windows(2).all(|w| w[0] < w[1]), "sorted, no duplicates");
                    assert!(got.len() >= last_len, "an entry disappeared");
                    last_len = got.len();
                    let present: BTreeSet<&Vec<u8>> = got.iter().map(|(k, _)| k).collect();
                    for (t, &p) in before.iter().enumerate() {
                        for i in 0..p {
                            assert!(
                                present.contains(&key(id(t, i), PAD)),
                                "insert completed before the scan is missing"
                            );
                        }
                    }
                    scans += 1;
                }
            });
            // Writers are joined when the scope ends; signal the scanner
            // once they have all finished.
            s.spawn(|| {
                while progress
                    .iter()
                    .any(|p| p.load(Ordering::SeqCst) < PER_THREAD)
                {
                    std::thread::yield_now();
                }
                done.store(true, Ordering::SeqCst);
            });
        });

        let height = tree.height().unwrap() as usize;
        assert!((2..=MAX_HEIGHT).contains(&height), "height {height}");
        assert!(
            pool.nblocks(REL).unwrap() as usize > pool.capacity() * 2,
            "eviction exercised"
        );
        let entries = check_invariants(&tree);
        assert_eq!(entries.len(), THREADS * PER_THREAD);
        let expected: BTreeSet<(Vec<u8>, Tid)> = (0..THREADS)
            .flat_map(|t| (0..PER_THREAD).map(move |i| (key(id(t, i), PAD), tid(i as u32))))
            .collect();
        assert_eq!(all(&tree), expected.into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn concurrent_inserts_and_deletes() {
        const THREADS: usize = 4;
        const PER_THREAD: usize = 1000;
        let (_pool, tree) = concurrent_setup(THREADS);
        // Thread t owns ids with id % THREADS == t; it inserts all of them in
        // a scattered order and deletes every third one afterwards.
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let tree = &tree;
                s.spawn(move || {
                    let ids: Vec<i64> = (0..PER_THREAD)
                        .map(|i| ((i * 7919) % PER_THREAD * THREADS + t) as i64)
                        .collect();
                    for &k in &ids {
                        fail_on_exhausted(tree.insert(&key(k, 150), tid(t as u32)), "insert");
                    }
                    for &k in ids.iter().filter(|&&k| k % 3 == 0) {
                        assert!(fail_on_exhausted(
                            tree.delete(&key(k, 150), tid(t as u32)),
                            "delete"
                        ));
                    }
                });
            }
        });
        let expected: BTreeSet<(Vec<u8>, Tid)> = (0..THREADS * PER_THREAD)
            .filter(|k| k % 3 != 0)
            .map(|k| (key(k as i64, 150), tid((k % THREADS) as u32)))
            .collect();
        assert!(tree.height().unwrap() as usize <= MAX_HEIGHT);
        assert_eq!(check_invariants(&tree).len(), expected.len());
        assert_eq!(all(&tree), expected.into_iter().collect::<Vec<_>>());
    }

    // ---- WAL ----

    use crate::storage::backend::FixedFence;
    use crate::storage::page::{Lsn, PAGE_SIZE, is_new_page, page_lsn, set_page_lsn};
    use crate::wal::log::FIRST_LSN;
    use crate::wal::{Wal, WalConfig, WalReader};
    use std::collections::HashMap;

    fn wal_cfg() -> WalConfig {
        WalConfig {
            timeline: 1,
            segment_size: 256 * 1024,
        }
    }

    fn setup_wal() -> (Arc<MemBackend>, Arc<BufferPool>, BTree) {
        let mem = Arc::new(MemBackend::default());
        let disk = Arc::new(DiskManager::new(mem.clone()));
        disk.create_relation(REL, FENCE).unwrap();
        let wal =
            Arc::new(Wal::create(mem.clone(), Arc::new(FixedFence(FENCE)), wal_cfg()).unwrap());
        let pool = Arc::new(
            BufferPool::new(256, disk, Arc::new(SingleModeFlush::new(FENCE)))
                .unwrap()
                .with_wal(wal),
        );
        let tree = BTree::create(REL, pool.clone()).unwrap();
        (mem, pool, tree)
    }

    fn replay(mem: &Arc<MemBackend>) -> HashMap<u32, Vec<u8>> {
        let mut rd = WalReader::new(mem.clone(), wal_cfg(), FIRST_LSN, Some(Lsn::INVALID)).unwrap();
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
        while let Some((_, end, d)) = rd.next_record().unwrap() {
            let rec = d.record;
            for (i, b) in rec.blocks.iter().enumerate() {
                let page = pages.entry(b.blk.0).or_insert_with(|| vec![0u8; PAGE_SIZE]);
                if let Some(img) = &b.image {
                    page.copy_from_slice(img);
                } else if page_lsn(page) < end {
                    redo(&rec, i, page).unwrap();
                } else {
                    continue;
                }
                set_page_lsn(page, end);
            }
        }
        pages
    }

    #[test]
    fn wal_replay_reproduces_tree() {
        let (mem, pool, tree) = setup_wal();
        let mut x: u64 = 777;
        let mut inserted = Vec::new();
        for i in 0..1500u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let e = (key((x % 50_000) as i64, 250), tid(i));
            tree.insert(&e.0, e.1).unwrap();
            inserted.push(e);
        }
        for e in inserted.iter().step_by(3) {
            assert!(tree.delete(&e.0, e.1).unwrap());
        }
        assert!(tree.height().unwrap() >= 3);
        check_invariants(&tree);
        pool.wal().unwrap().flush_all();
        let replayed = replay(&mem);
        for blk in 0..pool.nblocks(REL).unwrap() {
            let pin = tree.fetch(blk).unwrap();
            let mut live = pin.read().to_vec();
            live[8..12].fill(0);
            let mut want = replayed
                .get(&blk)
                .cloned()
                .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
            want[8..12].fill(0);
            assert_eq!(live, want, "block {blk} differs after replay");
        }
    }

    /// First page (in block order) with INCOMPLETE_SPLIT set.
    fn find_flagged(tree: &BTree) -> Option<u32> {
        (1..tree.pool.nblocks(REL).unwrap()).find(|&blk| {
            let pin = tree.fetch(blk).unwrap();
            let page = pin.read();
            !is_new_page(&page)
                && Node::open(&page[..]).unwrap().opaque().flags & flags::INCOMPLETE_SPLIT != 0
        })
    }

    #[test]
    fn unfinished_split_is_completed_by_next_insert() {
        let (_mem, _pool, tree) = setup_wal();
        for i in 0..200 {
            tree.insert(&key(i * 10, 300), tid(i as u32)).unwrap();
        }
        let before_height = tree.height().unwrap();
        // "Crash" right after logging the next split.
        tree.stop_after_split.store(true, Ordering::SeqCst);
        let mut i = 0;
        let flagged = loop {
            tree.insert(&key(i * 10 + 5, 300), tid(10_000 + i as u32))
                .unwrap();
            if let Some(b) = find_flagged(&tree) {
                break b;
            }
            i += 1;
        };
        tree.stop_after_split.store(false, Ordering::SeqCst);
        // Searches still see everything through the right-link.
        assert_eq!(all(&tree).len(), 200 + i as usize + 1);

        // The next insert into that page finishes the split first.
        let first_key = {
            let pin = tree.fetch(flagged).unwrap();
            let page = pin.read();
            let node = Node::open(&page[..]).unwrap();
            let e = node.key_at(node.first_data()).unwrap().to_vec();
            e[..e.len() - TID_LEN].to_vec()
        };
        tree.insert(&first_key, tid(99_999)).unwrap();
        assert!(find_flagged(&tree).is_none());
        assert_eq!(check_invariants(&tree).len(), 200 + i as usize + 2);
        assert_eq!(tree.height().unwrap(), before_height);
    }

    #[test]
    fn unfinished_root_split_is_completed() {
        let (_mem, _pool, tree) = setup_wal();
        tree.stop_after_split.store(true, Ordering::SeqCst);
        let mut n = 0;
        while find_flagged(&tree).is_none() {
            tree.insert(&key(n, 500), tid(n as u32)).unwrap();
            n += 1;
        }
        tree.stop_after_split.store(false, Ordering::SeqCst);
        assert_eq!(
            tree.height().unwrap(),
            1,
            "metapage still points at the old root"
        );
        assert_eq!(all(&tree).len(), n as usize);
        tree.insert(&key(0, 500), tid(50_000)).unwrap();
        assert_eq!(tree.height().unwrap(), 2);
        assert_eq!(check_invariants(&tree).len(), n as usize + 1);
    }

    #[test]
    fn bulk_delete_removes_dead_tids_and_replays() {
        let (mem, pool, tree) = setup_wal();
        let mut model = BTreeSet::new();
        for i in 0..1200u32 {
            let e = (key((i % 300) as i64, 120), tid(i));
            tree.insert(&e.0, e.1).unwrap();
            model.insert(e);
        }
        let dead = |t: Tid| t.slot.0.is_multiple_of(3);
        let n = tree.bulk_delete(&dead).unwrap();
        model.retain(|(_, t)| !dead(*t));
        assert_eq!(n, 1200 - model.len());
        assert_eq!(all(&tree), model.iter().cloned().collect::<Vec<_>>());
        assert_eq!(tree.bulk_delete(&dead).unwrap(), 0, "idempotent");
        check_invariants(&tree);

        pool.wal().unwrap().flush_all();
        let replayed = replay(&mem);
        for blk in 0..pool.nblocks(REL).unwrap() {
            let pin = tree.fetch(blk).unwrap();
            let mut live = pin.read().to_vec();
            live[8..12].fill(0);
            let mut want = replayed
                .get(&blk)
                .cloned()
                .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
            want[8..12].fill(0);
            assert_eq!(live, want, "block {blk} differs after replay");
        }
    }

    #[test]
    fn bulk_delete_with_concurrent_splits() {
        let (_pool, tree) = concurrent_setup(2);
        for i in 0..400u32 {
            tree.insert(&key(i as i64 * 2, 150), tid(i)).unwrap();
        }
        // Dead: the original entries with even TID numbers.
        let dead = |t: Tid| t.blk.0 < 4 && t.slot.0.is_multiple_of(2);
        std::thread::scope(|s| {
            s.spawn(|| {
                // Inserts between the existing keys keep splitting pages.
                for i in 0..400u32 {
                    let t = Tid {
                        blk: BlockNumber(100 + i / 100),
                        slot: SlotId((i % 100) as u16),
                    };
                    fail_on_exhausted(tree.insert(&key(i as i64 * 2 + 1, 150), t), "insert");
                }
            });
            fail_on_exhausted(tree.bulk_delete(&dead), "bulk delete");
        });
        let left: Vec<Tid> = all(&tree).into_iter().map(|(_, t)| t).collect();
        assert!(left.iter().all(|t| !dead(*t)), "no dead entry survives");
        assert_eq!(left.len(), 200 + 400);
        check_invariants(&tree);
    }

    /// Number of leaves reachable through the leaf level's right-links.
    fn leaf_count(tree: &BTree) -> usize {
        let mut n = 0;
        let mut blk = tree.leftmost_at(0).unwrap();
        loop {
            n += 1;
            let pin = tree.fetch(blk).unwrap();
            let page = pin.read();
            let op = Node::open(&page[..]).unwrap().opaque();
            if op.is_rightmost() {
                return n;
            }
            blk = op.next;
        }
    }

    /// A tree with several leaves whose middle entries (ids 100..300) are
    /// then removed by a bulk delete.
    type Entries = Vec<(Vec<u8>, Tid)>;

    fn tree_with_empty_middle() -> (Arc<MemBackend>, Arc<BufferPool>, BTree, Entries) {
        let (mem, pool, tree) = setup_wal();
        let mut keep = Vec::new();
        for i in 0..400u32 {
            let e = (key(i as i64, 200), tid(i));
            tree.insert(&e.0, e.1).unwrap();
            if !(100..300).contains(&i) {
                keep.push(e);
            }
        }
        let n = tree
            .bulk_delete(&|t: Tid| (100..300).contains(&(t.blk.0 * 100 + u32::from(t.slot.0))))
            .unwrap();
        assert_eq!(n, 200);
        keep.sort();
        (mem, pool, tree, keep)
    }

    #[test]
    fn empty_leaves_are_deleted_in_two_phases() {
        let (mem, pool, tree, keep) = tree_with_empty_middle();
        let before = leaf_count(&tree);
        let deleted = tree.delete_empty_pages().unwrap();
        assert!(deleted >= 3, "deleted {deleted}");
        assert_eq!(leaf_count(&tree), before - deleted);
        assert_eq!(check_invariants(&tree).len(), keep.len());
        assert_eq!(all(&tree), keep);
        assert_eq!(
            tree.delete_empty_pages().unwrap(),
            0,
            "nothing left to delete"
        );

        // Keys in a deleted page's range now go to its right sibling.
        tree.insert(&key(150, 200), tid(9_999)).unwrap();
        let got: Vec<Tid> = tree
            .scan(
                Bound::Included(key(150, 200)),
                Bound::Included(key(150, 200)),
            )
            .unwrap()
            .map(|r| r.unwrap().1)
            .collect();
        assert_eq!(got, vec![tid(9_999)]);
        check_invariants(&tree);

        pool.wal().unwrap().flush_all();
        let replayed = replay(&mem);
        for blk in 0..pool.nblocks(REL).unwrap() {
            let pin = tree.fetch(blk).unwrap();
            let mut live = pin.read().to_vec();
            live[8..12].fill(0);
            let mut want = replayed
                .get(&blk)
                .cloned()
                .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
            want[8..12].fill(0);
            assert_eq!(live, want, "block {blk} differs after replay");
        }
    }

    #[test]
    fn half_dead_page_is_finished_later() {
        let (_mem, _pool, tree, keep) = tree_with_empty_middle();
        tree.stop_after_halfdead.store(true, Ordering::SeqCst);
        assert_eq!(tree.delete_empty_pages().unwrap(), 0);
        tree.stop_after_halfdead.store(false, Ordering::SeqCst);
        // Half-dead pages have no downlink but are still chained: searches
        // and inserts pass them to the right.
        assert_eq!(all(&tree), keep);
        tree.insert(&key(120, 200), tid(8_888)).unwrap();
        assert!(tree.delete_empty_pages().unwrap() >= 1);
        check_invariants(&tree);
        assert_eq!(all(&tree).len(), keep.len() + 1);
    }

    #[test]
    fn page_deletion_with_concurrent_inserts_and_scans() {
        let (_mem, _pool, tree, _keep) = tree_with_empty_middle();
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0..300u32 {
                    tree.insert(&key(1000 + i as i64, 200), tid(50_000 + i))
                        .unwrap();
                }
            });
            s.spawn(|| {
                for _ in 0..30 {
                    let got = all(&tree);
                    assert!(got.windows(2).all(|w| w[0] < w[1]));
                }
            });
            tree.delete_empty_pages().unwrap();
        });
        let entries = check_invariants(&tree);
        assert_eq!(entries.len(), 200 + 300);
    }

    #[test]
    fn unique_insert_outcomes() {
        let (_mem, _pool, tree) = setup_wal();
        let live = |_: Tid| Ok(UniqueCheck::Live);
        let dead = |_: Tid| Ok(UniqueCheck::Ignore);
        assert_eq!(
            tree.insert_unique(&key(1, 0), tid(1), &live).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            tree.insert_unique(&key(1, 0), tid(2), &live).unwrap(),
            InsertOutcome::Duplicate
        );
        let waiting = |_: Tid| Ok(UniqueCheck::Wait(Xid(77)));
        assert_eq!(
            tree.insert_unique(&key(1, 0), tid(2), &waiting).unwrap(),
            InsertOutcome::Wait(Xid(77))
        );
        assert_eq!(
            tree.insert_unique(&key(1, 0), tid(2), &dead).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            tree.insert_unique(&key(2, 0), tid(3), &live).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(all(&tree).len(), 3);
    }

    #[test]
    fn unique_check_spans_pages_of_dead_duplicates() {
        let (_mem, _pool, tree) = setup_wal();
        for i in 0..200u32 {
            tree.insert(&key(i as i64, 0), tid(i)).unwrap();
        }
        // Hundreds of dead versions of key 100, spreading over many pages.
        for t in 1000..1600u32 {
            tree.insert(&key(100, 300), tid(t)).unwrap();
        }
        let live_one = Tid {
            blk: BlockNumber(900), // not among the tids above
            slot: SlotId(0),
        };
        let checked = std::sync::Mutex::new(0usize);
        let only_one_live = |t: Tid| {
            *checked.lock().unwrap() += 1;
            Ok(if t == live_one {
                UniqueCheck::Live
            } else {
                UniqueCheck::Ignore
            })
        };
        // A new version lands among them after checking all of them.
        let r = tree
            .insert_unique(&key(100, 300), tid(5000), &only_one_live)
            .unwrap();
        assert_eq!(r, InsertOutcome::Inserted);
        assert_eq!(*checked.lock().unwrap(), 600);
        // With one live version at the far end of the chain: duplicate.
        tree.insert(&key(100, 300), live_one).unwrap();
        let r = tree
            .insert_unique(&key(100, 300), tid(6000), &only_one_live)
            .unwrap();
        assert_eq!(r, InsertOutcome::Duplicate);
        check_invariants(&tree);
    }

    #[test]
    fn concurrent_unique_inserts_admit_one_per_key() {
        let (_pool, tree) = concurrent_setup(4);
        let live = |_: Tid| Ok(UniqueCheck::Live);
        let wins: Vec<usize> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..4u32)
                .map(|t| {
                    let (tree, live) = (&tree, &live);
                    s.spawn(move || {
                        (0..300u32)
                            .filter(|&k| {
                                let r = tree.insert_unique(
                                    &key(k as i64, 100),
                                    tid(t * 1000 + k),
                                    live,
                                );
                                fail_on_exhausted(r, "unique insert") == InsertOutcome::Inserted
                            })
                            .count()
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(
            wins.iter().sum::<usize>(),
            300,
            "exactly one insert per key"
        );
        assert_eq!(check_invariants(&tree).len(), 300);
    }

    fn in_range(k: &[u8], lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> bool {
        (match lo {
            Bound::Included(b) => k >= b.as_slice(),
            Bound::Excluded(b) => k > b.as_slice(),
            Bound::Unbounded => true,
        }) && (match hi {
            Bound::Included(b) => k <= b.as_slice(),
            Bound::Excluded(b) => k < b.as_slice(),
            Bound::Unbounded => true,
        })
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(i64, usize, u32),
        Delete(usize),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (0i64..200, 0usize..600, 0u32..50).prop_map(|(k, p, t)| Op::Insert(k, p, t)),
            1 => any::<usize>().prop_map(Op::Delete),
        ]
    }

    fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Bound::Unbounded),
            (0i64..200).prop_map(|i| Bound::Included(key(i, 0))),
            (0i64..200).prop_map(|i| Bound::Excluded(key(i, 0))),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn matches_model(ops in prop::collection::vec(op(), 1..400), lo in bound(), hi in bound()) {
            let (_mem, _pool, tree) = setup();
            let mut model: BTreeSet<(Vec<u8>, Tid)> = BTreeSet::new();
            for op in ops {
                match op {
                    Op::Insert(k, pad, t) => {
                        let e = (key(k, pad), tid(t));
                        let r = tree.insert(&e.0, e.1);
                        if model.insert(e) {
                            prop_assert!(r.is_ok());
                        } else {
                            prop_assert!(matches!(r, Err(BTreeError::Duplicate)));
                        }
                    }
                    Op::Delete(i) => {
                        if let Some(e) = model.iter().nth(i % model.len().max(1)).cloned() {
                            prop_assert!(tree.delete(&e.0, e.1).unwrap());
                            model.remove(&e);
                        }
                    }
                }
            }
            prop_assert_eq!(check_invariants(&tree).len(), model.len());
            prop_assert_eq!(all(&tree), model.iter().cloned().collect::<Vec<_>>());
            let got: Vec<_> = tree.scan(lo.clone(), hi.clone()).unwrap().collect::<Result<_>>().unwrap();
            let want: Vec<_> = model.iter().filter(|(k, _)| in_range(k, &lo, &hi)).cloned().collect();
            prop_assert_eq!(got, want);
        }
    }
}
