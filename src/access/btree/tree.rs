//! B+Tree operations with Lehman-Yao concurrency.
//!
//! * Descent holds one content lock at a time. If the target is not below a
//!   page's high key, the page has split since its parent was read: release
//!   it and follow the right-link ("move right").
//! * Insert remembers the parents it passed. A split builds the left and new
//!   right pages under both locks, then locks the parent (moving right in
//!   the parent level if it split too) before releasing the children, and
//!   inserts the downlink. Locks are taken bottom-up and left-to-right only,
//!   so writers cannot deadlock (as in PostgreSQL's nbtree).
//! * A split of the root creates a new root and updates the metapage while
//!   still holding the old root's halves.
//! * Scans copy one leaf at a time and remember its right-link, so a later
//!   split of that leaf only moves entries that were already copied.
//!
//! Pins: an insert keeps every page of its split path pinned until it
//! returns, up to about two per level plus the metapage.

use std::ops::Bound;
use std::sync::Arc;

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

const META_BLOCK: u32 = 0;

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
}

impl BTree {
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
        }
        {
            let mut m = mpin.write();
            build_meta(
                &mut m,
                &Meta {
                    root: rblk.0,
                    level: 0,
                },
            )?;
            m.mark_dirty();
        }
        drop((rpin, mpin));
        Ok(Self { rel, pool })
    }

    /// Opens an existing index.
    pub fn open(rel: RelFileId, pool: Arc<BufferPool>) -> Result<Self> {
        let tree = Self { rel, pool };
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
        let (mut pin, mut blk, mut stack) = self.descend(&entry)?;
        loop {
            let page = pin.write();
            let node = Node::open(&page[..])?;
            if node.must_move_right(&entry)? {
                let next = node.opaque().next;
                drop(page);
                blk = next;
                pin = self.fetch(next)?;
                continue;
            }
            let pos = node.lower_bound(&entry)?;
            if pos < node.slot_count() && node.key_at(pos)? == entry.as_slice() {
                return Err(BTreeError::Duplicate);
            }
            return self.insert_item(&pin, page, blk, pos, entry, &mut stack);
        }
    }

    /// Removes the entry `key || tid`. Returns whether it existed.
    /// Pages are never merged; empty pages are left for vacuum.
    pub fn delete(&self, key: &[u8], tid: Tid) -> Result<bool> {
        let entry = entry_key(key, tid)?;
        let (mut pin, _, _) = self.descend(&entry)?;
        loop {
            let mut page = pin.write();
            let mut node = Node::open(&mut page[..])?;
            if node.must_move_right(&entry)? {
                let next = node.opaque().next;
                drop(page);
                pin = self.fetch(next)?;
                continue;
            }
            let pos = node.lower_bound(&entry)?;
            if pos < node.slot_count() && node.key_at(pos)? == entry.as_slice() {
                node.remove_at(pos)?;
                page.mark_dirty();
                return Ok(true);
            }
            return Ok(false);
        }
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
    /// recursing into the parent if needed. `pin` is the caller's pin of
    /// that page and stays alive for the whole call.
    fn insert_item(
        &self,
        _pin: &PinGuard<'_>,
        mut page: PageMut<'_>,
        blk: u32,
        pos: u16,
        item: Vec<u8>,
        stack: &mut Vec<u32>,
    ) -> Result<()> {
        if Node::open(&mut page[..])?.insert_at(pos, &item)? {
            page.mark_dirty();
            return Ok(());
        }

        let (rblk, rpin) = self.pool.extend(self.rel)?;
        let mut rpage = rpin.write();
        let (sep, op) = self.split(blk, &mut page, rblk.0, &mut rpage, pos, item)?;
        page.mark_dirty();
        rpage.mark_dirty();
        if op.is_root() {
            return self.new_root(blk, &mut page, rblk.0, &sep, op.level);
        }

        // Lock the parent before releasing the children, so nobody can split
        // the new right page and look for its (not yet inserted) downlink.
        let mut pblk = match stack.pop() {
            Some(b) => b,
            None => self.leftmost_at(op.level + 1)?,
        };
        let mut ppin = self.fetch(pblk)?;
        loop {
            let ppage = ppin.write();
            let found = {
                let pnode = Node::open(&ppage[..])?;
                match pnode.find_downlink(blk)? {
                    Some(slot) => Ok(slot),
                    None => Err(pnode.opaque().next),
                }
            };
            match found {
                Ok(slot) => {
                    drop(rpage);
                    drop(page);
                    let downlink = internal_item(&sep, rblk.0);
                    return self.insert_item(&ppin, ppage, pblk, slot + 1, downlink, stack);
                }
                Err(next) => {
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

    /// Splits the full page `lblk` (adding `item` at `pos`) into itself and
    /// the new page `rblk`. Returns the separator (the right page's lower
    /// bound and the left page's new high key) and the left page's opaque.
    fn split(
        &self,
        lblk: u32,
        lpage: &mut PageMut<'_>,
        rblk: u32,
        rpage: &mut PageMut<'_>,
        pos: u16,
        item: Vec<u8>,
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

        let left_op = Opaque { next: rblk, ..op };
        let right_op = Opaque {
            prev: lblk,
            next: op.next,
            level: op.level,
            flags: op.flags & !flags::ROOT,
        };
        build_page(lpage, left_op, Some(&sep), &items)?;
        build_page(rpage, right_op, high.as_deref(), &right)?;

        if op.next != P_NONE {
            // Left-to-right lock order: left, new right, old right sibling.
            let spin = self.fetch(op.next)?;
            let mut spage = spin.write();
            let mut snode = Node::open(&mut spage[..])?;
            let sop = Opaque {
                prev: rblk,
                ..snode.opaque()
            };
            snode.set_opaque(sop);
            spage.mark_dirty();
        }
        Ok((sep, left_op))
    }

    /// Creates a new root above the split old root and points the metapage
    /// at it. Called with both halves still locked.
    fn new_root(
        &self,
        lblk: u32,
        lpage: &mut PageMut<'_>,
        rblk: u32,
        sep: &[u8],
        level: u32,
    ) -> Result<()> {
        let (nblk, npin) = self.pool.extend(self.rel)?;
        {
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
        }
        {
            let mut lnode = Node::open(&mut lpage[..])?;
            let op = Opaque {
                flags: lnode.opaque().flags & !flags::ROOT,
                ..lnode.opaque()
            };
            lnode.set_opaque(op);
        }
        lpage.mark_dirty();

        let mpin = self.fetch(META_BLOCK)?;
        let mut m = mpin.write();
        Meta {
            root: nblk.0,
            level: level + 1,
        }
        .write(&mut m)?;
        m.mark_dirty();
        Ok(())
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
        for level in (0..=meta.level).rev() {
            let mut blk = tree.leftmost_at(level).unwrap();
            let mut prev = P_NONE;
            let mut lower: Option<Vec<u8>> = None;
            loop {
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
