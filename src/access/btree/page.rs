//! B+Tree page layout on top of the slotted page.
//!
//! ```text
//! special (16 bytes): prev u32 | next u32 | level u32 | flags u16 | pad u16
//! slot 0            : high key (only if the page is not rightmost)
//! slots 1..         : data items, in key order
//! ```
//!
//! Leaf items are full entry keys (`key || tid`). Internal items are
//! `separator || child block (u32 LE)`; the first data item of an internal
//! page has an empty separator (minus infinity), as in PostgreSQL.
//! High keys are bare keys. All keys compare with `memcmp`.

use crate::storage::page::{PAGE_HEADER_SIZE, PAGE_SIZE, PageKind};
use crate::storage::slotted::{ITEM_ALIGN, LINE_POINTER_SIZE, SlotId, SlottedPage};

use super::{BTreeError, Result};

/// Size of the B+Tree special area.
pub const SPECIAL_SIZE: usize = 16;

/// "No page" in `prev`/`next`.
pub const P_NONE: u32 = u32::MAX;

/// Size of a child block pointer in internal items.
pub const CHILD_LEN: usize = 4;

/// Space for line pointers and items on a B+Tree page.
pub const USABLE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - SPECIAL_SIZE;

/// Largest item. A quarter of the page guarantees that any split leaves
/// both halves, each with a high key, within one page (PostgreSQL allows a
/// third because it truncates high keys; we store them in full).
pub const MAX_ITEM_SIZE: usize = (USABLE / 4 - LINE_POINTER_SIZE) & !(ITEM_ALIGN - 1);

/// Page flags.
pub mod flags {
    pub const LEAF: u16 = 0x0001;
    pub const ROOT: u16 = 0x0002;
    /// Reserved for page deletion (vacuum).
    pub const DELETED: u16 = 0x0004;
    /// Reserved for page deletion (vacuum).
    pub const HALF_DEAD: u16 = 0x0008;
    /// Reserved for crash recovery of splits (WAL step).
    pub const INCOMPLETE_SPLIT: u16 = 0x0010;
    /// The metapage.
    pub const META: u16 = 0x0020;
}

/// B+Tree per-page metadata stored in the special area.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Opaque {
    pub prev: u32,
    pub next: u32,
    pub level: u32,
    pub flags: u16,
}

impl Opaque {
    pub fn is_leaf(&self) -> bool {
        self.level == 0
    }

    pub fn is_rightmost(&self) -> bool {
        self.next == P_NONE
    }

    pub fn is_root(&self) -> bool {
        self.flags & flags::ROOT != 0
    }

    fn decode(b: &[u8]) -> Self {
        let u32_at = |i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        Self {
            prev: u32_at(0),
            next: u32_at(4),
            level: u32_at(8),
            flags: u16::from_le_bytes([b[12], b[13]]),
        }
    }

    fn encode(&self, b: &mut [u8]) {
        b[0..4].copy_from_slice(&self.prev.to_le_bytes());
        b[4..8].copy_from_slice(&self.next.to_le_bytes());
        b[8..12].copy_from_slice(&self.level.to_le_bytes());
        b[12..14].copy_from_slice(&self.flags.to_le_bytes());
        b[14..16].fill(0);
    }
}

/// Builds an internal item.
pub fn internal_item(sep: &[u8], child: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(sep.len() + CHILD_LEN);
    v.extend_from_slice(sep);
    v.extend_from_slice(&child.to_le_bytes());
    v
}

/// Space an item takes on a page (aligned data + line pointer).
pub fn item_cost(len: usize) -> usize {
    len.next_multiple_of(ITEM_ALIGN) + LINE_POINTER_SIZE
}

/// A B+Tree node page view.
#[derive(Debug)]
pub struct Node<B> {
    page: SlottedPage<B>,
    op: Opaque,
}

impl<B: AsRef<[u8]>> Node<B> {
    /// Opens a node page (not the metapage).
    pub fn open(buf: B) -> Result<Self> {
        let page = SlottedPage::open(buf)?;
        if page.special().len() != SPECIAL_SIZE {
            return Err(BTreeError::Corrupt("not a btree page"));
        }
        let op = Opaque::decode(page.special());
        if op.flags & flags::META != 0 {
            return Err(BTreeError::Corrupt("unexpected metapage"));
        }
        Ok(Self { page, op })
    }

    pub fn opaque(&self) -> Opaque {
        self.op
    }

    pub fn slot_count(&self) -> u16 {
        self.page.slot_count()
    }

    /// First slot holding a data item.
    pub fn first_data(&self) -> u16 {
        if self.op.is_rightmost() { 0 } else { 1 }
    }

    pub fn item(&self, slot: u16) -> Result<&[u8]> {
        self.page
            .get_item(SlotId(slot))
            .ok_or(BTreeError::Corrupt("missing item"))
    }

    /// Upper bound of keys on this page; `None` means +infinity.
    pub fn high_key(&self) -> Result<Option<&[u8]>> {
        if self.op.is_rightmost() {
            Ok(None)
        } else {
            self.item(0).map(Some)
        }
    }

    /// Sort key of a data item.
    pub fn key_at(&self, slot: u16) -> Result<&[u8]> {
        let item = self.item(slot)?;
        if self.op.is_leaf() {
            Ok(item)
        } else {
            item.get(..item.len().wrapping_sub(CHILD_LEN))
                .ok_or(BTreeError::Corrupt("internal item too short"))
        }
    }

    /// Child pointer of an internal data item.
    pub fn child_at(&self, slot: u16) -> Result<u32> {
        let item = self.item(slot)?;
        let b = item
            .get(item.len().wrapping_sub(CHILD_LEN)..)
            .filter(|b| b.len() == CHILD_LEN)
            .ok_or(BTreeError::Corrupt("internal item too short"))?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Lehman-Yao: the target lies to the right if it is not below the
    /// high key.
    /// A half-dead or deleted page is always passed to the right
    /// (PostgreSQL's P_IGNORE): its key range now belongs to its right
    /// sibling.
    pub fn must_move_right(&self, target: &[u8]) -> Result<bool> {
        if self.op.flags & (flags::HALF_DEAD | flags::DELETED) != 0 && !self.op.is_rightmost() {
            return Ok(true);
        }
        Ok(match self.high_key()? {
            Some(high) => target >= high,
            None => false,
        })
    }

    /// First data slot whose key is `>= target` (may be `slot_count`).
    pub fn lower_bound(&self, target: &[u8]) -> Result<u16> {
        let (mut lo, mut hi) = (self.first_data(), self.slot_count());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key_at(mid)? < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Internal page: child of the last data item whose key is `<= target`.
    pub fn choose_child(&self, target: &[u8]) -> Result<u32> {
        let (mut lo, mut hi) = (self.first_data(), self.slot_count());
        if lo >= hi {
            return Err(BTreeError::Corrupt("internal page without items"));
        }
        // Find the first key > target; the item before it is the answer.
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key_at(mid)? <= target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == self.first_data() {
            return Err(BTreeError::Corrupt("first internal key above target"));
        }
        self.child_at(lo - 1)
    }

    /// Slot of the internal item pointing at `child`, if on this page.
    pub fn find_downlink(&self, child: u32) -> Result<Option<u16>> {
        for s in self.first_data()..self.slot_count() {
            if self.child_at(s)? == child {
                return Ok(Some(s));
            }
        }
        Ok(None)
    }

    /// Copies all data items in order.
    pub fn data_items(&self) -> Result<Vec<Vec<u8>>> {
        (self.first_data()..self.slot_count())
            .map(|s| self.item(s).map(<[u8]>::to_vec))
            .collect()
    }

    /// Free space for one more item (including its line pointer).
    pub fn free_space(&self) -> usize {
        self.page.upper() - self.page.lower()
    }
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> Node<B> {
    /// Inserts an item at `slot`. `false` if it does not fit.
    pub fn insert_at(&mut self, slot: u16, item: &[u8]) -> Result<bool> {
        Ok(self.page.insert_item_at(SlotId(slot), item)?)
    }

    pub fn remove_at(&mut self, slot: u16) -> Result<()> {
        Ok(self.page.remove_item_at(SlotId(slot))?)
    }

    pub fn set_opaque(&mut self, op: Opaque) {
        op.encode(self.page.special_mut());
        self.op = op;
    }
}

/// Formats `buf` as a node page with the given metadata, high key and
/// items (in order).
pub fn build_page(
    buf: &mut [u8],
    op: Opaque,
    high: Option<&[u8]>,
    items: &[Vec<u8>],
) -> Result<()> {
    let mut page = SlottedPage::init(&mut *buf, PageKind::BTree, SPECIAL_SIZE)?;
    op.encode(page.special_mut());
    if op.is_rightmost() != high.is_none() {
        return Err(BTreeError::Corrupt("high key presence does not match next"));
    }
    for (slot, item) in high
        .into_iter()
        .chain(items.iter().map(Vec::as_slice))
        .enumerate()
    {
        if !page.insert_item_at(SlotId(slot as u16), item)? {
            return Err(BTreeError::Corrupt("page overflow while building"));
        }
    }
    Ok(())
}

/// Formats `buf` as the metapage.
pub fn build_meta(buf: &mut [u8], meta: &Meta) -> Result<()> {
    let mut page = SlottedPage::init(&mut *buf, PageKind::BTree, SPECIAL_SIZE)?;
    Opaque {
        prev: P_NONE,
        next: P_NONE,
        level: 0,
        flags: flags::META,
    }
    .encode(page.special_mut());
    if page.add_item(&meta.encode())?.is_none() {
        return Err(BTreeError::Corrupt("metapage overflow"));
    }
    Ok(())
}

const META_MAGIC: u32 = 0x4254_5245; // "BTRE"
const META_VERSION: u32 = 1;

/// Metapage contents: where the root is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Meta {
    pub root: u32,
    pub level: u32,
}

impl Meta {
    fn encode(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&META_MAGIC.to_le_bytes());
        b[4..8].copy_from_slice(&META_VERSION.to_le_bytes());
        b[8..12].copy_from_slice(&self.root.to_le_bytes());
        b[12..16].copy_from_slice(&self.level.to_le_bytes());
        b
    }

    /// Reads the metapage.
    pub fn read(buf: &[u8]) -> Result<Self> {
        let page = SlottedPage::open(buf)?;
        let op = Opaque::decode(page.special());
        let b = page
            .get_item(SlotId(0))
            .filter(|b| b.len() == 16 && op.flags & flags::META != 0)
            .ok_or(BTreeError::NotABTree)?;
        let u32_at = |i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        if u32_at(0) != META_MAGIC || u32_at(4) != META_VERSION {
            return Err(BTreeError::NotABTree);
        }
        Ok(Self {
            root: u32_at(8),
            level: u32_at(12),
        })
    }

    /// Overwrites the metapage contents in place.
    pub fn write(&self, buf: &mut [u8]) -> Result<()> {
        let mut page = SlottedPage::open(buf)?;
        let item = page
            .get_item_mut(SlotId(0))
            .filter(|b| b.len() == 16)
            .ok_or(BTreeError::NotABTree)?;
        item.copy_from_slice(&self.encode());
        Ok(())
    }
}
