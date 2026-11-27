//! Slotted page layout.
//!
//! ```text
//! +--------+----------------------+------------+-------------+---------+
//! | header | line pointers ->     | free space |  <- items   | special |
//! +--------+----------------------+------------+-------------+---------+
//! 0        24                   lower        upper       special  PAGE_SIZE
//! ```
//!
//! Line pointers (4 bytes, same packing as PostgreSQL's `ItemIdData`):
//! bits 0..15 = offset, bits 15..17 = state, bits 17..32 = length.
//!
//! Slot numbers are stable: compaction moves item data but never renumbers
//! slots, so a TID (block, slot) stays valid while an index may reference it.
//! Slots are 0-based (PostgreSQL's `OffsetNumber` is 1-based).
//!
//! Item lifecycle: `Normal` -> `mark_dead` -> `Dead` (storage kept until
//! `compact`) -> `mark_unused` -> `Unused` (slot reusable by `add_item`).

use super::page::{
    self, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageHeader, PageKind, SPECIAL_ALIGN, off,
};
use super::{Result, StorageError};

/// Size of one line pointer.
pub const LINE_POINTER_SIZE: usize = 4;

/// Item data start offsets are aligned to this (like PostgreSQL's MAXALIGN).
pub const ITEM_ALIGN: usize = 8;

/// Largest item that fits on an empty page without a special area.
pub const MAX_ITEM_SIZE: usize =
    (PAGE_SIZE - PAGE_HEADER_SIZE - LINE_POINTER_SIZE) & !(ITEM_ALIGN - 1);

const LP_OFF_MASK: u32 = 0x7fff;
const LP_STATE_SHIFT: u32 = 15;
const LP_LEN_SHIFT: u32 = 17;

const STATE_UNUSED: u32 = 0;
const STATE_NORMAL: u32 = 1;
// 2 is reserved (PostgreSQL's LP_REDIRECT, used by HOT; not decided yet).
const STATE_DEAD: u32 = 3;

// `upper` stays aligned only if the special area start is item-aligned.
const _: () = assert!(SPECIAL_ALIGN.is_multiple_of(ITEM_ALIGN));
// Offsets and lengths must fit in 15 bits.
const _: () = assert!(PAGE_SIZE - 1 <= LP_OFF_MASK as usize);

/// Index of a line pointer within a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId(pub u16);

/// State of a line pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemState {
    /// Free; may be reused by a new item.
    Unused,
    /// Points to live item data.
    Normal,
    /// Item is gone but the slot may still be referenced (e.g. by an index).
    Dead,
}

/// Decoded line pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemId {
    pub off: u16,
    pub len: u16,
    pub state: ItemState,
}

impl ItemId {
    const UNUSED: ItemId = ItemId {
        off: 0,
        len: 0,
        state: ItemState::Unused,
    };

    fn decode(raw: u32) -> Option<Self> {
        let state = match (raw >> LP_STATE_SHIFT) & 0b11 {
            STATE_UNUSED => ItemState::Unused,
            STATE_NORMAL => ItemState::Normal,
            STATE_DEAD => ItemState::Dead,
            _ => return None,
        };
        Some(Self {
            off: (raw & LP_OFF_MASK) as u16,
            len: (raw >> LP_LEN_SHIFT) as u16,
            state,
        })
    }

    fn encode(self) -> u32 {
        let state = match self.state {
            ItemState::Unused => STATE_UNUSED,
            ItemState::Normal => STATE_NORMAL,
            ItemState::Dead => STATE_DEAD,
        };
        (u32::from(self.off) & LP_OFF_MASK)
            | (state << LP_STATE_SHIFT)
            | (u32::from(self.len) << LP_LEN_SHIFT)
    }

    /// Whether this line pointer still owns item bytes on the page.
    pub fn has_storage(&self) -> bool {
        self.len > 0
    }
}

/// A slotted-page view over a `PAGE_SIZE` buffer.
///
/// Constructed via [`SlottedPage::open`] (validates) or [`SlottedPage::init`]
/// (formats). All mutating methods preserve the invariants checked by `open`.
#[derive(Debug)]
pub struct SlottedPage<B> {
    buf: B,
}

impl<B: AsRef<[u8]>> SlottedPage<B> {
    /// Validates the header and every line pointer of an existing page.
    pub fn open(buf: B) -> Result<Self> {
        let page = Self { buf };
        let h = PageHeader::decode(page.bytes())?;
        if !(usize::from(h.lower) - PAGE_HEADER_SIZE).is_multiple_of(LINE_POINTER_SIZE) {
            return Err(StorageError::CorruptPage("line pointer array not whole"));
        }
        if !usize::from(h.upper).is_multiple_of(ITEM_ALIGN) {
            return Err(StorageError::CorruptPage("misaligned upper"));
        }
        for i in 0..page.slot_count() {
            let id = ItemId::decode(page.raw_lp(i))
                .ok_or(StorageError::CorruptPage("invalid line pointer state"))?;
            check_item_id(id, h.upper, h.special)?;
        }
        Ok(page)
    }

    /// Releases the underlying buffer.
    pub fn into_inner(self) -> B {
        self.buf
    }

    /// Raw page bytes.
    pub fn bytes(&self) -> &[u8] {
        self.buf.as_ref()
    }

    pub fn lsn(&self) -> Lsn {
        Lsn(page::read_u64(self.bytes(), off::LSN))
    }

    /// End of the line pointer array.
    pub fn lower(&self) -> usize {
        usize::from(page::read_u16(self.bytes(), off::LOWER))
    }

    /// Start of item data.
    pub fn upper(&self) -> usize {
        usize::from(page::read_u16(self.bytes(), off::UPPER))
    }

    /// Start of the special area.
    pub fn special_offset(&self) -> usize {
        usize::from(page::read_u16(self.bytes(), off::SPECIAL))
    }

    /// Number of line pointers (including unused and dead ones).
    pub fn slot_count(&self) -> u16 {
        ((self.lower() - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE) as u16
    }

    /// Free bytes for item data assuming a new line pointer is needed
    /// (same definition as PostgreSQL's `PageGetFreeSpace`).
    pub fn free_space(&self) -> usize {
        (self.upper() - self.lower()).saturating_sub(LINE_POINTER_SIZE)
    }

    /// Line pointer of `slot`, or `None` if out of range.
    pub fn item_id(&self, slot: SlotId) -> Option<ItemId> {
        if slot.0 >= self.slot_count() {
            return None;
        }
        ItemId::decode(self.raw_lp(slot.0))
    }

    /// Item bytes of `slot` if it is `Normal`.
    pub fn get_item(&self, slot: SlotId) -> Option<&[u8]> {
        let id = self.item_id(slot)?;
        if id.state != ItemState::Normal {
            return None;
        }
        let (o, l) = (usize::from(id.off), usize::from(id.len));
        Some(&self.bytes()[o..o + l])
    }

    /// The special area (empty if the page has none).
    pub fn special(&self) -> &[u8] {
        &self.bytes()[self.special_offset()..]
    }

    fn raw_lp(&self, slot: u16) -> u32 {
        page::read_u32(self.bytes(), lp_offset(slot))
    }

    fn first_unused_slot(&self) -> Option<SlotId> {
        (0..self.slot_count())
            .map(SlotId)
            .find(|&s| matches!(self.item_id(s), Some(id) if id.state == ItemState::Unused))
    }
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> SlottedPage<B> {
    /// Zeroes `buf` and formats it as an empty page with a special area of
    /// `special_size` bytes (a multiple of `SPECIAL_ALIGN`).
    pub fn init(mut buf: B, kind: PageKind, special_size: usize) -> Result<Self> {
        page::check_page_len(buf.as_ref())?;
        let h = PageHeader::new_empty(kind, special_size)?;
        buf.as_mut().fill(0);
        h.encode(buf.as_mut())?;
        Ok(Self { buf })
    }

    /// Mutable raw page bytes. Callers must not break slotted-page invariants.
    fn bytes_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut()
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.bytes_mut()[off::LSN..off::LSN + 8].copy_from_slice(&lsn.0.to_le_bytes());
    }

    /// Mutable item bytes of `slot` if it is `Normal` (in-place updates of
    /// fixed-size fields such as hint bits; the length cannot change).
    pub fn get_item_mut(&mut self, slot: SlotId) -> Option<&mut [u8]> {
        let id = self.item_id(slot)?;
        if id.state != ItemState::Normal {
            return None;
        }
        let (o, l) = (usize::from(id.off), usize::from(id.len));
        Some(&mut self.bytes_mut()[o..o + l])
    }

    /// The special area (mutable).
    pub fn special_mut(&mut self) -> &mut [u8] {
        let s = self.special_offset();
        &mut self.bytes_mut()[s..]
    }

    /// Adds an item, reusing an unused slot if any.
    ///
    /// Returns `Ok(None)` if the page does not have enough contiguous free
    /// space (the caller may `compact` and retry, or use another page).
    pub fn add_item(&mut self, data: &[u8]) -> Result<Option<SlotId>> {
        let len = data.len();
        if len == 0 || len > MAX_ITEM_SIZE {
            return Err(StorageError::InvalidItemSize { len });
        }
        let reuse = self.first_unused_slot();
        let lower = self.lower();
        let upper = self.upper();
        let new_lower = if reuse.is_some() {
            lower
        } else {
            lower + LINE_POINTER_SIZE
        };
        let aligned = len.next_multiple_of(ITEM_ALIGN);
        if upper < new_lower || upper - new_lower < aligned {
            return Ok(None);
        }
        let new_upper = upper - aligned;
        let slot = reuse.unwrap_or(SlotId(self.slot_count()));

        let bytes = self.bytes_mut();
        bytes[new_upper..new_upper + len].copy_from_slice(data);
        bytes[new_upper + len..upper].fill(0);
        self.set_lp(
            slot.0,
            ItemId {
                off: new_upper as u16,
                len: len as u16,
                state: ItemState::Normal,
            },
        );
        self.set_lower(new_lower);
        self.set_upper(new_upper);
        Ok(Some(slot))
    }

    /// `Normal` -> `Dead`. The item bytes stay until `compact`.
    pub fn mark_dead(&mut self, slot: SlotId) -> Result<()> {
        let mut id = self.expect_state(slot, ItemState::Normal)?;
        id.state = ItemState::Dead;
        self.set_lp(slot.0, id);
        Ok(())
    }

    /// `Dead` -> `Unused`. Only safe once nothing references the slot.
    pub fn mark_unused(&mut self, slot: SlotId) -> Result<()> {
        self.expect_state(slot, ItemState::Dead)?;
        self.set_lp(slot.0, ItemId::UNUSED);
        Ok(())
    }

    /// Defragments item data toward the special area.
    ///
    /// Slot numbers of `Normal` items are preserved, dead items lose their
    /// storage, trailing unused line pointers are truncated, and the free
    /// space between `lower` and `upper` is zeroed.
    pub fn compact(&mut self) {
        let mut items: Vec<(u16, usize, usize)> = Vec::new();
        for s in 0..self.slot_count() {
            if let Some(id) = self.item_id(SlotId(s)) {
                match id.state {
                    ItemState::Normal => items.push((s, usize::from(id.off), usize::from(id.len))),
                    ItemState::Dead if id.has_storage() => self.set_lp(
                        s,
                        ItemId {
                            off: 0,
                            len: 0,
                            state: ItemState::Dead,
                        },
                    ),
                    _ => {}
                }
            }
        }

        // Highest offset first: each item moves toward higher addresses and
        // all not-yet-moved items lie strictly below its old start.
        items.sort_unstable_by_key(|&(_, off, _)| std::cmp::Reverse(off));
        let mut pos = self.special_offset();
        for (slot, old, len) in items {
            let aligned = len.next_multiple_of(ITEM_ALIGN);
            pos -= aligned;
            let bytes = self.bytes_mut();
            bytes.copy_within(old..old + len, pos);
            bytes[pos + len..pos + aligned].fill(0);
            self.set_lp(
                slot,
                ItemId {
                    off: pos as u16,
                    len: len as u16,
                    state: ItemState::Normal,
                },
            );
        }

        let mut count = self.slot_count();
        while count > 0
            && matches!(self.item_id(SlotId(count - 1)), Some(id) if id.state == ItemState::Unused)
        {
            count -= 1;
        }
        let lower = lp_offset(count);
        self.bytes_mut()[lower..pos].fill(0);
        self.set_lower(lower);
        self.set_upper(pos);
    }

    /// Inserts an item as slot `pos`, shifting the line pointers at `pos`
    /// and after up by one. Returns `false` if there is not enough free space.
    ///
    /// For index pages, whose line pointer order is the key order. Unlike
    /// [`SlottedPage::add_item`], this renumbers later slots, so it must not
    /// be used on heap pages (TIDs there must stay stable).
    pub fn insert_item_at(&mut self, pos: SlotId, data: &[u8]) -> Result<bool> {
        let len = data.len();
        if len == 0 || len > MAX_ITEM_SIZE {
            return Err(StorageError::InvalidItemSize { len });
        }
        let count = self.slot_count();
        if pos.0 > count {
            return Err(StorageError::InvalidSlot { slot: pos.0 });
        }
        let (lower, upper) = (self.lower(), self.upper());
        let aligned = len.next_multiple_of(ITEM_ALIGN);
        if upper - lower < aligned + LINE_POINTER_SIZE {
            return Ok(false);
        }
        let new_upper = upper - aligned;
        let at = lp_offset(pos.0);
        let bytes = self.bytes_mut();
        bytes[new_upper..new_upper + len].copy_from_slice(data);
        bytes[new_upper + len..upper].fill(0);
        bytes.copy_within(at..lower, at + LINE_POINTER_SIZE);
        self.set_lp(
            pos.0,
            ItemId {
                off: new_upper as u16,
                len: len as u16,
                state: ItemState::Normal,
            },
        );
        self.set_lower(lower + LINE_POINTER_SIZE);
        self.set_upper(new_upper);
        Ok(true)
    }

    /// Removes slot `pos`, shifting later line pointers down by one, and
    /// closes the gap in the item data immediately (PostgreSQL's
    /// `PageIndexTupleDelete`). Index pages only, as with `insert_item_at`.
    pub fn remove_item_at(&mut self, pos: SlotId) -> Result<()> {
        let id = self
            .item_id(pos)
            .ok_or(StorageError::InvalidSlot { slot: pos.0 })?;
        if id.has_storage() {
            let (off, size) = (
                usize::from(id.off),
                usize::from(id.len).next_multiple_of(ITEM_ALIGN),
            );
            let upper = self.upper();
            // Move everything stored below the removed item up by `size`.
            self.bytes_mut().copy_within(upper..off, upper + size);
            self.bytes_mut()[upper..upper + size].fill(0);
            for s in 0..self.slot_count() {
                if let Some(mut other) = self.item_id(SlotId(s))
                    && other.has_storage()
                    && usize::from(other.off) < off
                {
                    other.off += size as u16;
                    self.set_lp(s, other);
                }
            }
            self.set_upper(upper + size);
        }
        let lower = self.lower();
        let at = lp_offset(pos.0);
        let bytes = self.bytes_mut();
        bytes.copy_within(at + LINE_POINTER_SIZE..lower, at);
        bytes[lower - LINE_POINTER_SIZE..lower].fill(0);
        self.set_lower(lower - LINE_POINTER_SIZE);
        Ok(())
    }

    fn expect_state(&self, slot: SlotId, expected: ItemState) -> Result<ItemId> {
        let id = self
            .item_id(slot)
            .ok_or(StorageError::InvalidSlot { slot: slot.0 })?;
        if id.state != expected {
            return Err(StorageError::InvalidSlotState {
                slot: slot.0,
                state: id.state,
            });
        }
        Ok(id)
    }

    fn set_lp(&mut self, slot: u16, id: ItemId) {
        let at = lp_offset(slot);
        self.bytes_mut()[at..at + LINE_POINTER_SIZE].copy_from_slice(&id.encode().to_le_bytes());
    }

    fn set_lower(&mut self, v: usize) {
        self.bytes_mut()[off::LOWER..off::LOWER + 2].copy_from_slice(&(v as u16).to_le_bytes());
    }

    fn set_upper(&mut self, v: usize) {
        self.bytes_mut()[off::UPPER..off::UPPER + 2].copy_from_slice(&(v as u16).to_le_bytes());
    }
}

fn lp_offset(slot: u16) -> usize {
    PAGE_HEADER_SIZE + usize::from(slot) * LINE_POINTER_SIZE
}

fn check_item_id(id: ItemId, upper: u16, special: u16) -> Result<()> {
    match id.state {
        ItemState::Unused if id.off != 0 || id.len != 0 => {
            return Err(StorageError::CorruptPage("unused line pointer has storage"));
        }
        ItemState::Normal if id.len == 0 => {
            return Err(StorageError::CorruptPage(
                "normal line pointer without storage",
            ));
        }
        ItemState::Dead if id.len == 0 && id.off != 0 => {
            return Err(StorageError::CorruptPage(
                "dead line pointer with offset only",
            ));
        }
        _ => {}
    }
    if id.has_storage() {
        let (o, l) = (usize::from(id.off), usize::from(id.len));
        if !o.is_multiple_of(ITEM_ALIGN) || o < usize::from(upper) || o + l > usize::from(special) {
            return Err(StorageError::CorruptPage("item out of bounds"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::AlignedBuf;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn empty(special: usize) -> SlottedPage<Vec<u8>> {
        SlottedPage::init(vec![0u8; PAGE_SIZE], PageKind::Heap, special).unwrap()
    }

    #[test]
    fn init_empty_page() {
        let p = empty(16);
        assert_eq!(p.slot_count(), 0);
        assert_eq!(p.lower(), PAGE_HEADER_SIZE);
        assert_eq!(p.upper(), PAGE_SIZE - 16);
        assert_eq!(p.special().len(), 16);
        assert_eq!(
            p.free_space(),
            PAGE_SIZE - 16 - PAGE_HEADER_SIZE - LINE_POINTER_SIZE
        );
        assert_eq!(p.lsn(), Lsn::INVALID);
    }

    #[test]
    fn works_on_aligned_buf() {
        let buf = AlignedBuf::new(PAGE_SIZE).unwrap();
        let mut p = SlottedPage::init(buf, PageKind::Heap, 0).unwrap();
        let s = p.add_item(b"hello").unwrap().unwrap();
        p.set_lsn(Lsn(42));
        let p = SlottedPage::open(p.into_inner()).unwrap();
        assert_eq!(p.get_item(s), Some(&b"hello"[..]));
        assert_eq!(p.lsn(), Lsn(42));
    }

    #[test]
    fn add_and_get_items() {
        let mut p = empty(0);
        let a = p.add_item(b"abc").unwrap().unwrap();
        let b = p.add_item(&[7u8; 100]).unwrap().unwrap();
        assert_eq!((a, b), (SlotId(0), SlotId(1)));
        assert_eq!(p.get_item(a), Some(&b"abc"[..]));
        assert_eq!(p.get_item(b), Some(&[7u8; 100][..]));
        assert_eq!(p.get_item(SlotId(2)), None);
        assert_eq!(p.upper() % ITEM_ALIGN, 0);
        assert_eq!(p.upper(), PAGE_SIZE - 8 - 104);
        SlottedPage::open(p.bytes()).unwrap();
    }

    #[test]
    fn rejects_bad_item_sizes() {
        let mut p = empty(0);
        assert!(matches!(
            p.add_item(&[]),
            Err(StorageError::InvalidItemSize { len: 0 })
        ));
        assert!(p.add_item(&vec![0u8; MAX_ITEM_SIZE + 1]).is_err());
        assert!(p.add_item(&vec![1u8; MAX_ITEM_SIZE]).unwrap().is_some());
        assert_eq!(p.add_item(b"x").unwrap(), None);
    }

    #[test]
    fn fills_until_full() {
        let mut p = empty(0);
        let mut n = 0;
        while p.add_item(&[n as u8; 64]).unwrap().is_some() {
            n += 1;
        }
        // Each item costs 64 data + 4 line pointer bytes.
        assert_eq!(n, (PAGE_SIZE - PAGE_HEADER_SIZE) / 68);
        for i in 0..n {
            assert_eq!(p.get_item(SlotId(i as u16)), Some(&[i as u8; 64][..]));
        }
    }

    #[test]
    fn state_transitions() {
        let mut p = empty(0);
        let s = p.add_item(b"row").unwrap().unwrap();
        assert!(matches!(
            p.mark_unused(s),
            Err(StorageError::InvalidSlotState { .. })
        ));
        p.mark_dead(s).unwrap();
        assert_eq!(p.get_item(s), None);
        assert_eq!(p.item_id(s).unwrap().state, ItemState::Dead);
        assert!(p.mark_dead(s).is_err());
        p.mark_unused(s).unwrap();
        assert_eq!(p.item_id(s).unwrap(), ItemId::UNUSED);
        assert!(matches!(
            p.mark_dead(SlotId(9)),
            Err(StorageError::InvalidSlot { slot: 9 })
        ));
    }

    #[test]
    fn unused_slot_is_reused() {
        let mut p = empty(0);
        let a = p.add_item(b"a").unwrap().unwrap();
        p.add_item(b"b").unwrap().unwrap();
        p.mark_dead(a).unwrap();
        p.mark_unused(a).unwrap();
        let lower = p.lower();
        assert_eq!(p.add_item(b"c").unwrap(), Some(a));
        assert_eq!(p.lower(), lower);
    }

    #[test]
    fn compact_preserves_slots_and_reclaims_space() {
        let mut p = empty(8);
        p.special_mut().copy_from_slice(b"SPECIAL!");
        let slots: Vec<_> = (0..5u8)
            .map(|i| p.add_item(&[i; 30]).unwrap().unwrap())
            .collect();
        p.mark_dead(slots[1]).unwrap();
        p.mark_dead(slots[3]).unwrap();
        p.mark_dead(slots[4]).unwrap();
        p.mark_unused(slots[4]).unwrap();
        p.compact();

        assert_eq!(p.slot_count(), 4); // trailing unused slot 4 truncated
        assert_eq!(p.get_item(slots[0]), Some(&[0u8; 30][..]));
        assert_eq!(p.get_item(slots[2]), Some(&[2u8; 30][..]));
        assert_eq!(p.item_id(slots[1]).unwrap().state, ItemState::Dead);
        assert!(!p.item_id(slots[1]).unwrap().has_storage());
        assert_eq!(p.upper(), PAGE_SIZE - 8 - 2 * 32);
        assert_eq!(p.special(), b"SPECIAL!");
        assert!(p.bytes()[p.lower()..p.upper()].iter().all(|&b| b == 0));
        SlottedPage::open(p.bytes()).unwrap();
    }

    #[test]
    fn open_rejects_corrupt_line_pointers() {
        let mut p = empty(0);
        p.add_item(b"abc").unwrap().unwrap();
        let raw = p.raw_lp(0);

        let mut bad = p.bytes().to_vec();
        let reserved = (raw & !(0b11 << LP_STATE_SHIFT)) | (2 << LP_STATE_SHIFT);
        bad[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4].copy_from_slice(&reserved.to_le_bytes());
        assert!(SlottedPage::open(&bad[..]).is_err());

        let mut bad = p.bytes().to_vec();
        let oob = (raw & !LP_OFF_MASK) | 16; // points into the header area
        bad[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4].copy_from_slice(&oob.to_le_bytes());
        assert!(SlottedPage::open(&bad[..]).is_err());

        let mut bad = p.bytes().to_vec();
        bad[off::LOWER..off::LOWER + 2]
            .copy_from_slice(&((PAGE_HEADER_SIZE + 2) as u16).to_le_bytes());
        assert!(SlottedPage::open(&bad[..]).is_err());
    }

    #[test]
    fn ordered_insert_and_remove() {
        let mut p = empty(0);
        assert!(p.insert_item_at(SlotId(0), b"b").unwrap());
        assert!(p.insert_item_at(SlotId(0), b"a").unwrap());
        assert!(p.insert_item_at(SlotId(2), b"d").unwrap());
        assert!(p.insert_item_at(SlotId(2), b"c").unwrap());
        let items = |p: &SlottedPage<Vec<u8>>| -> Vec<Vec<u8>> {
            (0..p.slot_count())
                .map(|s| p.get_item(SlotId(s)).unwrap().to_vec())
                .collect()
        };
        assert_eq!(items(&p), [b"a", b"b", b"c", b"d"]);
        assert!(matches!(
            p.insert_item_at(SlotId(9), b"x"),
            Err(StorageError::InvalidSlot { slot: 9 })
        ));

        p.remove_item_at(SlotId(1)).unwrap();
        assert_eq!(items(&p), [b"a", b"c", b"d"]);
        assert_eq!(p.upper(), PAGE_SIZE - 3 * ITEM_ALIGN, "gap closed");
        assert!(p.bytes()[p.lower()..p.upper()].iter().all(|&b| b == 0));
        assert!(p.remove_item_at(SlotId(3)).is_err());
        SlottedPage::open(p.bytes()).unwrap();
    }

    #[test]
    fn ordered_insert_reports_full_page() {
        let mut p = empty(0);
        let mut n = 0u16;
        while p.insert_item_at(SlotId(0), &[n as u8; 100]).unwrap() {
            n += 1;
        }
        assert_eq!(usize::from(n), (PAGE_SIZE - PAGE_HEADER_SIZE) / 108);
        // Newest first: slot 0 holds the last inserted item.
        assert_eq!(p.get_item(SlotId(0)), Some(&[(n - 1) as u8; 100][..]));
    }

    #[test]
    fn line_pointer_roundtrip() {
        for id in [
            ItemId::UNUSED,
            ItemId {
                off: 0x7ff8,
                len: 0x7fff,
                state: ItemState::Normal,
            },
            ItemId {
                off: 8,
                len: 1,
                state: ItemState::Dead,
            },
        ] {
            assert_eq!(ItemId::decode(id.encode()), Some(id));
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Add(Vec<u8>),
        Dead(usize),
        Unused(usize),
        Compact,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Model {
        Normal(Vec<u8>),
        Dead,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => prop::collection::vec(any::<u8>(), 1..400).prop_map(Op::Add),
            2 => any::<usize>().prop_map(Op::Dead),
            2 => any::<usize>().prop_map(Op::Unused),
            1 => Just(Op::Compact),
        ]
    }

    fn nth_key(m: &BTreeMap<u16, Model>, i: usize) -> Option<u16> {
        if m.is_empty() {
            None
        } else {
            m.keys().nth(i % m.len()).copied()
        }
    }

    #[derive(Debug, Clone)]
    enum OrderedOp {
        InsertAt(usize, Vec<u8>),
        RemoveAt(usize),
    }

    fn ordered_op() -> impl Strategy<Value = OrderedOp> {
        prop_oneof![
            3 => (any::<usize>(), prop::collection::vec(any::<u8>(), 1..300))
                .prop_map(|(i, d)| OrderedOp::InsertAt(i, d)),
            1 => any::<usize>().prop_map(OrderedOp::RemoveAt),
        ]
    }

    proptest! {
        #[test]
        fn ordered_ops_match_model(ops in prop::collection::vec(ordered_op(), 1..200),
                                   special_units in 0usize..3) {
            let special = special_units * SPECIAL_ALIGN;
            let mut p = empty(special);
            p.special_mut().fill(0xAB);
            let mut model: Vec<Vec<u8>> = Vec::new();
            for op in ops {
                match op {
                    OrderedOp::InsertAt(i, d) => {
                        let pos = i % (model.len() + 1);
                        let fits = p.upper() - p.lower()
                            >= d.len().next_multiple_of(ITEM_ALIGN) + LINE_POINTER_SIZE;
                        prop_assert_eq!(p.insert_item_at(SlotId(pos as u16), &d).unwrap(), fits);
                        if fits {
                            model.insert(pos, d);
                        }
                    }
                    OrderedOp::RemoveAt(i) => {
                        if model.is_empty() {
                            prop_assert!(p.remove_item_at(SlotId(0)).is_err());
                        } else {
                            let pos = i % model.len();
                            p.remove_item_at(SlotId(pos as u16)).unwrap();
                            model.remove(pos);
                        }
                    }
                }
                prop_assert!(SlottedPage::open(p.bytes()).is_ok());
                prop_assert_eq!(usize::from(p.slot_count()), model.len());
                for (s, d) in model.iter().enumerate() {
                    prop_assert_eq!(p.get_item(SlotId(s as u16)), Some(&d[..]));
                }
                // No fragmentation: data is always contiguous below special.
                let used: usize = model.iter().map(|d| d.len().next_multiple_of(ITEM_ALIGN)).sum();
                prop_assert_eq!(p.upper(), PAGE_SIZE - special - used);
                prop_assert!(p.special().iter().all(|&b| b == 0xAB));
            }
        }

        #[test]
        fn matches_model(ops in prop::collection::vec(op(), 1..200),
                         special_units in 0usize..4) {
            let special = special_units * SPECIAL_ALIGN;
            let mut p = empty(special);
            p.special_mut().fill(0xEE);
            let mut model: BTreeMap<u16, Model> = BTreeMap::new();

            for op in ops {
                match op {
                    Op::Add(data) => {
                        let has_unused = (p.slot_count() as usize) > model.len();
                        let need = data.len().next_multiple_of(ITEM_ALIGN)
                            + if has_unused { 0 } else { LINE_POINTER_SIZE };
                        let gap = p.upper() - p.lower();
                        match p.add_item(&data).unwrap() {
                            Some(s) => {
                                prop_assert!(need <= gap);
                                prop_assert!(!model.contains_key(&s.0));
                                model.insert(s.0, Model::Normal(data));
                            }
                            None => prop_assert!(need > gap),
                        }
                    }
                    Op::Dead(i) => if let Some(k) = nth_key(&model, i) {
                        let r = p.mark_dead(SlotId(k));
                        if model[&k] == Model::Dead {
                            prop_assert!(r.is_err());
                        } else {
                            prop_assert!(r.is_ok());
                            model.insert(k, Model::Dead);
                        }
                    },
                    Op::Unused(i) => if let Some(k) = nth_key(&model, i) {
                        let r = p.mark_unused(SlotId(k));
                        if model[&k] == Model::Dead {
                            prop_assert!(r.is_ok());
                            model.remove(&k);
                        } else {
                            prop_assert!(r.is_err());
                        }
                    },
                    Op::Compact => {
                        p.compact();
                        let used: usize = model.values().map(|m| match m {
                            Model::Normal(d) => d.len().next_multiple_of(ITEM_ALIGN),
                            Model::Dead => 0,
                        }).sum();
                        prop_assert_eq!(p.upper(), PAGE_SIZE - special - used);
                        let expected_count = model.keys().next_back().map_or(0, |k| k + 1);
                        prop_assert_eq!(p.slot_count(), expected_count);
                    }
                }

                // Invariants hold and contents match the model after every op.
                let reopened = SlottedPage::open(p.bytes());
                prop_assert!(reopened.is_ok());
                prop_assert!(p.special().iter().all(|&b| b == 0xEE));
                for s in 0..p.slot_count() {
                    let id = p.item_id(SlotId(s)).unwrap();
                    match model.get(&s) {
                        Some(Model::Normal(d)) => {
                            prop_assert_eq!(p.get_item(SlotId(s)), Some(&d[..]));
                        }
                        Some(Model::Dead) => prop_assert_eq!(id.state, ItemState::Dead),
                        None => prop_assert_eq!(id, ItemId::UNUSED),
                    }
                }
                prop_assert!(model.keys().all(|&k| k < p.slot_count()));
            }
        }
    }
}
