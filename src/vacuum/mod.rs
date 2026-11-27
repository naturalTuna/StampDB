//! Manual VACUUM of one table and its B+Tree indexes (DESIGN r8; the
//! steps of PostgreSQL's lazy vacuum, in one pass):
//!
//! 1. Take ShareUpdateExclusive on the table (DML continues meanwhile).
//! 2. Compute the horizon: the oldest xmin any snapshot may hold.
//! 3. Prune every heap page: tuples dead to everyone become DEAD line
//!    pointers and the page is compacted; collect the dead TIDs.
//! 4. Remove index entries pointing at those TIDs.
//! 5. Mark the DEAD line pointers unused (now unreferenced) and record the
//!    free space in the free space map.
//! 6. Delete index leaf pages left empty.
//!
//! Unlike PostgreSQL, which processes dead TIDs in batches bounded by
//! maintenance_work_mem, all dead TIDs of a run are kept in memory.

use std::collections::{BTreeMap, HashSet};

use crate::access::btree::{BTree, BTreeError};
use crate::access::heap::{HeapError, HeapFile};
use crate::access::tuple::Tid;
use crate::lock::LockMode;
use crate::storage::page::BlockNumber;
use crate::storage::slotted::SlotId;
use crate::txn::{Transaction, TransactionError};

/// Errors from vacuum.
#[derive(Debug, thiserror::Error)]
pub enum VacuumError {
    #[error(transparent)]
    Heap(#[from] HeapError),

    #[error(transparent)]
    BTree(#[from] BTreeError),

    #[error(transparent)]
    Transaction(#[from] TransactionError),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, VacuumError>;

/// What a vacuum run did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VacuumStats {
    pub heap_pages: u32,
    /// Heap line pointers freed for reuse.
    pub tuples_removed: usize,
    pub index_entries_removed: usize,
    pub index_pages_deleted: usize,
}

/// Vacuums `heap` and its `indexes` within `txn` (which gets the table
/// lock and should be committed by the caller afterwards).
pub fn vacuum(heap: &HeapFile, indexes: &[&BTree], txn: &mut Transaction) -> Result<VacuumStats> {
    txn.lock_relation(heap.rel(), LockMode::ShareUpdateExclusive)?;
    let tm = txn.tm();
    let horizon = tm.oldest_xmin();
    let mut stats = VacuumStats {
        heap_pages: heap.nblocks()?,
        ..VacuumStats::default()
    };

    let mut dead: BTreeMap<BlockNumber, Vec<SlotId>> = BTreeMap::new();
    for blk in 0..stats.heap_pages {
        for t in heap.prune_page(BlockNumber(blk), horizon, tm)? {
            dead.entry(t.blk).or_default().push(t.slot);
        }
    }

    if !dead.is_empty() {
        let set: HashSet<Tid> = dead
            .iter()
            .flat_map(|(blk, slots)| slots.iter().map(|&slot| Tid { blk: *blk, slot }))
            .collect();
        for idx in indexes {
            stats.index_entries_removed += idx.bulk_delete(&|t| set.contains(&t))?;
        }
        for (blk, slots) in &dead {
            stats.tuples_removed += heap.vacuum_page(*blk, slots)?;
        }
    }

    for idx in indexes {
        stats.index_pages_deleted += idx.delete_empty_pages()?;
    }
    Ok(stats)
}
