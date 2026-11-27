//! In-memory free space map (DESIGN r8).
//!
//! Per relation, how many bytes each heap block can take for a new tuple.
//! Vacuum records the space it frees; inserts look here before extending
//! the relation. The map is advisory: it is neither persisted nor logged
//! (after a restart it is empty until the next vacuum), and a stale entry
//! only costs a failed attempt, which corrects it.

use std::collections::{BTreeMap, HashMap};

use parking_lot::Mutex;

use crate::storage::backend::RelFileId;

/// Free space per block, per relation.
#[derive(Debug, Default)]
pub struct FreeSpaceMap {
    rels: Mutex<HashMap<RelFileId, BTreeMap<u32, usize>>>,
}

impl FreeSpaceMap {
    /// Records that `blk` of `rel` can take `free` bytes (item + line
    /// pointer). Blocks with too little space to matter are dropped.
    pub fn record(&self, rel: RelFileId, blk: u32, free: usize) {
        let mut rels = self.rels.lock();
        let map = rels.entry(rel).or_default();
        if free >= MIN_USEFUL {
            map.insert(blk, free);
        } else {
            map.remove(&blk);
        }
    }

    /// The lowest block believed to have at least `need` bytes free.
    pub fn find(&self, rel: RelFileId, need: usize) -> Option<u32> {
        let rels = self.rels.lock();
        rels.get(&rel)?
            .iter()
            .find(|(_, free)| **free >= need)
            .map(|(blk, _)| *blk)
    }

    /// Forgets a dropped relation.
    pub fn forget(&self, rel: RelFileId) {
        self.rels.lock().remove(&rel);
    }
}

/// Below this, a block is not worth remembering.
const MIN_USEFUL: usize = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_find_forget() {
        let fsm = FreeSpaceMap::default();
        let r = RelFileId(5);
        assert_eq!(fsm.find(r, 10), None);
        fsm.record(r, 7, 500);
        fsm.record(r, 3, 200);
        fsm.record(r, 9, 10); // too small to keep
        assert_eq!(fsm.find(r, 100), Some(3));
        assert_eq!(fsm.find(r, 300), Some(7));
        assert_eq!(fsm.find(r, 600), None);
        fsm.record(r, 3, 0);
        assert_eq!(fsm.find(r, 100), Some(7));
        fsm.forget(r);
        assert_eq!(fsm.find(r, 100), None);
    }
}
