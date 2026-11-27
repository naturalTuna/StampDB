//! Access methods: heap tuples and files, B+Tree indexes.

pub mod btree;
pub mod heap;
pub mod tuple;

use crate::wal::record::{Record, RmgrId};

/// Errors from applying a WAL record during recovery.
#[derive(Debug, thiserror::Error)]
pub enum RedoError {
    #[error(transparent)]
    Heap(#[from] heap::HeapError),

    #[error(transparent)]
    BTree(#[from] btree::BTreeError),

    #[error(transparent)]
    Clog(#[from] crate::txn::clog::ClogError),

    #[error("record of rmgr {0:?} has no page redo")]
    NoPageRedo(RmgrId),
}

/// Applies block `idx` of `rec` (which carries no image for it) to `page`,
/// dispatching on the resource manager.
pub fn redo_block(rec: &Record, idx: usize, page: &mut [u8]) -> Result<(), RedoError> {
    let block = rec.blocks.get(idx).ok_or(RedoError::NoPageRedo(rec.rmgr))?;
    match rec.rmgr {
        RmgrId::Heap => Ok(heap::redo(rec.info, idx, block, page)?),
        RmgrId::Btree => Ok(btree::redo(rec, idx, page)?),
        RmgrId::Xact => Ok(crate::txn::clog::redo(rec, page)?),
        RmgrId::Xlog | RmgrId::Smgr => Err(RedoError::NoPageRedo(rec.rmgr)),
    }
}
