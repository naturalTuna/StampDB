//! B+Tree index (Lehman-Yao: right-links and high keys, no merge).
//!
//! Keys are memcomparable byte strings (see [`key`]); every entry is
//! `key || tid`, so entries are unique and the tree never deals with
//! duplicates. Pages use the slotted layout with ordered line pointers
//! (see [`page`]).

pub mod key;
pub mod page;
mod tree;

pub use tree::{BTree, BTreeScan, InsertOutcome, UniqueCheck, info, redo};

use crate::buffer::BufferError;
use crate::storage::StorageError;

/// Errors from B+Tree operations.
#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Key(#[from] key::KeyError),

    #[error("index row size {len} exceeds maximum {max}")]
    KeyTooLarge { len: usize, max: usize },

    #[error("index entry already exists")]
    Duplicate,

    #[error("relation is not a btree index")]
    NotABTree,

    #[error("corrupt btree: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, BTreeError>;
