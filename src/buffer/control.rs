//! Flush gate: decides whether a dirty page may be written now, and supplies
//! the fencing token for the write.
//!
//! In ha mode the writer may only write a page whose LSN every reader has
//! already replayed (PolarDB-style flush constraint), and every write must
//! carry the current epoch. Both concerns gate writer I/O, so they share one
//! trait.

use std::fmt;

use super::pool::BufferTag;
use crate::storage::backend::{FenceSource, FenceToken};
use crate::storage::page::Lsn;

/// Policy consulted on every page flush; also supplies the fencing token
/// passed to the storage backend.
pub trait FlushControl: FenceSource + fmt::Debug {
    /// Whether the page may be written now. A `false` leaves it dirty; the
    /// replacement sweep then skips it.
    fn may_flush(&self, tag: BufferTag, page_lsn: Lsn) -> bool;
}

/// Single install mode: flushes are always allowed, the token is fixed.
#[derive(Debug, Clone, Copy)]
pub struct SingleModeFlush {
    fence: FenceToken,
}

impl SingleModeFlush {
    pub const fn new(fence: FenceToken) -> Self {
        Self { fence }
    }
}

impl FlushControl for SingleModeFlush {
    fn may_flush(&self, _tag: BufferTag, _page_lsn: Lsn) -> bool {
        true
    }
}

impl FenceSource for SingleModeFlush {
    fn fence_token(&self) -> FenceToken {
        self.fence
    }
}
