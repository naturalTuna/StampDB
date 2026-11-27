//! Transaction identifiers. Allocation, snapshots, and commit status come
//! in the transaction step; for now these are plain identifiers.

/// 64-bit transaction ID (no wraparound, no freezing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Xid(pub u64);

impl Xid {
    /// "No transaction" (e.g. a tuple's xmax when it was never deleted).
    pub const INVALID: Xid = Xid(0);

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }
}

/// Command number within a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CommandId(pub u32);
