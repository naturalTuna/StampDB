//! Table lock modes and their conflicts (same as PostgreSQL).

/// Lock modes, weakest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LockMode {
    /// SELECT.
    AccessShare,
    /// SELECT FOR UPDATE / FOR SHARE.
    RowShare,
    /// INSERT, UPDATE, DELETE.
    RowExclusive,
    /// VACUUM, ANALYZE, CREATE INDEX CONCURRENTLY.
    ShareUpdateExclusive,
    /// CREATE INDEX.
    Share,
    /// CREATE TRIGGER, some ALTER TABLE forms.
    ShareRowExclusive,
    /// REFRESH MATERIALIZED VIEW CONCURRENTLY.
    Exclusive,
    /// DROP TABLE, TRUNCATE, most ALTER TABLE forms.
    AccessExclusive,
}

use LockMode::*;

impl LockMode {
    pub const COUNT: usize = 8;

    pub const ALL: [LockMode; Self::COUNT] = [
        AccessShare,
        RowShare,
        RowExclusive,
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ];

    pub fn index(self) -> usize {
        self as usize
    }

    /// Whether two modes conflict (PostgreSQL's conflict table, symmetric).
    pub fn conflicts(self, other: LockMode) -> bool {
        CONFLICTS[self.index()] & (1 << other.index()) != 0
    }
}

const fn bits(modes: &[LockMode]) -> u8 {
    let mut b = 0u8;
    let mut i = 0;
    while i < modes.len() {
        b |= 1 << (modes[i] as u8);
        i += 1;
    }
    b
}

/// Row `m`: the modes that conflict with `m`.
const CONFLICTS: [u8; LockMode::COUNT] = [
    bits(&[AccessExclusive]),
    bits(&[Exclusive, AccessExclusive]),
    bits(&[Share, ShareRowExclusive, Exclusive, AccessExclusive]),
    bits(&[
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ]),
    bits(&[
        RowExclusive,
        ShareUpdateExclusive,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ]),
    bits(&[
        RowExclusive,
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ]),
    bits(&[
        RowShare,
        RowExclusive,
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ]),
    bits(&[
        AccessShare,
        RowShare,
        RowExclusive,
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ]),
];
