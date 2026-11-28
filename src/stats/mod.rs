//! Per-table activity statistics (DESIGN r14; PostgreSQL's cumulative
//! statistics for tables, as shown by pg_stat_user_tables).
//!
//! Counters live in memory only and start empty after a restart. A
//! transaction accumulates its own insert/update/delete counts per table
//! ([`XactCounts`]) and applies them when it ends, following pgstat's rules:
//!
//! - the operation counts (`n_tup_*`) always grow, even on abort;
//! - on commit, inserted rows become live and deleted rows (and the old
//!   versions of updated ones) dead;
//! - on abort, inserted rows and new versions of updated rows are dead.
//!
//! Vacuum overwrites the live and dead tuple counts with what it found.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::types::TimestampMicros;

/// Microseconds from 1970-01-01 to 2000-01-01 (the timestamp epoch).
const UNIX_TO_2000_USECS: i64 = 10_957 * 86_400_000_000;

/// One transaction's changes to one table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelCounts {
    pub inserted: u64,
    pub updated: u64,
    pub deleted: u64,
}

/// A transaction's changes, by table oid.
pub type XactCounts = HashMap<u32, RelCounts>;

/// Statistics of one table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableStats {
    pub n_tup_ins: u64,
    pub n_tup_upd: u64,
    pub n_tup_del: u64,
    /// Estimated live tuples.
    pub n_live_tup: u64,
    /// Estimated dead tuples (what autovacuum looks at).
    pub n_dead_tup: u64,
    pub last_vacuum: Option<TimestampMicros>,
    pub last_autovacuum: Option<TimestampMicros>,
    pub vacuum_count: u64,
    pub autovacuum_count: u64,
}

/// What a vacuum of one table found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumReport {
    /// Tuples left visible to someone (including ones being deleted).
    pub live: u64,
    /// Dead tuples that could not be removed yet.
    pub dead: u64,
    /// Run by autovacuum rather than a VACUUM command.
    pub auto: bool,
}

/// Statistics of all tables.
#[derive(Debug, Default)]
pub struct StatsCollector {
    tables: Mutex<HashMap<u32, TableStats>>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies the counts of a transaction that committed or aborted.
    pub fn apply(&self, counts: &XactCounts, committed: bool) {
        if counts.is_empty() {
            return;
        }
        let mut tables = self.tables.lock();
        for (&oid, c) in counts {
            let s = tables.entry(oid).or_default();
            s.n_tup_ins += c.inserted;
            s.n_tup_upd += c.updated;
            s.n_tup_del += c.deleted;
            if committed {
                s.n_live_tup = (s.n_live_tup + c.inserted).saturating_sub(c.deleted);
                s.n_dead_tup += c.updated + c.deleted;
            } else {
                s.n_dead_tup += c.inserted + c.updated;
            }
        }
    }

    /// Records a finished vacuum of table `oid`.
    pub fn report_vacuum(&self, oid: u32, r: VacuumReport) {
        let now = now_micros();
        let mut tables = self.tables.lock();
        let s = tables.entry(oid).or_default();
        s.n_live_tup = r.live;
        s.n_dead_tup = r.dead;
        if r.auto {
            s.last_autovacuum = Some(now);
            s.autovacuum_count += 1;
        } else {
            s.last_vacuum = Some(now);
            s.vacuum_count += 1;
        }
    }

    /// Statistics of table `oid` (zero if nothing was recorded).
    pub fn get(&self, oid: u32) -> TableStats {
        self.tables.lock().get(&oid).copied().unwrap_or_default()
    }

    /// All tables with statistics, by oid.
    pub fn all(&self) -> Vec<(u32, TableStats)> {
        let mut v: Vec<_> = self.tables.lock().iter().map(|(&o, &s)| (o, s)).collect();
        v.sort_by_key(|&(o, _)| o);
        v
    }

    /// Forgets table `oid` (dropped).
    pub fn remove(&self, oid: u32) {
        self.tables.lock().remove(&oid);
    }
}

/// The current time as a timestamp (microseconds since 2000-01-01 UTC).
pub fn now_micros() -> TimestampMicros {
    let since_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
    since_unix - UNIX_TO_2000_USECS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(oid: u32, inserted: u64, updated: u64, deleted: u64) -> XactCounts {
        XactCounts::from([(
            oid,
            RelCounts {
                inserted,
                updated,
                deleted,
            },
        )])
    }

    #[test]
    fn commit_and_abort_follow_pgstat_rules() {
        let s = StatsCollector::new();
        s.apply(&counts(10, 5, 0, 0), true);
        s.apply(&counts(10, 0, 2, 1), true);
        s.apply(&counts(10, 3, 1, 1), false);
        let t = s.get(10);
        assert_eq!((t.n_tup_ins, t.n_tup_upd, t.n_tup_del), (8, 3, 2));
        // Live: 5 inserted - 1 deleted; the aborted delete changed nothing.
        assert_eq!(t.n_live_tup, 4);
        // Dead: 2 old versions + 1 deleted, then 3 aborted inserts + 1
        // aborted update's new version.
        assert_eq!(t.n_dead_tup, 7);
        assert_eq!(s.get(11), TableStats::default());
    }

    #[test]
    fn vacuum_overwrites_tuple_counts() {
        let s = StatsCollector::new();
        s.apply(&counts(10, 10, 0, 4), true);
        s.report_vacuum(
            10,
            VacuumReport {
                live: 6,
                dead: 1,
                auto: true,
            },
        );
        let t = s.get(10);
        assert_eq!((t.n_live_tup, t.n_dead_tup), (6, 1));
        assert_eq!((t.vacuum_count, t.autovacuum_count), (0, 1));
        assert!(t.last_autovacuum.is_some() && t.last_vacuum.is_none());
        s.remove(10);
        assert!(s.all().is_empty());
    }

    #[test]
    fn now_is_after_2020() {
        // 2020-01-01 is 20 years (7305 days) after the timestamp epoch.
        assert!(now_micros() > 7_305 * 86_400_000_000);
    }
}
