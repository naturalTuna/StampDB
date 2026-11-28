//! Autovacuum (DESIGN r14): a background thread that wakes up every
//! `naptime` and vacuums the tables whose dead tuple estimate exceeds
//! `threshold + scale_factor * live tuples` (PostgreSQL's
//! autovacuum_vacuum_threshold and autovacuum_vacuum_scale_factor).
//!
//! PostgreSQL splits this into a launcher and up to
//! autovacuum_max_workers workers; here one thread does both, one table at
//! a time. A table whose lock is not immediately available is left for
//! the next round. Unlike PostgreSQL, a running autovacuum does not cancel
//! itself when a conflicting lock request (DDL) arrives.

use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use super::error::{Result, SqlError, code};
use super::session::Database;
use crate::lock::WaitPolicy;
use crate::stats::TableStats;
use crate::txn::{Isolation, Transaction, TransactionManager};

/// Autovacuum settings.
#[derive(Debug, Clone, Copy)]
pub struct AutovacuumConfig {
    /// Time between rounds (PostgreSQL's autovacuum_naptime).
    pub naptime: Duration,
    /// Dead tuples needed regardless of table size.
    pub threshold: u64,
    /// Fraction of live tuples added to the threshold.
    pub scale_factor: f64,
}

impl Default for AutovacuumConfig {
    fn default() -> Self {
        Self {
            naptime: Duration::from_secs(60),
            threshold: 50,
            scale_factor: 0.2,
        }
    }
}

impl AutovacuumConfig {
    /// Whether a table with statistics `s` is due for vacuum.
    pub fn needs_vacuum(&self, s: &TableStats) -> bool {
        s.n_dead_tup as f64 > self.threshold as f64 + self.scale_factor * s.n_live_tup as f64
    }
}

/// What one autovacuum round did, by table oid.
#[derive(Debug, Default)]
pub struct Round {
    pub vacuumed: Vec<u32>,
    /// Due, but the table lock was taken.
    pub skipped: Vec<u32>,
    pub errors: Vec<(u32, SqlError)>,
}

impl Database {
    /// Runs one autovacuum round: vacuums every table that is due.
    /// Statistics of tables that no longer exist are dropped.
    pub fn autovacuum_round(&self, cfg: &AutovacuumConfig) -> Round {
        let mut round = Round::default();
        for (oid, s) in self.stats().all() {
            if !cfg.needs_vacuum(&s) {
                continue;
            }
            let info = match self.lookup_table(oid) {
                Ok(Some(info)) => info,
                Ok(None) => {
                    self.stats().remove(oid);
                    continue;
                }
                Err(e) => {
                    round.errors.push((oid, e));
                    continue;
                }
            };
            match self.vacuum_table(&info, true) {
                Ok(Some(_)) => round.vacuumed.push(oid),
                Ok(None) => self.stats().remove(oid),
                Err(e) if e.code == code::LOCK_NOT_AVAILABLE => round.skipped.push(oid),
                Err(e) => round.errors.push((oid, e)),
            }
        }
        round
    }

    /// Table `oid` as the latest committed catalogs describe it.
    fn lookup_table(&self, oid: u32) -> Result<Option<crate::catalog::RelationInfo>> {
        let tm: Arc<dyn TransactionManager> = self.engine().txn().clone();
        let mut t = Transaction::begin(
            tm,
            self.lock_manager().clone(),
            Isolation::ReadCommitted,
            WaitPolicy::default(),
        );
        t.statement_snapshot();
        let r = self.catalog().lookup_oid(&mut t, oid);
        t.abort()?;
        Ok(r?)
    }
}

/// A running autovacuum thread; dropping it stops the thread (after the
/// table being vacuumed, if any).
#[derive(Debug)]
pub struct Autovacuum {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Autovacuum {
    /// Starts autovacuum for `db`. The thread holds only a weak reference
    /// and exits when the database is gone.
    pub fn start(db: &Arc<Database>, cfg: AutovacuumConfig) -> std::io::Result<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let weak: Weak<Database> = Arc::downgrade(db);
        let s = stop.clone();
        let thread = std::thread::Builder::new()
            .name("autovacuum".into())
            .spawn(move || run(&weak, &cfg, &s))?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Autovacuum {
    fn drop(&mut self) {
        let (flag, cv) = &*self.stop;
        *flag.lock() = true;
        cv.notify_all();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn run(db: &Weak<Database>, cfg: &AutovacuumConfig, stop: &(Mutex<bool>, Condvar)) {
    let (flag, cv) = stop;
    loop {
        {
            let mut stopped = flag.lock();
            if !*stopped {
                cv.wait_for(&mut stopped, cfg.naptime);
            }
            if *stopped {
                return;
            }
        }
        let Some(db) = db.upgrade() else {
            return;
        };
        for (oid, e) in db.autovacuum_round(cfg).errors {
            eprintln!("autovacuum: table {oid}: {e} ({})", e.code);
        }
    }
}
