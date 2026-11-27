//! Local transaction manager: running set + commit log + WAL.

use std::sync::Arc;

use super::clog::Clog;
use super::{ProcArray, Result, Snapshot, TransactionManager, TxnError, TxnId, Xid, XidStatus};
use crate::buffer::BufferPool;
use crate::storage::backend::RelFileId;

/// Single-node implementation of [`TransactionManager`].
#[derive(Debug)]
pub struct LocalTxnManager {
    procs: ProcArray,
    clog: Clog,
    pool: Arc<BufferPool>,
}

impl LocalTxnManager {
    /// Starts handing out xids from `next_xid`, with the commit log in
    /// `pool` (which should have the WAL attached).
    pub fn new(pool: Arc<BufferPool>, next_xid: Xid) -> Result<Self> {
        Ok(Self {
            procs: ProcArray::new(next_xid),
            clog: Clog::open(pool.clone()).map_err(clog_err)?,
            pool,
        })
    }

    /// The next xid to be assigned (recorded at checkpoints).
    pub fn next_xid(&self) -> Xid {
        self.procs.next_xid()
    }

    pub fn is_running(&self, xid: Xid) -> bool {
        self.procs.is_running(xid)
    }

    fn finish(&self, xid: Xid, committed: bool, drop: &[RelFileId]) -> Result<()> {
        if !self.procs.is_running(xid) {
            return Err(TxnError::NotRunning(xid));
        }
        let end = self
            .clog
            .record_outcome(xid, committed, drop)
            .map_err(clog_err)?;
        if committed || !drop.is_empty() {
            // A commit is durable before anyone can see it as committed;
            // files are deleted only once the record saying so is durable.
            if let Some(wal) = self.pool.wal() {
                wal.flush_to(end);
            }
        }
        self.procs.remove(xid);
        for &rel in drop {
            self.pool
                .remove_relation(rel)
                .map_err(|e| TxnError::Clog(format!("deleting relation {}: {e}", rel.0)))?;
        }
        Ok(())
    }
}

fn clog_err(e: super::clog::ClogError) -> TxnError {
    TxnError::Clog(e.to_string())
}

impl TransactionManager for LocalTxnManager {
    fn begin(&self) -> TxnId {
        self.procs.new_txn_id()
    }

    fn assign_xid(&self) -> Result<Xid> {
        let xid = self.procs.assign_xid();
        if let Err(e) = self.clog.ensure(xid) {
            self.procs.remove(xid);
            return Err(clog_err(e));
        }
        Ok(xid)
    }

    fn snapshot(&self) -> Snapshot {
        self.procs.snapshot()
    }

    fn snapshot_for(&self, txn: TxnId) -> Snapshot {
        self.procs.snapshot_for(txn)
    }

    fn release_snapshot(&self, txn: TxnId) {
        self.procs.release_snapshot(txn)
    }

    fn oldest_xmin(&self) -> Xid {
        self.procs.oldest_xmin()
    }

    fn status(&self, xid: Xid) -> Result<XidStatus> {
        if xid == Xid::BOOTSTRAP {
            return Ok(XidStatus::Committed);
        }
        // Running first: a committing transaction's bits may already be set
        // before its commit record is durable.
        if self.procs.is_running(xid) {
            return Ok(XidStatus::InProgress);
        }
        match self.clog.get(xid).map_err(clog_err)? {
            // Not running, never finished, assigned in the past: it was
            // running when the server crashed.
            XidStatus::InProgress if xid < self.procs.next_xid() => Ok(XidStatus::Aborted),
            s => Ok(s),
        }
    }

    fn commit_dropping(&self, xid: Xid, drop: &[RelFileId]) -> Result<()> {
        self.finish(xid, true, drop)
    }

    fn abort_dropping(&self, xid: Xid, drop: &[RelFileId]) -> Result<()> {
        self.finish(xid, false, drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::SingleModeFlush;
    use crate::storage::backend::{FenceToken, FileKey, FixedFence};
    use crate::storage::mem::MemBackend;
    use crate::storage::page::{BlockNumber, PAGE_SIZE};
    use crate::txn::clog::{CLOG_REL, XIDS_PER_PAGE};
    use crate::wal::{Engine, WalConfig, initdb};

    const FENCE: FenceToken = FenceToken::new(1);

    fn cfg() -> WalConfig {
        WalConfig {
            timeline: 1,
            segment_size: 256 * 1024,
        }
    }

    fn fresh() -> Arc<MemBackend> {
        let mem = Arc::new(MemBackend::default());
        initdb(mem.clone(), Arc::new(FixedFence(FENCE)), cfg()).unwrap();
        mem
    }

    fn start(mem: &Arc<MemBackend>) -> Engine {
        Engine::start(
            mem.clone(),
            Arc::new(SingleModeFlush::new(FENCE)),
            cfg(),
            32,
        )
        .unwrap()
    }

    #[test]
    fn commit_and_abort() {
        let mem = fresh();
        let e = start(&mem);
        let tm = e.txn();
        let a = tm.assign_xid().unwrap();
        let b = tm.assign_xid().unwrap();
        assert_eq!(a, Xid::FIRST);
        assert_eq!(tm.status(a).unwrap(), XidStatus::InProgress);
        let flushed = e.wal().flushed_lsn();
        tm.commit(a).unwrap();
        assert!(e.wal().flushed_lsn() > flushed, "commit flushes WAL");
        tm.abort(b).unwrap();
        assert_eq!(tm.status(a).unwrap(), XidStatus::Committed);
        assert_eq!(tm.status(b).unwrap(), XidStatus::Aborted);
        assert!(matches!(tm.commit(a), Err(TxnError::NotRunning(_))));
        assert_eq!(
            tm.status(Xid(999)).unwrap(),
            XidStatus::InProgress,
            "future xid"
        );
        let s = tm.snapshot();
        assert!(!s.is_running(a) && !s.is_running(b));
    }

    #[test]
    fn running_transaction_is_in_progress_whatever_the_bits() {
        let mem = fresh();
        let e = start(&mem);
        let tm = e.txn();
        let x = tm.assign_xid().unwrap();
        // Bits written but the transaction still registered as running:
        // the state between setting the bits and the end of commit.
        tm.clog.record_outcome(x, true, &[]).unwrap();
        assert_eq!(tm.status(x).unwrap(), XidStatus::InProgress);
        tm.procs.remove(x);
        assert_eq!(tm.status(x).unwrap(), XidStatus::Committed);
    }

    #[test]
    fn xids_across_clog_pages() {
        let mem = fresh();
        let e = start(&mem);
        let tm = LocalTxnManager::new(e.pool().clone(), Xid(XIDS_PER_PAGE - 1)).unwrap();
        let a = tm.assign_xid().unwrap();
        let b = tm.assign_xid().unwrap();
        assert_eq!(b, Xid(XIDS_PER_PAGE));
        tm.commit(b).unwrap();
        tm.abort(a).unwrap();
        assert_eq!(tm.status(a).unwrap(), XidStatus::Aborted);
        assert_eq!(tm.status(b).unwrap(), XidStatus::Committed);
        assert_eq!(e.pool().nblocks(CLOG_REL).unwrap(), 2);
    }

    #[test]
    fn outcomes_survive_crash_and_running_becomes_aborted() {
        use crate::wal::record::{Record, RmgrId};
        let mem = fresh();
        let (c, a, r, unlogged) = {
            let e = start(&mem);
            let tm = e.txn();
            let c = tm.assign_xid().unwrap();
            let a = tm.assign_xid().unwrap();
            let r = tm.assign_xid().unwrap();
            // r writes something (a replayable record carrying its xid)...
            let mut rec = Record::new(RmgrId::Smgr, crate::wal::recovery::smgr_info::CREATE);
            rec.xid = r;
            rec.main = 50_000u32.to_le_bytes().to_vec();
            e.wal().insert(&rec).unwrap();
            // ...while `unlogged` never writes anything.
            let unlogged = tm.assign_xid().unwrap();
            tm.abort(a).unwrap();
            tm.commit(c).unwrap(); // flushes everything logged before it
            (c, a, r, unlogged) // r and `unlogged` are running at the crash
        };
        let e = start(&mem);
        let tm = e.txn();
        assert_eq!(tm.status(c).unwrap(), XidStatus::Committed);
        assert_eq!(tm.status(a).unwrap(), XidStatus::Aborted);
        assert_eq!(
            tm.status(r).unwrap(),
            XidStatus::Aborted,
            "crashed while running"
        );
        assert!(tm.next_xid() > r, "an xid that wrote WAL is never reused");
        // An xid that wrote nothing may be handed out again: harmless, as
        // no tuple carries it.
        assert_eq!(tm.next_xid(), unlogged);
        assert_eq!(tm.status(unlogged).unwrap(), XidStatus::InProgress);
    }

    #[test]
    fn torn_clog_page_is_repaired() {
        let mem = fresh();
        let x = {
            let e = start(&mem);
            let tm = e.txn();
            let first = tm.assign_xid().unwrap();
            tm.commit(first).unwrap();
            e.checkpoint().unwrap();
            let x = tm.assign_xid().unwrap();
            tm.commit(x).unwrap(); // first CLOG change after the checkpoint
            e.pool().flush_all().unwrap();
            x
        };
        mem.overwrite(
            FileKey::Relation(CLOG_REL),
            BlockNumber(0).byte_offset() as usize + PAGE_SIZE / 2,
            &[0x55; PAGE_SIZE / 2],
        );
        let e = start(&mem);
        assert_eq!(e.txn().status(x).unwrap(), XidStatus::Committed);
    }

    #[test]
    fn concurrent_commits() {
        let mem = fresh();
        let e = start(&mem);
        let tm = e.txn();
        let xids: Vec<Xid> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..4)
                .map(|_| {
                    s.spawn(|| {
                        (0..100)
                            .map(|i| {
                                let x = tm.assign_xid().unwrap();
                                if i % 3 == 0 {
                                    tm.abort(x).unwrap();
                                } else {
                                    tm.commit(x).unwrap();
                                }
                                x
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(xids.len(), 400);
        assert!(tm.snapshot().running.is_empty());
        let committed = xids
            .iter()
            .filter(|&&x| tm.status(x).unwrap() == XidStatus::Committed)
            .count();
        assert_eq!(committed, 4 * 66);
    }
}
