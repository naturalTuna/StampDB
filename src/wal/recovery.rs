//! Cluster creation, startup with crash recovery, and checkpoints.
//!
//! Recovery is redo only (DESIGN r6): starting at the redo point of the
//! latest checkpoint, every record is replayed. A block whose record carries
//! a full image (or rebuilds the page) is overwritten without reading the
//! page from disk, so a torn page is never read. Other blocks are redone
//! only if the page LSN is below the record's end. Afterwards all pages are
//! flushed and an end-of-recovery checkpoint is written.
//!
//! A checkpoint (fuzzy, as in PostgreSQL): take the current insert position
//! as the redo point, write every dirty page, log a CHECKPOINT record
//! carrying the redo point, flush the WAL, and record both in the control
//! file.

use std::sync::Arc;

use parking_lot::Mutex;

use super::control::{ControlData, ControlFile, DbState};
use super::log::{Wal, WalConfig, WalReader, xlog_info};
use super::record::{Record, RmgrId};
use super::{WalError, fatal};
use crate::access::{RedoError, redo_block};
use crate::buffer::{BufferError, BufferPool, BufferTag, FlushControl};
use crate::storage::StorageError;
use crate::storage::backend::{FenceSource, RelFileId, StorageBackend};
use crate::storage::disk::DiskManager;
use crate::storage::page::{Lsn, page_lsn, set_page_lsn};
use crate::txn::{LocalTxnManager, TxnError, Xid};

/// First OID handed out for user objects; lower ones are reserved for the
/// system (as in PostgreSQL).
pub const FIRST_OID: u32 = 16384;

/// OIDs reserved per NEXTOID record.
const OID_PREFETCH: u32 = 8192;

/// OID counter state.
#[derive(Debug)]
struct OidState {
    next: u32,
    /// Upper bound already logged (or recorded in the control file).
    logged_upto: u32,
}

/// `info` values of the Smgr resource manager.
pub mod smgr_info {
    /// Main data: relation file id (u32 LE).
    pub const CREATE: u8 = 0;
}

/// Errors from startup, recovery, and checkpoints.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Wal(#[from] WalError),

    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Redo(#[from] RedoError),

    #[error("checkpoint record at {0:?} not found or invalid")]
    BadCheckpoint(Lsn),

    #[error("record {0:?} is malformed")]
    BadRecord(Lsn),

    #[error(transparent)]
    Txn(#[from] TxnError),

    #[error("checkpoint could not write {0} pages (flush deferred)")]
    CheckpointDeferred(usize),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, RecoveryError>;

/// Creates a new cluster: an empty WAL with an initial checkpoint and the
/// control file (initdb).
pub fn initdb(
    backend: Arc<dyn StorageBackend>,
    fence: Arc<dyn FenceSource>,
    cfg: WalConfig,
) -> Result<()> {
    // The commit log's file exists from the start; its pages are created
    // (and logged) as xids are assigned.
    backend.create(
        crate::storage::backend::FileKey::Relation(crate::txn::clog::CLOG_REL),
        fence.fence_token(),
    )?;
    let wal = Wal::create(backend.clone(), fence.clone(), cfg)?;
    let redo = wal.begin_checkpoint();
    let (start, end) = wal.insert(&checkpoint_record(redo))?;
    wal.flush_to(end);
    ControlFile::create(
        backend,
        fence,
        &ControlData {
            state: DbState::Shutdown,
            timeline: cfg.timeline,
            checkpoint: start,
            redo,
            next_xid: Xid::FIRST,
            next_oid: FIRST_OID,
        },
    )?;
    Ok(())
}

/// A running storage engine: buffer pool with WAL, and the control file.
///
/// Dropping it without [`Engine::shutdown`] flushes nothing, which behaves
/// like a crash; the next [`Engine::start`] recovers.
#[derive(Debug)]
pub struct Engine {
    disk: Arc<DiskManager>,
    pool: Arc<BufferPool>,
    wal: Arc<Wal>,
    txn: Arc<LocalTxnManager>,
    oids: Mutex<OidState>,
    control: Mutex<(ControlFile, ControlData)>,
    /// Serializes checkpoints.
    checkpoint_lock: Mutex<()>,
}

impl Engine {
    /// Starts the engine on an initialized cluster, replaying WAL from the
    /// latest checkpoint's redo point.
    pub fn start(
        backend: Arc<dyn StorageBackend>,
        flush: Arc<dyn FlushControl>,
        cfg: WalConfig,
        nframes: usize,
    ) -> Result<Self> {
        let fence: Arc<dyn FenceSource> = flush.clone();
        let (mut cf, mut data) = ControlFile::open(backend.clone(), fence.clone())?;

        // The checkpoint record names the redo point.
        let mut rd = WalReader::new(backend.clone(), cfg, data.checkpoint, None)?;
        let redo = match rd.next_record()? {
            Some((_, _, d)) if is_checkpoint(&d.record) => {
                checkpoint_redo(&d.record).ok_or(RecoveryError::BadCheckpoint(data.checkpoint))?
            }
            _ => return Err(RecoveryError::BadCheckpoint(data.checkpoint)),
        };

        data.state = DbState::InRecovery;
        cf.write(&data);

        // Replay with an unlogged pool: the WAL being replayed is already
        // durable, so WAL-before-data holds trivially.
        let disk = Arc::new(DiskManager::new(backend.clone()));
        let mut max_oid = data.next_oid.max(FIRST_OID);
        let (end, last, max_xid) = {
            let pool = BufferPool::new(nframes, disk.clone(), flush.clone())?;
            let mut rd = WalReader::new(backend.clone(), cfg, redo, None)?;
            let mut max_xid = Xid::INVALID;
            while let Some((start, end, d)) = rd.next_record()? {
                max_xid = max_xid.max(d.record.xid);
                if d.record.rmgr == RmgrId::Xlog && d.record.info == xlog_info::NEXTOID {
                    let b: [u8; 4] = d
                        .record
                        .main
                        .as_slice()
                        .try_into()
                        .map_err(|_| RecoveryError::BadRecord(start))?;
                    max_oid = max_oid.max(u32::from_le_bytes(b));
                }
                apply(&disk, &pool, &d.record, start, end, fence.as_ref())?;
                for rel in crate::txn::clog::dropped_rels(&d.record) {
                    pool.remove_relation(rel)?;
                }
            }
            pool.flush_all()?;
            (rd.end_lsn(), rd.last_start(), max_xid)
        };
        // Xids seen in WAL may be newer than the control file's value.
        let next_xid = data.next_xid.max(max_xid.next());

        let wal = Arc::new(Wal::open_at(backend, fence, cfg, end, last, redo)?);
        let pool = Arc::new(BufferPool::new(nframes, disk.clone(), flush)?.with_wal(wal.clone()));
        let txn = Arc::new(LocalTxnManager::new(pool.clone(), next_xid)?);
        let engine = Self {
            disk,
            pool,
            wal,
            txn,
            oids: Mutex::new(OidState {
                next: max_oid,
                logged_upto: max_oid,
            }),
            control: Mutex::new((cf, data)),
            checkpoint_lock: Mutex::new(()),
        };
        // End-of-recovery checkpoint: every replayed page is on disk now.
        engine.checkpoint()?;
        Ok(engine)
    }

    pub fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    pub fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }

    /// The transaction manager.
    pub fn txn(&self) -> &Arc<LocalTxnManager> {
        &self.txn
    }

    /// Current control file contents.
    pub fn control_data(&self) -> ControlData {
        self.control.lock().1
    }

    /// Allocates a new OID (never reused, even after a crash, once anything
    /// logged after it is durable).
    pub fn allocate_oid(&self) -> Result<u32> {
        let mut o = self.oids.lock();
        if o.next >= o.logged_upto {
            let upto = o.next + OID_PREFETCH;
            let mut rec = Record::new(RmgrId::Xlog, xlog_info::NEXTOID);
            rec.main = upto.to_le_bytes().to_vec();
            self.wal.insert(&rec)?;
            o.logged_upto = upto;
        }
        let oid = o.next;
        o.next += 1;
        Ok(oid)
    }

    /// Creates an empty relation file and logs it, so recovery recreates
    /// the file if it is lost.
    pub fn create_relation(&self, rel: RelFileId) -> Result<()> {
        self.disk.create_relation(rel, self.pool.fence_token())?;
        let mut rec = Record::new(RmgrId::Smgr, smgr_info::CREATE);
        rec.main = rel.0.to_le_bytes().to_vec();
        self.wal.insert(&rec)?;
        Ok(())
    }

    /// Runs a checkpoint and returns its redo point.
    pub fn checkpoint(&self) -> Result<Lsn> {
        let _serial = self.checkpoint_lock.lock();
        let redo = self.wal.begin_checkpoint();
        let stats = self.pool.write_all()?;
        if stats.deferred > 0 {
            return Err(RecoveryError::CheckpointDeferred(stats.deferred));
        }
        if let Err(e) = self.pool.sync_pending() {
            fatal("data file fsync failed during checkpoint", &e);
        }
        let (start, end) = self.wal.insert(&checkpoint_record(redo))?;
        self.wal.flush_to(end);
        let mut ctl = self.control.lock();
        ctl.1.state = DbState::InProduction;
        ctl.1.checkpoint = start;
        ctl.1.redo = redo;
        ctl.1.next_xid = self.txn.next_xid();
        ctl.1.next_oid = self.oids.lock().logged_upto;
        let data = ctl.1;
        ctl.0.write(&data);
        drop(ctl);
        // Segments before the redo point are no longer needed.
        self.wal.remove_segments_before(redo)?;
        Ok(redo)
    }

    /// Checkpoints and marks the cluster cleanly shut down. Background
    /// threads must be stopped first.
    pub fn shutdown(&self) -> Result<()> {
        self.checkpoint()?;
        let mut ctl = self.control.lock();
        ctl.1.state = DbState::Shutdown;
        let data = ctl.1;
        ctl.0.write(&data);
        Ok(())
    }
}

/// Replays one record.
fn apply(
    disk: &DiskManager,
    pool: &BufferPool,
    rec: &Record,
    start: Lsn,
    end: Lsn,
    fence: &dyn FenceSource,
) -> Result<()> {
    match rec.rmgr {
        RmgrId::Smgr => {
            let rel = rel_from_main(rec).ok_or(RecoveryError::BadRecord(start))?;
            if disk.nblocks(rel).is_err() {
                disk.create_relation(rel, fence.fence_token())?;
            }
            return Ok(());
        }
        RmgrId::Xlog if rec.info == xlog_info::CHECKPOINT || rec.info == xlog_info::NEXTOID => {
            return Ok(());
        }
        _ => {}
    }
    for (idx, b) in rec.blocks.iter().enumerate() {
        // A relation dropped later in the WAL may already be gone: recreate
        // it so replay can continue; the later commit record deletes it.
        if matches!(disk.nblocks(b.rel), Err(ref e) if super::is_not_found(e)) {
            disk.create_relation(b.rel, fence.fence_token())?;
        }
        // Blocks extended but never written before the crash: re-extend.
        while disk.nblocks(b.rel)? <= b.blk.0 {
            drop(pool.extend(b.rel)?);
        }
        let tag = BufferTag {
            rel: b.rel,
            blk: b.blk,
        };
        let pin = if b.image.is_some() || b.will_init {
            pool.fetch_for_overwrite(tag)?
        } else {
            pool.fetch(tag)?
        };
        let mut page = pin.write();
        if let Some(img) = &b.image {
            page.copy_from_slice(img);
        } else if b.will_init || page_lsn(&page) < end {
            redo_block(rec, idx, &mut page)?;
        } else {
            continue;
        }
        set_page_lsn(&mut page, end);
        page.mark_dirty();
    }
    Ok(())
}

fn checkpoint_record(redo: Lsn) -> Record {
    let mut rec = Record::new(RmgrId::Xlog, xlog_info::CHECKPOINT);
    rec.main = redo.0.to_le_bytes().to_vec();
    rec
}

fn is_checkpoint(rec: &Record) -> bool {
    rec.rmgr == RmgrId::Xlog && rec.info == xlog_info::CHECKPOINT
}

fn checkpoint_redo(rec: &Record) -> Option<Lsn> {
    let b: [u8; 8] = rec.main.as_slice().try_into().ok()?;
    Some(Lsn(u64::from_le_bytes(b)))
}

fn rel_from_main(rec: &Record) -> Option<RelFileId> {
    let b: [u8; 4] = rec.main.as_slice().try_into().ok()?;
    Some(RelFileId(u32::from_le_bytes(b)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::btree::BTree;
    use crate::access::btree::key::encode_key;
    use crate::access::heap::HeapFile;
    use crate::access::tuple::{Tid, TupleDesc, TupleHeader, deform_tuple, form_tuple};
    use crate::buffer::SingleModeFlush;
    use crate::storage::backend::{FenceToken, FileKey, FixedFence, StorageBackend};
    use crate::storage::mem::MemBackend;
    use crate::storage::page::{BlockNumber, PAGE_SIZE};
    use crate::txn::CommandId;
    use crate::types::{DataType, Value};
    use std::collections::BTreeSet;
    use std::ops::Bound;

    const FENCE: FenceToken = FenceToken::new(1);
    const HEAP: RelFileId = RelFileId(100);
    const IDX: RelFileId = RelFileId(101);

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

    fn start(mem: &Arc<MemBackend>, frames: usize) -> Engine {
        Engine::start(
            mem.clone(),
            Arc::new(SingleModeFlush::new(FENCE)),
            cfg(),
            frames,
        )
        .unwrap()
    }

    fn desc() -> TupleDesc {
        TupleDesc::new(vec![DataType::Int8, DataType::Text]).unwrap()
    }

    fn key(id: i64) -> Vec<u8> {
        let mut k = Vec::new();
        encode_key(&[DataType::Int8], &[Value::Int8(id)], &mut k).unwrap();
        k
    }

    fn create_tables(e: &Engine) {
        e.create_relation(HEAP).unwrap();
        e.create_relation(IDX).unwrap();
        BTree::create(IDX, e.pool().clone()).unwrap();
    }

    /// Inserts rows `ids` into the heap and the index.
    fn load(e: &Engine, ids: std::ops::Range<i64>) -> Vec<Tid> {
        let heap = HeapFile::open(HEAP, e.pool().clone()).unwrap();
        let tree = BTree::open(IDX, e.pool().clone()).unwrap();
        ids.map(|i| {
            let t = form_tuple(
                &desc(),
                &[Value::Int8(i), Value::Text("r".repeat(120))],
                Xid(7),
                CommandId(0),
            )
            .unwrap();
            let tid = heap.insert(&t).unwrap();
            tree.insert(&key(i), tid).unwrap();
            tid
        })
        .collect()
    }

    /// Checks heap and index agree and returns the ids present in both.
    fn verify(e: &Engine) -> BTreeSet<i64> {
        let heap = HeapFile::open(HEAP, e.pool().clone()).unwrap();
        let tree = BTree::open(IDX, e.pool().clone()).unwrap();
        let heap_ids: BTreeSet<i64> = heap
            .scan()
            .unwrap()
            .map(|r| match deform_tuple(&desc(), &r.unwrap().1).unwrap()[0] {
                Value::Int8(i) => i,
                _ => panic!("bad row"),
            })
            .collect();
        let entries: Vec<(Vec<u8>, Tid)> = tree
            .scan(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert!(entries.windows(2).all(|w| w[0] < w[1]), "index sorted");
        let mut index_ids = BTreeSet::new();
        for (k, tid) in &entries {
            let row = heap
                .fetch(*tid)
                .unwrap()
                .expect("index points at a heap row");
            let Value::Int8(id) = deform_tuple(&desc(), &row).unwrap()[0] else {
                panic!("bad row");
            };
            assert_eq!(*k, key(id), "index key matches heap row");
            index_ids.insert(id);
        }
        // The index may only lack the very last row (crash between the
        // heap insert and the index insert).
        assert!(index_ids.is_subset(&heap_ids));
        assert!(heap_ids.len() - index_ids.len() <= 1);
        index_ids
    }

    #[test]
    fn initdb_then_clean_restart() {
        let mem = fresh();
        let e = start(&mem, 16);
        assert_eq!(e.control_data().state, DbState::InProduction);
        create_tables(&e);
        load(&e, 0..10);
        e.shutdown().unwrap();
        let (_, data) = ControlFile::open(mem.clone(), Arc::new(FixedFence(FENCE))).unwrap();
        assert_eq!(data.state, DbState::Shutdown);
        let e = start(&mem, 16);
        assert_eq!(verify(&e), (0..10).collect());
    }

    #[test]
    fn recovers_after_crash_with_evictions() {
        let mem = fresh();
        let e = start(&mem, 24);
        create_tables(&e);
        load(&e, 0..1500);
        e.wal().flush_all(); // "commit": everything so far is durable
        load(&e, 1500..1600); // may be partly lost
        drop(e); // crash: buffer pool contents are gone

        let e = start(&mem, 24);
        let ids = verify(&e);
        assert!((0..1500).all(|i| ids.contains(&i)), "durable rows survive");
        // The recovered cluster keeps working.
        load(&e, 5000..5100);
        e.wal().flush_all();
        drop(e);
        let e = start(&mem, 24);
        let ids = verify(&e);
        assert!((5000..5100).all(|i| ids.contains(&i)));
    }

    #[test]
    fn recovers_from_later_checkpoint() {
        let mem = fresh();
        let e = start(&mem, 32);
        create_tables(&e);
        load(&e, 0..500);
        let redo = e.checkpoint().unwrap();
        assert_eq!(e.control_data().redo, redo);
        load(&e, 500..900);
        e.wal().flush_all();
        drop(e);
        let e = start(&mem, 32);
        assert!(e.control_data().redo > redo);
        assert_eq!(verify(&e), (0..900).collect());
    }

    #[test]
    fn torn_page_is_repaired_from_full_page_image() {
        let mem = fresh();
        let e = start(&mem, 64);
        create_tables(&e);
        let tids = load(&e, 0..200);
        e.checkpoint().unwrap();
        // First change to heap block 0 after the checkpoint: logged with a
        // full image. Then the page is written and its write "tears".
        let heap = HeapFile::open(HEAP, e.pool().clone()).unwrap();
        let victim = tids[0];
        assert_eq!(victim.blk, BlockNumber(0));
        heap.delete(victim, Xid(9), CommandId(1)).unwrap();
        e.wal().flush_all();
        e.pool().flush_all().unwrap();
        drop(heap);
        drop(e);
        mem.overwrite(
            FileKey::Relation(HEAP),
            PAGE_SIZE / 2,
            &[0xEE; PAGE_SIZE / 2],
        );

        let e = start(&mem, 64);
        assert_eq!(verify(&e), (0..200).collect());
        let heap = HeapFile::open(HEAP, e.pool().clone()).unwrap();
        let row = heap.fetch(victim).unwrap().unwrap();
        assert_eq!(TupleHeader::decode(&row).unwrap().xmax, Xid(9));
    }

    #[test]
    fn lost_relation_file_is_recreated() {
        let mem = fresh();
        let e = start(&mem, 32);
        e.checkpoint().unwrap();
        create_tables(&e);
        load(&e, 0..50);
        e.wal().flush_all();
        drop(e);
        // The files' creation "did not survive" the crash.
        mem.remove(FileKey::Relation(HEAP), FENCE).unwrap();
        mem.remove(FileKey::Relation(IDX), FENCE).unwrap();
        let e = start(&mem, 32);
        assert_eq!(verify(&e), (0..50).collect());
    }

    #[test]
    fn old_segments_are_removed_after_checkpoint() {
        let mem = fresh();
        let e = start(&mem, 32);
        create_tables(&e);
        load(&e, 0..3000);
        e.wal().flush_all();
        assert!(e.wal().insert_lsn().0 / cfg().segment_size >= 3);
        let seg0 = FileKey::Wal {
            timeline: 1,
            segno: 0,
        };
        assert!(mem.file_len(seg0).is_ok());
        let redo = e.checkpoint().unwrap();
        assert!(
            mem.file_len(seg0).is_err(),
            "segment before the redo point removed"
        );
        let cur = FileKey::Wal {
            timeline: 1,
            segno: redo.0 / cfg().segment_size,
        };
        assert!(
            mem.file_len(cur).is_ok(),
            "segment holding the redo point kept"
        );
        load(&e, 3000..3100);
        e.wal().flush_all();
        drop(e);
        let e = start(&mem, 32);
        assert_eq!(verify(&e), (0..3100).collect());
    }

    #[test]
    fn background_threads_then_crash() {
        use crate::wal::{Background, BackgroundConfig};
        use std::time::Duration;
        let mem = fresh();
        let e = Arc::new(start(&mem, 24));
        create_tables(&e);
        let bg = Background::start(
            &e,
            BackgroundConfig {
                checkpoint_timeout: Duration::from_millis(30),
                checkpoint_wal_bytes: 64 * 1024,
                bgwriter_delay: Duration::from_millis(5),
                bgwriter_max_pages: 8,
            },
        )
        .unwrap();
        let first_redo = e.control_data().redo;
        load(&e, 0..1500);
        e.wal().flush_all();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while e.control_data().redo == first_redo {
            assert!(std::time::Instant::now() < deadline, "no checkpoint ran");
            std::thread::sleep(Duration::from_millis(10));
        }
        bg.stop();
        drop(e);
        let e = start(&mem, 24);
        assert_eq!(verify(&e), (0..1500).collect());
    }

    #[test]
    fn oids_are_unique_across_crash_and_checkpoint() {
        let mem = fresh();
        let used = {
            let e = start(&mem, 16);
            let a = e.allocate_oid().unwrap();
            let b = e.allocate_oid().unwrap();
            assert_eq!((a, b), (FIRST_OID, FIRST_OID + 1));
            e.create_relation(RelFileId(b)).unwrap();
            e.wal().flush_all();
            b
        };
        let e = start(&mem, 16);
        let c = e.allocate_oid().unwrap();
        assert!(c > used, "{c} reused after crash");
        let redo = e.checkpoint().unwrap();
        let _ = redo;
        drop(e);
        let e = start(&mem, 16);
        assert!(e.allocate_oid().unwrap() > c);
    }

    #[test]
    fn bootstrap_xid_is_committed() {
        use crate::txn::{TransactionManager, XidStatus};
        let mem = fresh();
        let e = start(&mem, 16);
        assert_eq!(
            e.txn().status(Xid::BOOTSTRAP).unwrap(),
            XidStatus::Committed
        );
        assert_eq!(e.txn().assign_xid().unwrap(), Xid::FIRST);
    }

    fn txn(e: &Engine, locks: &Arc<crate::lock::LockManager>) -> crate::txn::Transaction {
        let tm: Arc<dyn crate::txn::TransactionManager> = e.txn().clone();
        crate::txn::Transaction::begin(
            tm,
            locks.clone(),
            crate::txn::Isolation::ReadCommitted,
            crate::lock::WaitPolicy::default(),
        )
    }

    fn exists(mem: &MemBackend, rel: RelFileId) -> bool {
        mem.file_len(FileKey::Relation(rel)).is_ok()
    }

    #[test]
    fn created_file_is_removed_on_abort() {
        let mem = fresh();
        let e = start(&mem, 16);
        let locks = Arc::new(crate::lock::LockManager::new());
        let mut t = txn(&e, &locks);
        t.write_xid().unwrap();
        e.create_relation(HEAP).unwrap();
        t.drop_on_abort(HEAP);
        assert!(exists(&mem, HEAP));
        t.abort().unwrap();
        assert!(!exists(&mem, HEAP));
    }

    #[test]
    fn dropped_file_is_removed_only_on_commit() {
        let mem = fresh();
        let e = start(&mem, 16);
        let locks = Arc::new(crate::lock::LockManager::new());
        e.create_relation(HEAP).unwrap();
        let mut t = txn(&e, &locks);
        t.write_xid().unwrap();
        t.drop_on_commit(HEAP);
        t.abort().unwrap();
        assert!(exists(&mem, HEAP), "DROP rolled back");
        let mut t = txn(&e, &locks);
        t.write_xid().unwrap();
        t.drop_on_commit(HEAP);
        t.commit().unwrap();
        assert!(!exists(&mem, HEAP));
    }

    #[test]
    fn replay_of_a_later_dropped_relation() {
        let mem = fresh();
        {
            let e = start(&mem, 16);
            let locks = Arc::new(crate::lock::LockManager::new());
            e.checkpoint().unwrap();
            create_tables(&e);
            load(&e, 0..200); // page records for HEAP and IDX
            let mut t = txn(&e, &locks);
            t.write_xid().unwrap();
            t.drop_on_commit(HEAP);
            t.drop_on_commit(IDX);
            t.commit().unwrap(); // files deleted
            assert!(!exists(&mem, HEAP));
        }
        // Replay meets HEAP/IDX page records with the files gone.
        let e = start(&mem, 16);
        assert!(!exists(&mem, HEAP) && !exists(&mem, IDX));
        drop(e);
    }

    #[test]
    fn deletion_is_redone_from_the_commit_record() {
        let mem = fresh();
        {
            let e = start(&mem, 16);
            let locks = Arc::new(crate::lock::LockManager::new());
            e.create_relation(HEAP).unwrap();
            let mut t = txn(&e, &locks);
            t.write_xid().unwrap();
            t.drop_on_commit(HEAP);
            t.commit().unwrap();
        }
        // As if the process died between the commit and the unlink.
        mem.create(FileKey::Relation(HEAP), FENCE).unwrap();
        let e = start(&mem, 16);
        assert!(!exists(&mem, HEAP), "recovery deletes it");
        drop(e);
    }

    #[test]
    fn recovery_is_repeatable() {
        let mem = fresh();
        let e = start(&mem, 16);
        create_tables(&e);
        load(&e, 0..300);
        e.wal().flush_all();
        drop(e);
        let e = start(&mem, 16);
        let first = verify(&e);
        drop(e); // crash right after recovery
        let e = start(&mem, 16);
        assert_eq!(verify(&e), first);
    }
}
