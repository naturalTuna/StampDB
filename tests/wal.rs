//! WAL and control file over the real O_DIRECT backend.

use std::sync::Arc;

use mydb::storage::backend::{FenceSource, FenceToken, FixedFence, StorageBackend};
use mydb::storage::local::LocalDirBackend;
use mydb::storage::page::Lsn;
use mydb::txn::Xid;
use mydb::wal::log::FIRST_LSN;
use mydb::wal::record::{Record, RmgrId};
use mydb::wal::{ControlData, ControlFile, DbState, Wal, WalConfig, WalReader};

fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 64 * 1024,
    }
}

fn fence() -> Arc<dyn FenceSource> {
    Arc::new(FixedFence(FenceToken::new(1)))
}

fn rec(i: u32) -> Record {
    let mut r = Record::new(RmgrId::Heap, (i % 256) as u8);
    r.main = i.to_le_bytes().repeat(1 + (i as usize % 300));
    r
}

fn read_all(be: Arc<dyn StorageBackend>) -> (Vec<Record>, Lsn, Lsn) {
    let mut rd = WalReader::new(be, cfg(), FIRST_LSN, Some(Lsn::INVALID)).unwrap();
    let mut out = Vec::new();
    while let Some((_, _, d)) = rd.next_record().unwrap() {
        out.push(d.record);
    }
    (out, rd.end_lsn(), rd.last_start())
}

#[test]
fn wal_survives_reopen_on_direct_io() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    {
        let be: Arc<dyn StorageBackend> = Arc::new(LocalDirBackend::open(dir.path()).unwrap());
        let wal = Wal::create(be, fence(), cfg()).unwrap();
        for i in 0..300 {
            let (_, end) = wal.insert(&rec(i)).unwrap();
            if i % 10 == 0 {
                wal.flush_to(end);
            }
        }
        wal.flush_all();
    }
    let be: Arc<dyn StorageBackend> = Arc::new(LocalDirBackend::open(dir.path()).unwrap());
    let (got, end, last) = read_all(be.clone());
    assert_eq!(got.len(), 300);
    assert!(got.iter().enumerate().all(|(i, r)| *r == rec(i as u32)));
    assert!(end.0 / cfg().segment_size >= 2, "spans several segments");
    assert!(dir.path().join("wal/000000010000000000000000").is_file());
    assert!(dir.path().join("wal").read_dir().unwrap().count() >= 3);

    let wal = Wal::open_at(be.clone(), fence(), cfg(), end, last).unwrap();
    for i in 300..320 {
        wal.insert(&rec(i)).unwrap();
    }
    wal.flush_all();
    let (got, _, _) = read_all(be);
    assert_eq!(got.len(), 320);
    assert_eq!(got[319], rec(319));
}

#[test]
fn control_file_on_direct_io() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let data = |n: u64| ControlData {
        state: DbState::InProduction,
        timeline: 1,
        checkpoint: Lsn(n),
        redo: Lsn(n),
        next_xid: Xid(n),
    };
    {
        let be: Arc<dyn StorageBackend> = Arc::new(LocalDirBackend::open(dir.path()).unwrap());
        let mut cf = ControlFile::create(be, fence(), &data(1)).unwrap();
        cf.write(&data(2));
        cf.write(&data(3));
    }
    let be: Arc<dyn StorageBackend> = Arc::new(LocalDirBackend::open(dir.path()).unwrap());
    assert_eq!(ControlFile::open(be, fence()).unwrap().1, data(3));
    assert!(dir.path().join("control").is_file());
}
