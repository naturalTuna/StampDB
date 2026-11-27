//! kill -9 crash recovery test on the real O_DIRECT backend.
//!
//! `kill9_recovery` re-executes this test binary in worker mode
//! (`kill9_worker` with `MYDB_CRASH_DIR` set). The worker inserts rows into
//! a heap and a B+Tree index with background checkpoints and writes
//! running, flushes the WAL every few rows and reports `ACK n` (all ids
//! below n are durable). The parent SIGKILLs it at a random moment,
//! recovers the cluster in-process, and checks that every acknowledged
//! row is present in both the heap and the index, and that they agree.

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::ops::Bound;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mydb::access::btree::BTree;
use mydb::access::btree::key::encode_key;
use mydb::access::heap::HeapFile;
use mydb::access::tuple::{Tid, TupleDesc, deform_tuple, form_tuple};
use mydb::buffer::SingleModeFlush;
use mydb::storage::backend::{FenceToken, FixedFence, RelFileId, StorageBackend};
use mydb::storage::local::LocalDirBackend;
use mydb::txn::{CommandId, Xid};
use mydb::types::{DataType, Value};
use mydb::wal::{Background, BackgroundConfig, Engine, WalConfig, initdb};

const HEAP: RelFileId = RelFileId(16384);
const IDX: RelFileId = RelFileId(16385);
const FENCE: FenceToken = FenceToken::new(1);
const FRAMES: usize = 32;
const BATCH: i64 = 20;
const ROUNDS: u64 = 6;
const ROUND_BASE: i64 = 1_000_000;

fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 1 << 20,
    }
}

fn backend(dir: &Path) -> Arc<dyn StorageBackend> {
    Arc::new(LocalDirBackend::open(dir).unwrap())
}

fn start(dir: &Path) -> Engine {
    Engine::start(
        backend(dir),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
        FRAMES,
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

/// Worker mode: insert forever, acknowledging durable prefixes.
#[test]
fn kill9_worker() {
    let Ok(dir) = std::env::var("MYDB_CRASH_DIR") else {
        return; // not in worker mode
    };
    let mut next: i64 = std::env::var("MYDB_START_ID").unwrap().parse().unwrap();
    let engine = Arc::new(start(Path::new(&dir)));
    let _bg = Background::start(
        &engine,
        BackgroundConfig {
            checkpoint_timeout: Duration::from_millis(40),
            checkpoint_wal_bytes: 256 * 1024,
            bgwriter_delay: Duration::from_millis(5),
            bgwriter_max_pages: 16,
        },
    )
    .unwrap();
    let heap = HeapFile::open(HEAP, engine.pool().clone()).unwrap();
    let tree = BTree::open(IDX, engine.pool().clone()).unwrap();
    let mut out = std::io::stdout();
    loop {
        for _ in 0..BATCH {
            let pad = "k".repeat(50 + (next % 200) as usize);
            let t = form_tuple(
                &desc(),
                &[Value::Int8(next), Value::Text(pad)],
                Xid(5),
                CommandId(0),
            )
            .unwrap();
            let tid = heap.insert(&t).unwrap();
            tree.insert(&key(next), tid).unwrap();
            next += 1;
        }
        engine.wal().flush_all();
        writeln!(out, "ACK {next}").unwrap();
        out.flush().unwrap();
    }
}

/// Heap and index ids, after checking that they agree. Without
/// transactions, each crash can leave at most one heap row whose index
/// entry was not yet logged (the WAL survives as a prefix), so up to
/// `crashes` such rows may exist.
fn verify(engine: &Engine, crashes: usize) -> BTreeSet<i64> {
    let heap = HeapFile::open(HEAP, engine.pool().clone()).unwrap();
    let tree = BTree::open(IDX, engine.pool().clone()).unwrap();
    let id_of = |row: &[u8]| match deform_tuple(&desc(), row).unwrap()[0] {
        Value::Int8(i) => i,
        _ => panic!("bad row"),
    };
    let heap_ids: BTreeSet<i64> = heap.scan().unwrap().map(|r| id_of(&r.unwrap().1)).collect();
    let entries: Vec<(Vec<u8>, Tid)> = tree
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        entries.windows(2).all(|w| w[0] < w[1]),
        "index sorted, no duplicates"
    );
    let mut index_ids = BTreeSet::new();
    for (k, tid) in &entries {
        let row = heap
            .fetch(*tid)
            .unwrap()
            .expect("index entry points at a heap row");
        let id = id_of(&row);
        assert_eq!(*k, key(id));
        assert!(index_ids.insert(id), "one index entry per row");
    }
    assert!(index_ids.is_subset(&heap_ids));
    assert!(
        heap_ids.len() - index_ids.len() <= crashes,
        "at most one row per crash may lack its index entry"
    );
    index_ids
}

fn random_below(n: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    let mut x = nanos ^ 0x9E37_79B9_7F4A_7C15;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x % n
}

#[test]
fn kill9_recovery() {
    if std::env::var("MYDB_CRASH_DIR").is_ok() {
        return; // the worker process runs only `kill9_worker`
    }
    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dir = tmp.path();
    initdb(backend(dir), Arc::new(FixedFence(FENCE)), cfg()).unwrap();
    {
        let e = start(dir);
        e.create_relation(HEAP).unwrap();
        e.create_relation(IDX).unwrap();
        BTree::create(IDX, e.pool().clone()).unwrap();
        e.shutdown().unwrap();
    }

    let exe = std::env::current_exe().unwrap();
    let mut acked: Vec<(i64, i64)> = Vec::new(); // per round: [base, acked)
    for round in 0..ROUNDS {
        let base = (round as i64 + 1) * ROUND_BASE;
        let mut child = Command::new(&exe)
            .args(["kill9_worker", "--exact", "--nocapture", "--test-threads=1"])
            .env("MYDB_CRASH_DIR", dir)
            .env("MYDB_START_ID", base.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(n) = line.strip_prefix("ACK ")
                    && tx.send(n.parse::<i64>().unwrap()).is_err()
                {
                    break;
                }
            }
        });

        // Wait for the worker to make progress, then kill it at a random
        // moment (possibly mid-flush or mid-checkpoint).
        let first = rx
            .recv_timeout(Duration::from_secs(60))
            .expect("worker made no progress");
        let mut last = first;
        let kill_at = Instant::now() + Duration::from_millis(50 + random_below(400));
        while Instant::now() < kill_at {
            if let Ok(n) = rx.recv_timeout(Duration::from_millis(5)) {
                last = n;
            }
        }
        child.kill().unwrap(); // SIGKILL
        let status = child.wait().unwrap();
        assert!(!status.success(), "worker must have been killed");
        while let Ok(n) = rx.try_recv() {
            last = last.max(n);
        }
        drop(rx);
        let _ = reader.join();
        acked.push((base, last));

        // Recover and check every acknowledged row of every round so far.
        let e = start(dir);
        let ids = verify(&e, round as usize + 1);
        for &(b, a) in &acked {
            for id in b..a {
                assert!(
                    ids.contains(&id),
                    "round {round}: acknowledged row {id} lost"
                );
            }
        }
        eprintln!(
            "round {round}: acked {} rows, {} rows present",
            last - base,
            ids.len()
        );
        drop(e); // leave it as a crash too; the next start recovers again
    }
}
