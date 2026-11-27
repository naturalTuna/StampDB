//! SQL sessions on the O_DIRECT backend across a crash: committed
//! statements survive, an open transaction block does not.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mydb::buffer::SingleModeFlush;
use mydb::lock::WaitPolicy;
use mydb::sql::Database;
use mydb::sql::datum::format_value;
use mydb::storage::backend::FenceToken;
use mydb::storage::local::LocalDirBackend;
use mydb::wal::{Engine, WalConfig};

const FENCE: FenceToken = FenceToken::new(1);

fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 1 << 20,
    }
}

fn start(dir: &Path) -> Arc<Database> {
    let engine = Engine::start(
        Arc::new(LocalDirBackend::open(dir).unwrap()),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
        64,
    )
    .unwrap();
    Database::new(
        Arc::new(engine),
        WaitPolicy {
            lock_timeout: Some(Duration::from_secs(10)),
            deadlock_timeout: Duration::from_millis(100),
        },
    )
    .unwrap()
}

fn rows(db: &Arc<Database>, sql: &str) -> Vec<String> {
    let r = db.session().execute(sql).unwrap();
    r.last()
        .unwrap()
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| format_value(v).unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

#[test]
fn committed_sql_survives_a_crash() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    mydb::catalog::initdb(
        Arc::new(LocalDirBackend::open(dir.path()).unwrap()),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
    )
    .unwrap();
    {
        let db = start(dir.path());
        let mut s = db.session();
        s.execute("CREATE TABLE acct (id INT, owner TEXT, bal INT)")
            .unwrap();
        s.execute("CREATE UNIQUE INDEX acct_id ON acct (id)")
            .unwrap();
        for i in 0..200 {
            s.execute(&format!(
                "INSERT INTO acct VALUES ({i}, 'o{}', 100)",
                i % 10
            ))
            .unwrap();
        }
        s.execute("UPDATE acct SET bal = bal + id WHERE id % 2 = 0")
            .unwrap();
        s.execute("DELETE FROM acct WHERE id >= 190").unwrap();
        s.execute("BEGIN; CREATE TABLE gone (x INT); UPDATE acct SET bal = 0; DELETE FROM acct WHERE id < 50;")
            .unwrap();
        let mut other = db.session();
        other.execute("BEGIN").unwrap();
        other
            .execute("INSERT INTO acct VALUES (1000, 'late', 1)")
            .unwrap();
        // Crash: everything is dropped without a shutdown checkpoint. The
        // open transactions' WAL may or may not have reached disk.
        db.engine().wal().flush_all();
    }
    let db = start(dir.path());
    assert_eq!(
        rows(&db, "SELECT count(*), sum(bal), min(id), max(id) FROM acct"),
        ["190 27930 0 189"]
    );
    assert_eq!(rows(&db, "SELECT bal FROM acct WHERE id = 42"), ["142"]);
    assert_eq!(
        rows(
            &db,
            "SELECT owner, count(*) FROM acct GROUP BY owner ORDER BY owner LIMIT 1"
        ),
        ["o0 19"]
    );
    assert!(db.session().execute("SELECT * FROM gone").is_err());
    // The unique index is intact and usable.
    assert!(
        db.session()
            .execute("INSERT INTO acct VALUES (5, 'dup', 0)")
            .is_err()
    );
    db.session()
        .execute("INSERT INTO acct VALUES (1000, 'new', 1)")
        .unwrap();
    assert_eq!(rows(&db, "SELECT owner FROM acct WHERE id = 1000"), ["new"]);
}
