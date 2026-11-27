use std::sync::Arc;
use std::time::Duration;

use super::datum::format_value;
use super::error::code;
use super::*;
use crate::buffer::SingleModeFlush;
use crate::lock::WaitPolicy;
use crate::storage::backend::FenceToken;
use crate::storage::mem::MemBackend;
use crate::wal::{Engine, WalConfig};

const FENCE: FenceToken = FenceToken::new(1);

fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 256 * 1024,
    }
}

pub(super) fn db() -> Arc<Database> {
    let mem = Arc::new(MemBackend::default());
    crate::catalog::initdb(mem.clone(), Arc::new(SingleModeFlush::new(FENCE)), cfg()).unwrap();
    let engine =
        Arc::new(Engine::start(mem, Arc::new(SingleModeFlush::new(FENCE)), cfg(), 128).unwrap());
    Database::new(
        engine,
        WaitPolicy {
            lock_timeout: Some(Duration::from_secs(10)),
            deadlock_timeout: Duration::from_millis(100),
        },
    )
    .unwrap()
}

/// Rows of the last statement, values in text form (NULL as "NULL").
pub(super) fn q(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let r = s
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e} ({})", e.code));
    r.last()
        .unwrap()
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| format_value(v).unwrap_or_else(|| "NULL".to_owned()))
                .collect()
        })
        .collect()
}

pub(super) fn ok(s: &mut Session, sql: &str) -> String {
    let r = s
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e} ({})", e.code));
    r.last().unwrap().tag.clone()
}

pub(super) fn fails(s: &mut Session, sql: &str) -> SqlError {
    match s.execute(sql) {
        Ok(r) => panic!("{sql}: expected an error, got {r:?}"),
        Err(e) => e,
    }
}

pub(super) fn rows(v: &[&[&str]]) -> Vec<Vec<String>> {
    v.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[test]
fn select_without_from() {
    let db = db();
    let mut s = db.session();
    assert_eq!(
        q(
            &mut s,
            "SELECT 1, 'a', 1 + 2 * 3, 7 / 2, NULL, -7 % 3, 2.5::float8 * 2"
        ),
        rows(&[&["1", "a", "7", "3", "NULL", "-1", "5"]])
    );
    let r = s
        .execute("SELECT 1 AS x, 'a' || 1, 2::int8, CAST('3' AS integer), lower('AbC')")
        .unwrap();
    let names: Vec<&str> = r[0].columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["x", "?column?", "int8", "int4", "lower"]);
    assert_eq!(
        q(
            &mut s,
            "SELECT 1 < 2, 'a' < 'b', NULL = NULL, 1 IN (2, 3, NULL), 3 BETWEEN 1 AND 5"
        ),
        rows(&[&["t", "t", "NULL", "NULL", "t"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT CASE WHEN 1 > 2 THEN 'x' ELSE 'y' END, COALESCE(NULL, 2), NULLIF(1, 1), abs(-3)"
        ),
        rows(&[&["y", "2", "NULL", "3"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT -2147483648, 2147483648, 1.5::float8 > 1, 'ab' LIKE 'a%'"
        ),
        rows(&[&["-2147483648", "2147483648", "t", "t"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT TIMESTAMP '2024-01-02 03:04:05.5', 1e3::float8, true::text"
        ),
        rows(&[&["2024-01-02 03:04:05.5", "1000", "true"]])
    );
}

#[test]
fn expression_errors_have_sqlstates() {
    let db = db();
    let mut s = db.session();
    assert_eq!(fails(&mut s, "SELECT 1 / 0").code, code::DIVISION_BY_ZERO);
    assert_eq!(
        fails(&mut s, "SELECT 2147483647 + 1").code,
        code::NUMERIC_VALUE_OUT_OF_RANGE
    );
    assert_eq!(
        fails(&mut s, "SELECT 1 + 'a'").code,
        code::INVALID_TEXT_REPRESENTATION
    );
    let e = fails(&mut s, "SELECT 1 = true");
    assert_eq!(e.code, code::UNDEFINED_FUNCTION);
    assert_eq!(e.message, "operator does not exist: integer = boolean");
    assert_eq!(
        fails(&mut s, "SELECT 1.5").code,
        code::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        fails(&mut s, "SELECT 1 + 1.5").code,
        code::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(fails(&mut s, "SELECT x").code, code::UNDEFINED_COLUMN);
    assert_eq!(
        fails(&mut s, "SELECT * FROM nope").code,
        code::UNDEFINED_TABLE
    );
    assert_eq!(fails(&mut s, "SELEC 1").code, code::SYNTAX_ERROR);
    assert_eq!(
        fails(&mut s, "SELECT 1 WHERE 1").code,
        code::DATATYPE_MISMATCH
    );
    assert_eq!(
        fails(&mut s, "SELECT 'x'::integer").code,
        code::INVALID_TEXT_REPRESENTATION
    );
}

#[test]
fn create_insert_select() {
    let db = db();
    let mut s = db.session();
    assert_eq!(
        ok(
            &mut s,
            "CREATE TABLE t (id INT, name TEXT, score FLOAT8, v VARCHAR(3))"
        ),
        "CREATE TABLE"
    );
    assert_eq!(
        ok(
            &mut s,
            "INSERT INTO t VALUES (1, 'a', 1.5, 'x'), (2, 'b', NULL, 'yy'), (3, NULL, 2, NULL)"
        ),
        "INSERT 0 3"
    );
    assert_eq!(
        ok(&mut s, "INSERT INTO t (name, id) VALUES ('d', 4)"),
        "INSERT 0 1"
    );
    assert_eq!(
        q(&mut s, "SELECT * FROM t ORDER BY id"),
        rows(&[
            &["1", "a", "1.5", "x"],
            &["2", "b", "NULL", "yy"],
            &["3", "NULL", "2", "NULL"],
            &["4", "d", "NULL", "NULL"],
        ])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT id, score * 2 AS s2 FROM t WHERE score IS NOT NULL ORDER BY s2 DESC"
        ),
        rows(&[&["3", "4"], &["1", "3"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT name FROM t ORDER BY name DESC LIMIT 2 OFFSET 1"
        ),
        rows(&[&["d"], &["b"]]),
        "NULLs first when descending"
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT t.id FROM t AS t WHERE t.name IN ('a', 'b') ORDER BY 1"
        ),
        rows(&[&["1"], &["2"]])
    );
    assert_eq!(
        q(&mut s, "SELECT id FROM t ORDER BY score NULLS FIRST, id"),
        rows(&[&["2"], &["4"], &["1"], &["3"]])
    );
    // Assignment casts and errors.
    let e = fails(&mut s, "INSERT INTO t (v) VALUES ('toolong')");
    assert_eq!(e.code, code::STRING_DATA_RIGHT_TRUNCATION);
    let e = fails(&mut s, "INSERT INTO t (id) VALUES (true)");
    assert_eq!(e.code, code::DATATYPE_MISMATCH);
    assert_eq!(
        e.message,
        "column \"id\" is of type integer but expression is of type boolean"
    );
    assert_eq!(
        fails(&mut s, "INSERT INTO t VALUES (1)").code,
        code::SYNTAX_ERROR
    );
    ok(&mut s, "INSERT INTO t (id, v) VALUES (2.6, 'ab ')");
    assert_eq!(
        q(&mut s, "SELECT v FROM t WHERE id = 3 AND v IS NOT NULL"),
        rows(&[&["ab "]])
    );
    // INSERT ... SELECT doubles the table; the scan does not see its own rows.
    assert_eq!(ok(&mut s, "INSERT INTO t SELECT * FROM t"), "INSERT 0 5");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["10"]]));
}

#[test]
fn aggregates_and_grouping() {
    let db = db();
    let mut s = db.session();
    ok(&mut s, "CREATE TABLE g (k TEXT, n INT, f FLOAT8)");
    ok(
        &mut s,
        "INSERT INTO g VALUES ('a', 1, 0.5), ('a', 2, 1.5), ('b', 5, NULL), ('b', NULL, 2), (NULL, 7, 1)",
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT k, count(*), count(n), sum(n), min(n), max(f), avg(f) FROM g GROUP BY k ORDER BY k"
        ),
        rows(&[
            &["a", "2", "2", "3", "1", "1.5", "1"],
            &["b", "2", "1", "5", "5", "2", "2"],
            &["NULL", "1", "1", "7", "7", "1", "1"],
        ])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT count(*), sum(n), max(k) FROM g WHERE n > 100"
        ),
        rows(&[&["0", "NULL", "NULL"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT k, sum(n) + 1 AS s FROM g GROUP BY 1 HAVING count(*) > 1 ORDER BY s DESC"
        ),
        rows(&[&["b", "6"], &["a", "4"]])
    );
    assert_eq!(
        q(&mut s, "SELECT count(DISTINCT k), count(k) FROM g"),
        rows(&[&["2", "4"]])
    );
    assert_eq!(
        q(&mut s, "SELECT DISTINCT k FROM g ORDER BY k"),
        rows(&[&["a"], &["b"], &["NULL"]])
    );
    assert_eq!(
        q(&mut s, "SELECT k FROM g GROUP BY k ORDER BY max(n) LIMIT 1"),
        rows(&[&["a"]])
    );
    let e = fails(&mut s, "SELECT k, n FROM g GROUP BY k");
    assert_eq!(e.code, code::GROUPING_ERROR);
    assert_eq!(
        e.message,
        "column \"g.n\" must appear in the GROUP BY clause or be used in an aggregate function"
    );
    assert_eq!(
        fails(&mut s, "SELECT k FROM g WHERE count(*) > 1").code,
        code::GROUPING_ERROR
    );
    assert_eq!(
        fails(&mut s, "SELECT sum(count(*)) FROM g").code,
        code::GROUPING_ERROR
    );
    assert_eq!(
        fails(&mut s, "SELECT avg(n) FROM g").code,
        code::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        fails(&mut s, "SELECT DISTINCT k FROM g ORDER BY n").code,
        code::INVALID_COLUMN_REFERENCE
    );
}

#[test]
fn joins() {
    let db = db();
    let mut s = db.session();
    ok(&mut s, "CREATE TABLE a (id INT, x TEXT)");
    ok(&mut s, "CREATE TABLE b (aid INT, y TEXT)");
    ok(
        &mut s,
        "INSERT INTO a VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    );
    ok(
        &mut s,
        "INSERT INTO b VALUES (1, 'p'), (1, 'q'), (3, 'r'), (9, 's'), (NULL, 't')",
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT a.x, b.y FROM a JOIN b ON a.id = b.aid ORDER BY 1, 2"
        ),
        rows(&[&["one", "p"], &["one", "q"], &["three", "r"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT x, y FROM a LEFT JOIN b ON id = aid AND y <> 'q' ORDER BY id, y"
        ),
        rows(&[&["one", "p"], &["two", "NULL"], &["three", "r"]])
    );
    assert_eq!(q(&mut s, "SELECT count(*) FROM a, b"), rows(&[&["15"]]));
    assert_eq!(
        q(
            &mut s,
            "SELECT count(*) FROM a CROSS JOIN b WHERE a.id < b.aid"
        ),
        rows(&[&["5"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT a.x, count(b.y) FROM a LEFT JOIN b ON a.id = b.aid GROUP BY a.x ORDER BY a.x"
        ),
        rows(&[&["one", "2"], &["three", "1"], &["two", "0"]])
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT p.x FROM a p JOIN a q ON p.id = q.id + 1 ORDER BY 1"
        ),
        rows(&[&["three"], &["two"]])
    );
    assert_eq!(
        fails(&mut s, "SELECT id FROM a, a").code,
        code::DUPLICATE_ALIAS
    );
    ok(&mut s, "CREATE TABLE c (id INT)");
    assert_eq!(
        fails(&mut s, "SELECT id FROM a, c").code,
        code::AMBIGUOUS_COLUMN
    );
}

#[test]
fn transaction_blocks() {
    let db = db();
    let mut s = db.session();
    let mut other = db.session();
    ok(&mut s, "CREATE TABLE t (id INT)");
    assert_eq!(ok(&mut s, "BEGIN"), "BEGIN");
    assert_eq!(s.status(), 'T');
    ok(&mut s, "INSERT INTO t VALUES (1)");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["1"]]));
    assert_eq!(q(&mut other, "SELECT count(*) FROM t"), rows(&[&["0"]]));
    assert_eq!(ok(&mut s, "ROLLBACK"), "ROLLBACK");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["0"]]));

    // An error fails the block until ROLLBACK; COMMIT then rolls back.
    ok(&mut s, "BEGIN");
    ok(&mut s, "INSERT INTO t VALUES (1)");
    assert_eq!(fails(&mut s, "SELECT 1/0").code, code::DIVISION_BY_ZERO);
    assert_eq!(s.status(), 'E');
    assert_eq!(
        fails(&mut s, "SELECT 1").code,
        code::IN_FAILED_SQL_TRANSACTION
    );
    assert_eq!(ok(&mut s, "COMMIT"), "ROLLBACK");
    assert_eq!(s.status(), 'I');
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["0"]]));

    // DDL is transactional.
    ok(&mut s, "BEGIN");
    ok(&mut s, "CREATE TABLE u (x INT)");
    ok(&mut s, "INSERT INTO u VALUES (1)");
    ok(&mut s, "DROP TABLE t");
    ok(&mut s, "ROLLBACK");
    assert_eq!(fails(&mut s, "SELECT * FROM u").code, code::UNDEFINED_TABLE);
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["0"]]));

    // Snapshot isolation keeps the first snapshot; read committed does not.
    ok(&mut s, "BEGIN ISOLATION LEVEL REPEATABLE READ");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["0"]]));
    ok(&mut other, "INSERT INTO t VALUES (5)");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["0"]]));
    ok(&mut s, "COMMIT");
    ok(&mut s, "BEGIN");
    ok(&mut s, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["1"]]));
    ok(&mut other, "INSERT INTO t VALUES (6)");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["2"]]));
    assert_eq!(
        fails(&mut s, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").code,
        code::ACTIVE_SQL_TRANSACTION
    );
    ok(&mut s, "ROLLBACK");
    assert_eq!(
        fails(&mut s, "BEGIN ISOLATION LEVEL SERIALIZABLE").code,
        code::FEATURE_NOT_SUPPORTED
    );
}

#[test]
fn ddl_statements() {
    let db = db();
    let mut s = db.session();
    ok(&mut s, "CREATE TABLE t (a INT, b TEXT)");
    assert_eq!(
        fails(&mut s, "CREATE TABLE t (a INT)").code,
        code::DUPLICATE_TABLE
    );
    let r = s.execute("CREATE TABLE IF NOT EXISTS t (a INT)").unwrap();
    assert_eq!(r[0].notices.len(), 1);
    assert_eq!(
        fails(&mut s, "CREATE TABLE d (a INT, a INT)").code,
        code::DUPLICATE_COLUMN
    );
    assert_eq!(
        fails(&mut s, "CREATE TABLE d (a NUMERIC)").code,
        code::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        fails(&mut s, "CREATE TABLE d (a INT NOT NULL)").code,
        code::FEATURE_NOT_SUPPORTED
    );
    ok(&mut s, "INSERT INTO t VALUES (1, 'x'), (2, 'x')");
    assert_eq!(ok(&mut s, "CREATE INDEX ON t (a)"), "CREATE INDEX");
    assert!(db.session().execute("DROP INDEX t_a_idx").is_ok());
    assert_eq!(
        fails(&mut s, "CREATE UNIQUE INDEX t_b ON t (b)").code,
        code::UNIQUE_VIOLATION
    );
    ok(&mut s, "CREATE UNIQUE INDEX t_a ON t (a)");
    let e = fails(&mut s, "INSERT INTO t VALUES (1, 'y')");
    assert_eq!(e.code, code::UNIQUE_VIOLATION);
    assert_eq!(
        e.message,
        "duplicate key value violates unique constraint \"t_a\""
    );
    assert_eq!(
        fails(&mut s, "DROP INDEX nope").code,
        code::UNDEFINED_OBJECT
    );
    assert_eq!(
        fails(&mut s, "DROP TABLE t_a").code,
        code::WRONG_OBJECT_TYPE
    );
    assert_eq!(fails(&mut s, "DROP TABLE sys_class").code, "42501");
    assert_eq!(
        fails(&mut s, "INSERT INTO sys_class VALUES (1)").code,
        "42501"
    );
    assert_eq!(
        q(
            &mut s,
            "SELECT relname FROM sys_class WHERE relname LIKE 't%' ORDER BY 1"
        ),
        rows(&[&["t"], &["t_a"]])
    );
    ok(&mut s, "DROP TABLE t");
    assert_eq!(fails(&mut s, "DROP TABLE t").code, code::UNDEFINED_TABLE);
    ok(&mut s, "DROP TABLE IF EXISTS t, t");
}

#[test]
fn update_and_delete() {
    let db = db();
    let mut s = db.session();
    ok(&mut s, "CREATE TABLE t (id INT, n INT, s TEXT)");
    ok(&mut s, "CREATE UNIQUE INDEX t_id ON t (id)");
    ok(
        &mut s,
        "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')",
    );
    assert_eq!(
        ok(&mut s, "UPDATE t SET n = n + 1 WHERE id >= 2"),
        "UPDATE 2"
    );
    assert_eq!(ok(&mut s, "UPDATE t SET n = n * 2"), "UPDATE 3");
    assert_eq!(
        q(&mut s, "SELECT id, n FROM t ORDER BY id"),
        rows(&[&["1", "20"], &["2", "42"], &["3", "62"]])
    );
    // Assignments see the old row; the unique index sees the new keys.
    assert_eq!(
        ok(&mut s, "UPDATE t AS x SET id = x.id + 10, s = upper(s)"),
        "UPDATE 3"
    );
    assert_eq!(
        q(&mut s, "SELECT id, s FROM t ORDER BY id"),
        rows(&[&["11", "A"], &["12", "B"], &["13", "C"]])
    );
    assert_eq!(
        fails(&mut s, "UPDATE t SET id = 11").code,
        code::UNIQUE_VIOLATION
    );
    assert_eq!(ok(&mut s, "DELETE FROM t WHERE n > 50"), "DELETE 1");
    assert_eq!(
        ok(&mut s, "INSERT INTO t VALUES (13, 0, 'again')"),
        "INSERT 0 1"
    );
    assert_eq!(ok(&mut s, "DELETE FROM t WHERE s IS NULL"), "DELETE 0");
    assert_eq!(
        fails(&mut s, "UPDATE t SET nope = 1").code,
        code::UNDEFINED_COLUMN
    );
    assert_eq!(
        fails(&mut s, "UPDATE t SET n = 'x'").code,
        code::INVALID_TEXT_REPRESENTATION
    );
    assert_eq!(
        fails(&mut s, "UPDATE t SET n = 1, n = 2").code,
        code::SYNTAX_ERROR
    );
    assert_eq!(ok(&mut s, "DELETE FROM t"), "DELETE 3");
    assert_eq!(q(&mut s, "SELECT count(*) FROM t"), rows(&[&["0"]]));

    // Changes inside a block are visible to later statements only.
    ok(&mut s, "INSERT INTO t VALUES (1, 1, 'x')");
    ok(&mut s, "BEGIN");
    ok(&mut s, "UPDATE t SET n = n + 1");
    ok(&mut s, "UPDATE t SET n = n + 1");
    assert_eq!(q(&mut s, "SELECT n FROM t"), rows(&[&["3"]]));
    ok(&mut s, "ROLLBACK");
    assert_eq!(q(&mut s, "SELECT n FROM t"), rows(&[&["1"]]));
}

/// Runs `sql` in `s` on another thread once `s` is blocked behind a lock
/// held by the caller, returning the outcome after `release` runs.
fn blocked<T: Send>(
    s: &mut Session,
    sql: &str,
    release: impl FnOnce() -> T,
) -> (T, Result<Vec<QueryResult>>) {
    std::thread::scope(|sc| {
        let h = sc.spawn(|| s.execute(sql));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!h.is_finished(), "{sql} should wait");
        let t = release();
        (t, h.join().unwrap())
    })
}

#[test]
fn read_committed_rechecks_concurrent_updates() {
    let db = db();
    let mut a = db.session();
    let mut b = db.session();
    ok(&mut a, "CREATE TABLE acct (id INT, bal INT)");
    ok(&mut a, "INSERT INTO acct VALUES (1, 100), (2, 100)");

    // Both increment: the second applies to the first one's result.
    ok(&mut a, "BEGIN");
    ok(&mut a, "UPDATE acct SET bal = bal + 10 WHERE id = 1");
    let (_, r) = blocked(&mut b, "UPDATE acct SET bal = bal * 2 WHERE id = 1", || {
        ok(&mut a, "COMMIT")
    });
    assert_eq!(r.unwrap()[0].tag, "UPDATE 1");
    assert_eq!(
        q(&mut a, "SELECT bal FROM acct WHERE id = 1"),
        rows(&[&["220"]])
    );

    // The WHERE clause is re-evaluated on the new version.
    ok(&mut a, "BEGIN");
    ok(&mut a, "UPDATE acct SET bal = 0 WHERE id = 2");
    let (_, r) = blocked(
        &mut b,
        "UPDATE acct SET bal = bal + 1 WHERE bal = 100",
        || ok(&mut a, "COMMIT"),
    );
    assert_eq!(r.unwrap()[0].tag, "UPDATE 0");

    // A concurrent delete makes the row disappear.
    ok(&mut a, "BEGIN");
    ok(&mut a, "DELETE FROM acct WHERE id = 2");
    let (_, r) = blocked(&mut b, "DELETE FROM acct WHERE id = 2", || {
        ok(&mut a, "COMMIT")
    });
    assert_eq!(r.unwrap()[0].tag, "DELETE 0");

    // A rolled-back change lets the waiter proceed on the original row.
    ok(&mut a, "BEGIN");
    ok(&mut a, "UPDATE acct SET bal = -1 WHERE id = 1");
    let (_, r) = blocked(&mut b, "UPDATE acct SET bal = bal + 1 WHERE id = 1", || {
        ok(&mut a, "ROLLBACK")
    });
    assert_eq!(r.unwrap()[0].tag, "UPDATE 1");
    assert_eq!(
        q(&mut a, "SELECT id, bal FROM acct"),
        rows(&[&["1", "221"]])
    );
}

#[test]
fn snapshot_isolation_reports_serialization_failures() {
    let db = db();
    let mut a = db.session();
    let mut b = db.session();
    ok(&mut a, "CREATE TABLE t (id INT, v INT)");
    ok(&mut a, "INSERT INTO t VALUES (1, 0)");

    ok(&mut b, "BEGIN ISOLATION LEVEL REPEATABLE READ");
    assert_eq!(q(&mut b, "SELECT v FROM t"), rows(&[&["0"]]));
    ok(&mut a, "UPDATE t SET v = 1");
    let e = fails(&mut b, "UPDATE t SET v = 2");
    assert_eq!(e.code, code::SERIALIZATION_FAILURE);
    assert_eq!(b.status(), 'E');
    ok(&mut b, "ROLLBACK");

    // Waiting on a transaction that then commits also fails.
    ok(&mut b, "BEGIN ISOLATION LEVEL REPEATABLE READ");
    q(&mut b, "SELECT v FROM t");
    ok(&mut a, "BEGIN");
    ok(&mut a, "UPDATE t SET v = 3");
    let (_, r) = blocked(&mut b, "DELETE FROM t", || ok(&mut a, "COMMIT"));
    assert_eq!(r.unwrap_err().code, code::SERIALIZATION_FAILURE);
    ok(&mut b, "ROLLBACK");
    assert_eq!(q(&mut b, "SELECT v FROM t"), rows(&[&["3"]]));
}

#[test]
fn deadlocks_are_detected() {
    let db = db();
    let mut a = db.session();
    let mut b = db.session();
    ok(&mut a, "CREATE TABLE t (id INT)");
    ok(&mut a, "INSERT INTO t VALUES (1), (2)");
    ok(&mut a, "BEGIN");
    ok(&mut b, "BEGIN");
    ok(&mut a, "UPDATE t SET id = 10 WHERE id = 1");
    ok(&mut b, "UPDATE t SET id = 20 WHERE id = 2");
    let (ra, rb) = std::thread::scope(|sc| {
        let h = sc.spawn(|| b.execute("UPDATE t SET id = 21 WHERE id = 1"));
        std::thread::sleep(Duration::from_millis(50));
        let ra = a.execute("UPDATE t SET id = 11 WHERE id = 2");
        if ra.is_err() {
            ok(&mut a, "ROLLBACK");
        } else {
            ok(&mut a, "COMMIT");
        }
        (ra, h.join().unwrap())
    });
    let errs: Vec<_> = [&ra, &rb].iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(errs.len(), 1, "{ra:?} {rb:?}");
    assert_eq!(errs[0].code, code::DEADLOCK_DETECTED);
}
