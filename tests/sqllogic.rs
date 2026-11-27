//! sqllogictest files (tests/slt/*.slt) run against mydb on the O_DIRECT
//! backend. Expected results come from a real PostgreSQL (DESIGN r11):
//!
//! ```text
//! MYDB_PSQL=~/pg18/bin/psql PGHOST=/tmp PGPORT=54321 PGUSER=postgres \
//!     cargo test --test sqllogic -- --ignored complete_with_postgres
//! ```
//!
//! rewrites each file's query results with PostgreSQL's (C collation; see
//! docs/SETUP.md) and checks that PostgreSQL agrees on which statements
//! fail.
//!
//! Format (a subset of sqllogictest-rs):
//!
//! ```text
//! statement ok
//! <sql>
//!
//! statement error <substring of the message>
//! <sql>
//!
//! query <one letter per column> [rowsort]
//! <sql>
//! ----
//! <row: values separated by single spaces; NULL, (empty) for ''>
//! ```

use std::path::{Path, PathBuf};
use std::process::Command;
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

#[derive(Debug, Clone, PartialEq)]
enum Kind {
    StatementOk,
    StatementError(String),
    Query { types: String, rowsort: bool },
}

#[derive(Debug, Clone)]
struct Record {
    line: usize,
    kind: Kind,
    sql: String,
    expected: Vec<String>,
}

fn parse(text: &str) -> Vec<Record> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let l = lines[i].trim_end();
        if l.is_empty() || l.starts_with('#') {
            i += 1;
            continue;
        }
        let line = i + 1;
        let kind = if l == "statement ok" {
            Kind::StatementOk
        } else if let Some(m) = l.strip_prefix("statement error") {
            Kind::StatementError(m.trim().to_owned())
        } else if let Some(rest) = l.strip_prefix("query ") {
            let mut p = rest.split_whitespace();
            Kind::Query {
                types: p.next().unwrap_or("").to_owned(),
                rowsort: p.next() == Some("rowsort"),
            }
        } else {
            panic!("line {line}: unknown record {l:?}");
        };
        i += 1;
        let mut sql = Vec::new();
        while i < lines.len() && !lines[i].trim().is_empty() && lines[i] != "----" {
            sql.push(lines[i]);
            i += 1;
        }
        let mut expected = Vec::new();
        if i < lines.len() && lines[i] == "----" {
            i += 1;
            while i < lines.len() && !lines[i].trim().is_empty() {
                expected.push(lines[i].to_owned());
                i += 1;
            }
        }
        out.push(Record {
            line,
            kind,
            sql: sql.join("\n"),
            expected,
        });
    }
    out
}

fn render(records: &[Record]) -> String {
    let mut s = String::new();
    for r in records {
        match &r.kind {
            Kind::StatementOk => s.push_str("statement ok\n"),
            Kind::StatementError(m) if m.is_empty() => s.push_str("statement error\n"),
            Kind::StatementError(m) => s.push_str(&format!("statement error {m}\n")),
            Kind::Query { types, rowsort } => {
                s.push_str(&format!(
                    "query {types}{}\n",
                    if *rowsort { " rowsort" } else { "" }
                ));
            }
        }
        s.push_str(&r.sql);
        s.push('\n');
        if let Kind::Query { .. } = r.kind {
            s.push_str("----\n");
            for e in &r.expected {
                s.push_str(e);
                s.push('\n');
            }
        }
        s.push('\n');
    }
    s
}

fn cell(v: Option<String>) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(s) if s.is_empty() => "(empty)".to_owned(),
        Some(s) => s,
    }
}

fn slt_files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "slt"))
        .collect();
    files.sort();
    files
}

fn database(dir: &Path) -> Arc<Database> {
    let cfg = WalConfig {
        timeline: 1,
        segment_size: 1 << 20,
    };
    let backend = || Arc::new(LocalDirBackend::open(dir).unwrap());
    mydb::catalog::initdb(backend(), Arc::new(SingleModeFlush::new(FENCE)), cfg).unwrap();
    let engine = Engine::start(backend(), Arc::new(SingleModeFlush::new(FENCE)), cfg, 256).unwrap();
    Database::new(
        Arc::new(engine),
        WaitPolicy {
            lock_timeout: Some(Duration::from_secs(10)),
            deadlock_timeout: Duration::from_millis(100),
        },
    )
    .unwrap()
}

fn run_file(path: &Path) -> Vec<String> {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let db = database(dir.path());
    let mut s = db.session();
    let name = path.file_name().unwrap().to_string_lossy().into_owned();
    let mut failures = Vec::new();
    for r in parse(&std::fs::read_to_string(path).unwrap()) {
        let at = format!("{name}:{}", r.line);
        let res = s.execute(&r.sql);
        match (&r.kind, res) {
            (Kind::StatementOk, Ok(_)) => {}
            (Kind::StatementOk, Err(e)) => {
                failures.push(format!("{at}: {e} ({})\n  {}", e.code, r.sql))
            }
            (Kind::StatementError(m), Err(e)) => {
                if !e.message.contains(m.as_str()) {
                    failures.push(format!(
                        "{at}: error {:?} does not contain {m:?}",
                        e.message
                    ));
                }
            }
            (Kind::StatementError(_), Ok(_)) => {
                failures.push(format!("{at}: expected an error\n  {}", r.sql))
            }
            (Kind::Query { .. }, Err(e)) => {
                failures.push(format!("{at}: {e} ({})\n  {}", e.code, r.sql))
            }
            (Kind::Query { types, rowsort }, Ok(results)) => {
                let Some(last) = results.last() else {
                    failures.push(format!("{at}: no result"));
                    continue;
                };
                if last.columns.len() != types.len() {
                    failures.push(format!(
                        "{at}: {} columns, type string {types:?}",
                        last.columns.len()
                    ));
                }
                let mut got: Vec<String> = last
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| cell(format_value(v)))
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect();
                let mut want = r.expected.clone();
                if *rowsort {
                    got.sort();
                    want.sort();
                }
                if got != want {
                    failures.push(format!(
                        "{at}: {}\n  expected:\n    {}\n  got:\n    {}",
                        r.sql,
                        want.join("\n    "),
                        got.join("\n    ")
                    ));
                }
            }
        }
    }
    failures
}

#[test]
fn sqllogictest_files() {
    let files = slt_files();
    assert!(!files.is_empty());
    let mut failures = Vec::new();
    for f in &files {
        failures.extend(run_file(f));
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Output of one record when run by psql.
#[derive(Debug)]
enum PgOutcome {
    Ok(Vec<String>),
    Error(String),
}

/// Runs all records of a file in one psql session (in a fresh database),
/// separating records with markers.
fn run_postgres(psql: &str, records: &[Record]) -> Vec<PgOutcome> {
    let db = format!("mydb_slt_{}", std::process::id());
    let admin = |sql: &str| {
        let st = Command::new(psql)
            .args(["-X", "-q", "-d", "postgres", "-c", sql])
            .status()
            .expect("run psql");
        assert!(st.success(), "{sql}");
    };
    admin(&format!("DROP DATABASE IF EXISTS {db}"));
    admin(&format!(
        "CREATE DATABASE {db} TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C'"
    ));
    let mut script = String::from("\\set VERBOSITY terse\n");
    for (i, r) in records.iter().enumerate() {
        script.push_str(&format!("\\echo @@REC {i}\n{}\n;\n", r.sql));
    }
    script.push_str("\\echo @@END\n");
    let out = Command::new(psql)
        .args([
            "-X",
            "-q",
            "-A",
            "-t",
            "-F",
            "\t",
            "-P",
            "null=\u{1}NULL",
            "-d",
            &db,
            "-f",
            "-",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin.take().unwrap().write_all(script.as_bytes())?;
            c.wait_with_output()
        })
        .expect("run psql");
    admin(&format!("DROP DATABASE {db}"));
    let stdout = String::from_utf8(out.stdout).unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    // Errors carry the script line; map them to records by line number.
    let mut rec_lines = Vec::new();
    let mut line = 2; // after \set
    for r in records {
        rec_lines.push(line);
        line += 1 + r.sql.lines().count() + 1;
    }
    let mut errors: Vec<Option<String>> = vec![None; records.len()];
    for l in stderr.lines() {
        // psql:<stdin>:LINE: ERROR:  message
        let Some(rest) = l.strip_prefix("psql:<stdin>:") else {
            continue;
        };
        let Some((n, msg)) = rest.split_once(": ") else {
            continue;
        };
        let Some(msg) = msg.strip_prefix("ERROR:  ") else {
            continue;
        };
        let n: usize = n.parse().unwrap();
        let idx = rec_lines.iter().rposition(|&s| s <= n).unwrap();
        errors[idx].get_or_insert_with(|| msg.to_owned());
    }
    let mut outcomes = Vec::new();
    let mut chunks: Vec<Vec<String>> = Vec::new();
    for l in stdout.lines() {
        if l.starts_with("@@REC ") {
            chunks.push(Vec::new());
        } else if l == "@@END" {
            break;
        } else if let Some(c) = chunks.last_mut() {
            c.push(l.to_owned());
        }
    }
    for (i, c) in chunks.into_iter().enumerate() {
        outcomes.push(match errors[i].take() {
            Some(e) => PgOutcome::Error(e),
            None => PgOutcome::Ok(
                c.iter()
                    .map(|row| {
                        row.split('\t')
                            .map(|v| match v {
                                "\u{1}NULL" => "NULL".to_owned(),
                                "" => "(empty)".to_owned(),
                                v => v.to_owned(),
                            })
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect(),
            ),
        });
    }
    assert_eq!(
        outcomes.len(),
        records.len(),
        "psql output:\n{stdout}\n{stderr}"
    );
    outcomes
}

#[test]
#[ignore = "needs PostgreSQL; regenerates expected results"]
fn complete_with_postgres() {
    let psql = std::env::var("MYDB_PSQL").expect("set MYDB_PSQL to a psql binary");
    let mut problems = Vec::new();
    for f in slt_files() {
        let name = f.file_name().unwrap().to_string_lossy().into_owned();
        let mut records = parse(&std::fs::read_to_string(&f).unwrap());
        let outcomes = run_postgres(&psql, &records);
        for (r, o) in records.iter_mut().zip(outcomes) {
            let at = format!("{name}:{}", r.line);
            match (&r.kind, o) {
                (Kind::StatementOk, PgOutcome::Error(e)) => {
                    problems.push(format!("{at}: PostgreSQL fails: {e}"));
                }
                (Kind::StatementError(m), PgOutcome::Error(e)) => {
                    if !e.contains(m.as_str()) {
                        problems.push(format!("{at}: PostgreSQL's error {e:?} lacks {m:?}"));
                    }
                }
                (Kind::StatementError(_), PgOutcome::Ok(_)) => {
                    problems.push(format!("{at}: PostgreSQL succeeds"));
                }
                (Kind::Query { .. }, PgOutcome::Error(e)) => {
                    problems.push(format!("{at}: PostgreSQL fails: {e}"));
                }
                (Kind::Query { rowsort, .. }, PgOutcome::Ok(mut rows)) => {
                    if *rowsort {
                        rows.sort();
                    }
                    r.expected = rows;
                }
                (Kind::StatementOk, PgOutcome::Ok(_)) => {}
            }
        }
        std::fs::write(&f, render(&records)).unwrap();
    }
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}
