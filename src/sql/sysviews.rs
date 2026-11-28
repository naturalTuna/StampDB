//! System views computed when queried (no stored rows): the statistics
//! views of PostgreSQL's pg_catalog. Only pg_stat_user_tables so far
//! (DESIGN r14), with a subset of its columns.

use super::error::Result;
use crate::catalog::Catalog;
use crate::stats::StatsCollector;
use crate::txn::Transaction;
use crate::types::{DataType, Value};
use crate::wal::recovery::FIRST_OID;

/// A view's columns and current rows.
pub struct ViewRows {
    pub columns: Vec<(&'static str, DataType)>,
    pub rows: Vec<Vec<Value>>,
}

/// The rows of system view `name`, or `None` if there is no such view.
pub fn view(
    name: &str,
    cat: &Catalog,
    txn: &mut Transaction,
    stats: &StatsCollector,
) -> Result<Option<ViewRows>> {
    match name {
        "pg_stat_user_tables" => Ok(Some(stat_user_tables(cat, txn, stats)?)),
        _ => Ok(None),
    }
}

fn stat_user_tables(
    cat: &Catalog,
    txn: &mut Transaction,
    stats: &StatsCollector,
) -> Result<ViewRows> {
    use DataType::{Int8, Text, Timestamp};
    let columns = vec![
        ("relid", Int8),
        ("relname", Text),
        ("n_tup_ins", Int8),
        ("n_tup_upd", Int8),
        ("n_tup_del", Int8),
        ("n_live_tup", Int8),
        ("n_dead_tup", Int8),
        ("last_vacuum", Timestamp),
        ("last_autovacuum", Timestamp),
        ("vacuum_count", Int8),
        ("autovacuum_count", Int8),
    ];
    let count = |n: u64| Value::Int8(i64::try_from(n).unwrap_or(i64::MAX));
    let time = |t: Option<i64>| t.map_or(Value::Null, Value::Timestamp);
    let mut rows = Vec::new();
    for t in cat.tables(txn)? {
        if t.oid < FIRST_OID {
            continue;
        }
        let s = stats.get(t.oid);
        rows.push(vec![
            Value::Int8(i64::from(t.oid)),
            Value::Text(t.name),
            count(s.n_tup_ins),
            count(s.n_tup_upd),
            count(s.n_tup_del),
            count(s.n_live_tup),
            count(s.n_dead_tup),
            time(s.last_vacuum),
            time(s.last_autovacuum),
            count(s.vacuum_count),
            count(s.autovacuum_count),
        ]);
    }
    Ok(ViewRows { columns, rows })
}
