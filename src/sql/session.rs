//! Sessions: statement execution and transaction blocks (DESIGN r11).
//!
//! Outside a block every statement runs in its own transaction. After
//! BEGIN, statements share one transaction; an error puts the block in the
//! failed state where everything but COMMIT/ROLLBACK is refused (25P02).

use std::sync::Arc;

use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::binder::{Binder, Bound, ConstraintDef, Kind, Rules, Utility, classify, parse_expr};
use super::error::{Result, SqlError, code};
use super::exec::{self, ExecEnv};
use super::expr::{Expr, is_true};
use super::plan::{OutCol, Plan};
use super::planner;
use super::writer::Writer;
use crate::access::heap::{HeapFile, TmResult};
use crate::access::tuple::deform_tuple;
use crate::access::visibility::Me;
use crate::catalog::{Catalog, Column, ConstraintKind, NewConstraint, RelKind, RelationInfo};
use crate::lock::{LockManager, WaitPolicy};
use crate::stats::StatsCollector;
use crate::txn::{Isolation, Transaction, TransactionManager};
use crate::types::Value;
use crate::wal::Engine;
use crate::wal::recovery::FIRST_OID;

/// Shared state of a running database: catalogs and the lock manager.
#[derive(Debug)]
pub struct Database {
    catalog: Catalog,
    locks: Arc<LockManager>,
    policy: WaitPolicy,
    stats: Arc<StatsCollector>,
}

impl Database {
    pub fn new(engine: Arc<Engine>, policy: WaitPolicy) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            catalog: Catalog::open(engine)?,
            locks: Arc::new(LockManager::new()),
            policy,
            stats: Arc::new(StatsCollector::new()),
        }))
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn engine(&self) -> &Arc<Engine> {
        self.catalog.engine()
    }

    /// Table statistics (DESIGN r14).
    pub fn stats(&self) -> &Arc<StatsCollector> {
        &self.stats
    }

    pub fn session(self: &Arc<Self>) -> Session {
        Session {
            db: self.clone(),
            state: State::Idle,
            default_isolation: Isolation::ReadCommitted,
        }
    }

    fn begin(&self, isolation: Isolation) -> Transaction {
        let tm: Arc<dyn TransactionManager> = self.engine().txn().clone();
        Transaction::begin(tm, self.locks.clone(), isolation, self.policy)
            .with_stats(self.stats.clone())
    }
}

/// Result of one statement.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Result columns (empty for statements returning no rows).
    pub columns: Vec<OutCol>,
    pub rows: Vec<Vec<Value>>,
    /// Command tag (CommandComplete), e.g. `SELECT 2`, `INSERT 0 1`.
    pub tag: String,
    /// NOTICE/WARNING messages raised by the statement.
    pub notices: Vec<String>,
}

impl QueryResult {
    fn tag(tag: impl Into<String>) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            tag: tag.into(),
            notices: Vec::new(),
        }
    }

    fn with_notice(mut self, n: impl Into<String>) -> Self {
        self.notices.push(n.into());
        self
    }
}

#[derive(Debug)]
enum State {
    Idle,
    Block(Box<Transaction>),
    /// A block whose transaction failed (and was already aborted, which
    /// released its locks); waits for COMMIT or ROLLBACK.
    Failed,
}

/// One client's session.
#[derive(Debug)]
pub struct Session {
    db: Arc<Database>,
    state: State,
    default_isolation: Isolation,
}

/// Parses SQL text (PostgreSQL dialect).
pub fn parse(sql: &str) -> Result<Vec<ast::Statement>> {
    Ok(Parser::parse_sql(&PostgreSqlDialect {}, sql)?)
}

impl Session {
    /// Whether a transaction block is open (`T`) or failed (`E`), else
    /// idle (`I`) — ReadyForQuery's status byte.
    pub fn status(&self) -> char {
        match self.state {
            State::Idle => 'I',
            State::Block(_) => 'T',
            State::Failed => 'E',
        }
    }

    /// Runs every statement in `sql`, stopping at the first error.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        let stmts = match parse(sql) {
            Ok(s) => s,
            Err(e) => {
                self.fail()?;
                return Err(e);
            }
        };
        stmts.iter().map(|s| self.run(s)).collect()
    }

    /// The optimized plan of one query or DML statement, bound in a
    /// throwaway transaction (for EXPLAIN and tests).
    pub fn plan(&mut self, sql: &str) -> Result<Plan> {
        let stmts = parse(sql)?;
        let [stmt] = stmts.as_slice() else {
            return Err(SqlError::new(code::SYNTAX_ERROR, "expected one statement"));
        };
        let mut t = self.db.begin(self.default_isolation);
        t.statement_snapshot();
        let plan = match Binder::new(&self.db.catalog, &mut t).bind(stmt) {
            Ok(Bound::Query { plan, .. }) => self.optimize(&mut t, plan),
            Ok(Bound::Insert { source, .. }) => self.optimize(&mut t, source),
            Ok(Bound::Update { scan, .. } | Bound::Delete { scan, .. }) => {
                self.optimize(&mut t, scan)
            }
            Err(e) => Err(e),
        };
        t.abort()?;
        plan
    }

    /// Runs one statement.
    pub fn run(&mut self, stmt: &ast::Statement) -> Result<QueryResult> {
        let kind = match classify(stmt) {
            Ok(k) => k,
            Err(e) => {
                self.fail()?;
                return Err(e);
            }
        };
        let state = std::mem::replace(&mut self.state, State::Idle);
        match (state, kind) {
            (State::Failed, Kind::Utility(Utility::Commit | Utility::Rollback)) => {
                Ok(QueryResult::tag("ROLLBACK"))
            }
            (State::Failed, _) => {
                self.state = State::Failed;
                Err(SqlError::new(
                    code::IN_FAILED_SQL_TRANSACTION,
                    "current transaction is aborted, commands ignored until end of transaction block",
                ))
            }
            (State::Idle, Kind::Utility(Utility::Begin(iso))) => {
                let t = self.db.begin(iso.unwrap_or(self.default_isolation));
                self.state = State::Block(Box::new(t));
                Ok(QueryResult::tag("BEGIN"))
            }
            (State::Block(t), Kind::Utility(Utility::Begin(_))) => {
                self.state = State::Block(t);
                Ok(QueryResult::tag("BEGIN")
                    .with_notice("WARNING: there is already a transaction in progress"))
            }
            (State::Block(t), Kind::Utility(Utility::Commit)) => {
                t.commit()?;
                Ok(QueryResult::tag("COMMIT"))
            }
            (State::Idle, Kind::Utility(Utility::Commit)) => Ok(QueryResult::tag("COMMIT")
                .with_notice("WARNING: there is no transaction in progress")),
            (State::Block(t), Kind::Utility(Utility::Rollback)) => {
                t.abort()?;
                Ok(QueryResult::tag("ROLLBACK"))
            }
            (State::Idle, Kind::Utility(Utility::Rollback)) => Ok(QueryResult::tag("ROLLBACK")
                .with_notice("WARNING: there is no transaction in progress")),
            (State::Block(mut t), Kind::Utility(Utility::SetTransaction(iso))) => {
                let ok = t.set_isolation(iso);
                self.state = State::Block(t);
                if ok {
                    Ok(QueryResult::tag("SET"))
                } else {
                    self.fail()?;
                    Err(SqlError::new(
                        code::ACTIVE_SQL_TRANSACTION,
                        "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                    ))
                }
            }
            (State::Idle, Kind::Utility(Utility::SetTransaction(_))) => Ok(QueryResult::tag("SET")
                .with_notice("WARNING: SET TRANSACTION can only be used in transaction blocks")),
            (State::Block(mut t), kind) => {
                let r = self.run_in(&mut t, &kind);
                t.next_command();
                match &r {
                    Ok(_) => self.state = State::Block(t),
                    Err(_) => {
                        t.abort()?;
                        self.state = State::Failed;
                    }
                }
                r
            }
            (State::Idle, kind) => {
                let mut t = self.db.begin(self.default_isolation);
                match self.run_in(&mut t, &kind) {
                    Ok(r) => {
                        t.commit()?;
                        Ok(r)
                    }
                    Err(e) => {
                        t.abort()?;
                        Err(e)
                    }
                }
            }
        }
    }

    /// Aborts an open block's transaction, leaving the block failed.
    fn fail(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Block(t) => {
                self.state = State::Failed;
                t.abort()?;
            }
            other => self.state = other,
        }
        Ok(())
    }

    fn run_in(&self, txn: &mut Transaction, kind: &Kind<'_>) -> Result<QueryResult> {
        txn.statement_snapshot();
        match kind {
            Kind::Utility(u) => self.utility(txn, u),
            Kind::Plannable(stmt) => {
                let bound = Binder::new(&self.db.catalog, txn).bind(stmt)?;
                self.execute_bound(txn, bound)
            }
        }
    }

    fn utility(&self, txn: &mut Transaction, u: &Utility) -> Result<QueryResult> {
        let cat = &self.db.catalog;
        match u {
            Utility::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => {
                if *if_not_exists && cat.lookup_relation(txn, name)?.is_some() {
                    return Ok(QueryResult::tag("CREATE TABLE").with_notice(format!(
                        "NOTICE: relation \"{name}\" already exists, skipping"
                    )));
                }
                let constraints = self.name_constraints(txn, name, columns, constraints)?;
                cat.create_table_with(txn, name, columns, &constraints)?;
                Ok(QueryResult::tag("CREATE TABLE"))
            }
            Utility::DropTable { names, if_exists } => {
                let mut r = QueryResult::tag("DROP TABLE");
                for name in names {
                    match cat.lookup_relation(txn, name)? {
                        None if *if_exists => {
                            r = r.with_notice(format!(
                                "NOTICE: table \"{name}\" does not exist, skipping"
                            ));
                        }
                        None => {
                            return Err(SqlError::new(
                                code::UNDEFINED_TABLE,
                                format!("table \"{name}\" does not exist"),
                            ));
                        }
                        Some(info) => {
                            check_user_relation(info.oid, name)?;
                            if info.kind != RelKind::Table {
                                return Err(SqlError::new(
                                    code::WRONG_OBJECT_TYPE,
                                    format!("\"{name}\" is not a table"),
                                ));
                            }
                            cat.drop_table(txn, name)?;
                        }
                    }
                }
                Ok(r)
            }
            Utility::CreateIndex {
                name,
                table,
                columns,
                unique,
                if_not_exists,
            } => {
                if let Some(t) = cat.lookup_relation(txn, table)? {
                    check_user_relation(t.oid, table)?;
                }
                let name = match name {
                    Some(n) => {
                        if *if_not_exists && cat.lookup_relation(txn, n)?.is_some() {
                            return Ok(QueryResult::tag("CREATE INDEX").with_notice(format!(
                                "NOTICE: relation \"{n}\" already exists, skipping"
                            )));
                        }
                        n.clone()
                    }
                    None => {
                        // PostgreSQL's ChooseRelationName: table_cols_idx[N].
                        let base = format!("{table}_{}_idx", columns.join("_"));
                        let mut n = base.clone();
                        let mut k = 0;
                        while cat.lookup_relation(txn, &n)?.is_some() {
                            k += 1;
                            n = format!("{base}{k}");
                        }
                        n
                    }
                };
                let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
                cat.create_index(txn, &name, table, &cols, *unique)?;
                Ok(QueryResult::tag("CREATE INDEX"))
            }
            Utility::DropIndex { names, if_exists } => {
                let mut r = QueryResult::tag("DROP INDEX");
                for name in names {
                    match cat.lookup_relation(txn, name)? {
                        Some(info) if info.kind == RelKind::Index => {
                            check_user_relation(info.oid, name)?;
                            cat.drop_index(txn, name)?;
                        }
                        Some(_) => {
                            return Err(SqlError::new(
                                code::WRONG_OBJECT_TYPE,
                                format!("\"{name}\" is not an index"),
                            ));
                        }
                        None if *if_exists => {
                            r = r.with_notice(format!(
                                "NOTICE: index \"{name}\" does not exist, skipping"
                            ));
                        }
                        None => {
                            return Err(SqlError::new(
                                code::UNDEFINED_OBJECT,
                                format!("index \"{name}\" does not exist"),
                            ));
                        }
                    }
                }
                Ok(r)
            }
            Utility::Begin(_)
            | Utility::Commit
            | Utility::Rollback
            | Utility::SetTransaction(_) => {
                Err(SqlError::internal("transaction control inside a statement"))
            }
        }
    }

    /// Validates DEFAULT and CHECK expressions and names the constraints
    /// the way PostgreSQL does: t_pkey, t_a_b_key, t_a_check (one
    /// referenced column) or t_check, with numbers appended on collisions.
    fn name_constraints(
        &self,
        txn: &mut Transaction,
        table: &str,
        columns: &[Column],
        defs: &[ConstraintDef],
    ) -> Result<Vec<NewConstraint>> {
        let cat = &self.db.catalog;
        for c in columns {
            if let Some(text) = &c.default {
                Binder::new(cat, txn).bind_default(c, &parse_expr(text)?)?;
            }
        }
        // Explicit names are reserved first.
        let mut taken: Vec<String> = defs
            .iter()
            .filter_map(|d| match d {
                ConstraintDef::PrimaryKey { name, .. }
                | ConstraintDef::Unique { name, .. }
                | ConstraintDef::Check { name, .. }
                | ConstraintDef::ForeignKey { name, .. } => name.clone(),
            })
            .collect();
        for (i, n) in taken.iter().enumerate() {
            if taken[..i].contains(n) {
                return Err(SqlError::new(
                    code::DUPLICATE_OBJECT,
                    format!("check constraint \"{n}\" already exists"),
                ));
            }
        }
        let mut out = Vec::new();
        for d in defs {
            let (name, kind) = match d {
                ConstraintDef::PrimaryKey { name, columns } => {
                    (name.clone(), ConstraintKind::PrimaryKey(columns.clone()))
                }
                ConstraintDef::Unique { name, columns } => {
                    (name.clone(), ConstraintKind::Unique(columns.clone()))
                }
                ConstraintDef::ForeignKey {
                    name,
                    columns,
                    parent,
                    parent_columns,
                    on_delete,
                    on_update,
                } => {
                    if parent != table
                        && let Some(p) = cat.lookup_relation(txn, parent)?
                    {
                        check_user_relation(p.oid, parent)?;
                    }
                    let base = format!("{table}_{}_fkey", columns.join("_"));
                    let name = name.clone().unwrap_or_else(|| {
                        let mut n = base.clone();
                        let mut k = 0;
                        while taken.contains(&n) {
                            k += 1;
                            n = format!("{base}{k}");
                        }
                        taken.push(n.clone());
                        n
                    });
                    out.push(NewConstraint {
                        name,
                        kind: ConstraintKind::ForeignKey {
                            columns: columns.clone(),
                            parent: parent.clone(),
                            parent_columns: parent_columns.clone(),
                            on_delete: *on_delete,
                            on_update: *on_update,
                        },
                    });
                    continue;
                }
                ConstraintDef::Check { name, expr } => {
                    let bound = Binder::new(cat, txn).bind_check(table, columns, expr)?;
                    let mut used = Vec::new();
                    bound.walk(&mut |e| {
                        if let Expr::Column(c) = e
                            && !used.contains(c)
                        {
                            used.push(*c);
                        }
                    });
                    let base = match used.as_slice() {
                        [c] => format!("{table}_{}_check", columns[*c].name),
                        _ => format!("{table}_check"),
                    };
                    let name = name.clone().unwrap_or_else(|| {
                        let mut n = base.clone();
                        let mut k = 0;
                        while taken.contains(&n) {
                            k += 1;
                            n = format!("{base}{k}");
                        }
                        taken.push(n.clone());
                        n
                    });
                    out.push(NewConstraint {
                        name,
                        kind: ConstraintKind::Check(expr.to_string()),
                    });
                    continue;
                }
            };
            let name = match name {
                Some(n) => n,
                None => {
                    let base = match &kind {
                        ConstraintKind::PrimaryKey(_) => format!("{table}_pkey"),
                        ConstraintKind::Unique(cols) => format!("{table}_{}_key", cols.join("_")),
                        ConstraintKind::Check(_) => format!("{table}_check"),
                        ConstraintKind::ForeignKey { columns, .. } => {
                            format!("{table}_{}_fkey", columns.join("_"))
                        }
                    };
                    let mut n = cat.choose_relation_name(txn, &base)?;
                    let mut k = 0;
                    while taken.contains(&n) {
                        k += 1;
                        n = cat.choose_relation_name(txn, &format!("{base}{k}"))?;
                    }
                    taken.push(n.clone());
                    n
                }
            };
            out.push(NewConstraint { name, kind });
        }
        Ok(out)
    }

    fn optimize(&self, txn: &mut Transaction, plan: Plan) -> Result<Plan> {
        let cat = &self.db.catalog;
        planner::optimize(plan, &mut |t| Ok(cat.indexes_of(txn, t.oid)?))
    }

    fn env(&self, txn: &mut Transaction) -> Arc<ExecEnv> {
        let tm: Arc<dyn TransactionManager> = self.db.engine().txn().clone();
        Arc::new(ExecEnv {
            pool: self.db.engine().pool().clone(),
            tm,
            snap: txn.snapshot(),
            me: Me {
                xid: txn.xid(),
                cid: txn.command_id(),
            },
        })
    }

    fn execute_bound(&self, txn: &mut Transaction, bound: Bound) -> Result<QueryResult> {
        match bound {
            Bound::Query { plan, columns } => {
                let plan = self.optimize(txn, plan)?;
                let env = self.env(txn);
                let rows = exec::run(&plan, &env)?;
                Ok(QueryResult {
                    tag: format!("SELECT {}", rows.len()),
                    columns,
                    rows,
                    notices: Vec::new(),
                })
            }
            Bound::Insert {
                table,
                source,
                rules,
            } => {
                let plan = self.optimize(txn, source)?;
                let env = self.env(txn);
                let rows = exec::run(&plan, &env)?;
                let mut w = Writer::new(&self.db.catalog);
                let tw = w.table(txn, &table, Some(rules))?;
                for row in &rows {
                    w.insert(txn, &tw, row)?;
                }
                Ok(QueryResult::tag(format!("INSERT 0 {}", rows.len())))
            }
            Bound::Update {
                table,
                scan,
                assignments,
                rules,
            } => {
                let n = self.modify(txn, &table, scan, rules, Some(&assignments))?;
                Ok(QueryResult::tag(format!("UPDATE {n}")))
            }
            Bound::Delete { table, scan, rules } => {
                let n = self.modify(txn, &table, scan, rules, None)?;
                Ok(QueryResult::tag(format!("DELETE {n}")))
            }
        }
    }

    /// UPDATE (with `assignments`) or DELETE of the rows `scan` finds.
    ///
    /// A row changed by a concurrent transaction is waited for; under Read
    /// Committed, if that transaction committed an update, the newest
    /// version is fetched and the WHERE clause re-evaluated on it before
    /// retrying (EvalPlanQual). Under Snapshot Isolation the heap returns a
    /// serialization failure instead.
    fn modify(
        &self,
        txn: &mut Transaction,
        table: &RelationInfo,
        scan: Plan,
        rules: Rules,
        assignments: Option<&[(usize, Expr)]>,
    ) -> Result<u64> {
        let plan = self.optimize(txn, scan)?;
        let filter = match &plan {
            Plan::Scan { filter, .. } | Plan::IndexScan { filter, .. } => filter.clone(),
            _ => return Err(SqlError::internal("DML plan is not a scan")),
        };
        let env = self.env(txn);
        let mut rows = exec::table_scan(&plan, &env)?;
        let heap = HeapFile::open(table.rel, self.db.engine().pool().clone())?;
        let mut w = Writer::new(&self.db.catalog);
        let tw = w.table(txn, table, Some(rules))?;
        let desc = table.desc();
        let mut count = 0;
        while let Some((first_tid, first_row)) = rows.next_with_tid()? {
            let (mut tid, mut row) = (first_tid, first_row);
            loop {
                let result = match assignments {
                    Some(set) => {
                        let mut new_row = row.clone();
                        for (col, e) in set {
                            new_row[*col] = e.eval(&row)?;
                        }
                        w.update(txn, &tw, tid, &row, &new_row)?
                    }
                    None => w.delete(txn, &tw, tid, &row)?,
                };
                match result {
                    TmResult::Ok(_) => {
                        count += 1;
                        break;
                    }
                    TmResult::Updated { ctid, .. } => {
                        // EvalPlanQual: recheck the newest version.
                        let Some(latest) = heap.latest_version(ctid, txn.tm())? else {
                            break; // deleted meanwhile
                        };
                        let Some(t) = heap.fetch(latest)? else {
                            break;
                        };
                        let r = deform_tuple(&desc, &t)?;
                        let passes = match &filter {
                            Some(f) => is_true(&f.eval(&r)?),
                            None => true,
                        };
                        if !passes {
                            break;
                        }
                        (tid, row) = (latest, r);
                    }
                    // Deleted by a committed transaction, or already
                    // changed by this statement: skip it (PostgreSQL does).
                    TmResult::Deleted { .. } | TmResult::SelfModified | TmResult::Invisible => {
                        break;
                    }
                    TmResult::BeingModified(_) => {
                        return Err(SqlError::internal("row still being modified after waiting"));
                    }
                }
            }
        }
        Ok(count)
    }
}

fn check_user_relation(oid: u32, name: &str) -> Result<()> {
    if oid < FIRST_OID {
        return Err(SqlError::new(
            "42501",
            format!("permission denied: \"{name}\" is a system catalog"),
        ));
    }
    Ok(())
}
