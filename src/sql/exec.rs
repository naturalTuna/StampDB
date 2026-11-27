//! Volcano-style executor: each plan node becomes an operator pulling rows
//! from its inputs. Hash tables, sorts, and the inner side of nested loops
//! are kept in memory (DESIGN r11: no spill).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::error::{Result, SqlError};
use super::expr::{BinOp, Expr, compare, compare_nulls_last, is_true};
use super::plan::{AggCall, AggFunc, JoinKind, Plan, SortKey};
use crate::access::btree::BTree;
use crate::access::heap::HeapFile;
use crate::access::tuple::{Tid, TupleDesc, deform_tuple};
use crate::access::visibility::Me;
use crate::buffer::BufferPool;
use crate::storage::page::BlockNumber;
use crate::txn::{Snapshot, TransactionManager};
use crate::types::Value;

pub type Row = Vec<Value>;

/// A row source.
pub trait Operator {
    fn next(&mut self) -> Result<Option<Row>>;
}

/// What scans need to read rows as one statement sees them.
#[derive(Clone)]
pub struct ExecEnv {
    pub pool: Arc<BufferPool>,
    pub tm: Arc<dyn TransactionManager>,
    pub snap: Snapshot,
    pub me: Me,
}

/// Appends a hashable encoding of `vals` in which equal values (by SQL
/// equality, with NULLs equal to each other as in GROUP BY and DISTINCT)
/// have equal bytes.
pub fn group_key(vals: &[Value], out: &mut Vec<u8>) {
    for v in vals {
        match v {
            Value::Null => out.push(0),
            Value::Int4(x) => {
                out.push(1);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Value::Int8(x) => {
                out.push(2);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Value::Bool(b) => out.extend_from_slice(&[3, u8::from(*b)]),
            Value::Float8(x) => {
                out.push(4);
                let bits = if x.is_nan() {
                    f64::NAN.to_bits()
                } else if *x == 0.0 {
                    0
                } else {
                    x.to_bits()
                };
                out.extend_from_slice(&bits.to_le_bytes());
            }
            Value::Timestamp(x) => {
                out.push(5);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Value::Text(s) => {
                out.push(6);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
}

fn eval_all(exprs: &[Expr], row: &[Value]) -> Result<Row> {
    exprs.iter().map(|e| e.eval(row)).collect()
}

fn passes(filter: &Option<Expr>, row: &[Value]) -> Result<bool> {
    Ok(match filter {
        Some(f) => is_true(&f.eval(row)?),
        None => true,
    })
}

/// Sequential scan returning visible rows with their TIDs.
pub struct SeqScan {
    heap: HeapFile,
    desc: TupleDesc,
    env: Arc<ExecEnv>,
    filter: Option<Expr>,
    nblocks: u32,
    next_blk: u32,
    buffered: std::vec::IntoIter<(Tid, Vec<u8>)>,
}

impl SeqScan {
    pub fn new(
        heap: HeapFile,
        desc: TupleDesc,
        filter: Option<Expr>,
        env: Arc<ExecEnv>,
    ) -> Result<Self> {
        let nblocks = heap.nblocks()?;
        Ok(Self {
            heap,
            desc,
            env,
            filter,
            nblocks,
            next_blk: 0,
            buffered: Vec::new().into_iter(),
        })
    }

    pub fn next_with_tid(&mut self) -> Result<Option<(Tid, Row)>> {
        loop {
            if let Some((tid, t)) = self.buffered.next() {
                let row = deform_tuple(&self.desc, &t)?;
                if passes(&self.filter, &row)? {
                    return Ok(Some((tid, row)));
                }
                continue;
            }
            if self.next_blk >= self.nblocks {
                return Ok(None);
            }
            let blk = BlockNumber(self.next_blk);
            self.next_blk += 1;
            let env = &self.env;
            self.buffered = self
                .heap
                .read_page_mvcc(blk, &env.snap, &env.me, env.tm.as_ref())?
                .into_iter();
        }
    }
}

impl Operator for SeqScan {
    fn next(&mut self) -> Result<Option<Row>> {
        Ok(self.next_with_tid()?.map(|(_, r)| r))
    }
}

/// Index scan: TIDs in key order from the index range, each fetched from
/// the heap if visible. The TIDs of the range are collected when the scan
/// opens (no long-lived index position).
pub struct IndexScan {
    heap: HeapFile,
    desc: TupleDesc,
    env: Arc<ExecEnv>,
    filter: Option<Expr>,
    tids: std::vec::IntoIter<Tid>,
}

impl IndexScan {
    pub fn new(
        heap: HeapFile,
        desc: TupleDesc,
        tree: &BTree,
        lower: std::ops::Bound<Vec<u8>>,
        upper: std::ops::Bound<Vec<u8>>,
        filter: Option<Expr>,
        env: Arc<ExecEnv>,
    ) -> Result<Self> {
        let tids = tree
            .scan(lower, upper)?
            .map(|e| e.map(|(_, tid)| tid))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            heap,
            desc,
            env,
            filter,
            tids: tids.into_iter(),
        })
    }

    pub fn next_with_tid(&mut self) -> Result<Option<(Tid, Row)>> {
        for tid in self.tids.by_ref() {
            let env = &self.env;
            let Some(t) = self
                .heap
                .fetch_visible(tid, &env.snap, &env.me, env.tm.as_ref())?
            else {
                continue;
            };
            let row = deform_tuple(&self.desc, &t)?;
            if passes(&self.filter, &row)? {
                return Ok(Some((tid, row)));
            }
        }
        Ok(None)
    }
}

impl Operator for IndexScan {
    fn next(&mut self) -> Result<Option<Row>> {
        Ok(self.next_with_tid()?.map(|(_, r)| r))
    }
}

/// A scan of one table returning TIDs (UPDATE and DELETE targets).
pub enum TableScan {
    Seq(SeqScan),
    Index(IndexScan),
}

impl TableScan {
    pub fn next_with_tid(&mut self) -> Result<Option<(Tid, Row)>> {
        match self {
            TableScan::Seq(s) => s.next_with_tid(),
            TableScan::Index(s) => s.next_with_tid(),
        }
    }
}

/// Opens a scan node of the plan (`Scan` or `IndexScan`).
pub fn table_scan(plan: &Plan, env: &Arc<ExecEnv>) -> Result<TableScan> {
    match plan {
        Plan::Scan { table, filter } => {
            let heap = HeapFile::open(table.rel, env.pool.clone())?;
            Ok(TableScan::Seq(SeqScan::new(
                heap,
                table.desc(),
                filter.clone(),
                env.clone(),
            )?))
        }
        Plan::IndexScan {
            table,
            index,
            lower,
            upper,
            filter,
        } => {
            let heap = HeapFile::open(table.rel, env.pool.clone())?;
            let tree = BTree::open(index.rel, env.pool.clone())?;
            Ok(TableScan::Index(IndexScan::new(
                heap,
                table.desc(),
                &tree,
                lower.clone(),
                upper.clone(),
                filter.clone(),
                env.clone(),
            )?))
        }
        _ => Err(SqlError::internal("not a table scan")),
    }
}

struct Values {
    rows: std::vec::IntoIter<Vec<Expr>>,
}

impl Operator for Values {
    fn next(&mut self) -> Result<Option<Row>> {
        match self.rows.next() {
            Some(exprs) => Ok(Some(eval_all(&exprs, &[])?)),
            None => Ok(None),
        }
    }
}

struct Filter {
    input: Box<dyn Operator>,
    pred: Expr,
}

impl Operator for Filter {
    fn next(&mut self) -> Result<Option<Row>> {
        while let Some(row) = self.input.next()? {
            if is_true(&self.pred.eval(&row)?) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct Project {
    input: Box<dyn Operator>,
    exprs: Vec<Expr>,
}

impl Operator for Project {
    fn next(&mut self) -> Result<Option<Row>> {
        match self.input.next()? {
            Some(row) => Ok(Some(eval_all(&self.exprs, &row)?)),
            None => Ok(None),
        }
    }
}

fn collect(op: &mut dyn Operator) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    while let Some(r) = op.next()? {
        rows.push(r);
    }
    Ok(rows)
}

fn joined(left: &[Value], right: &[Value]) -> Row {
    let mut r = Vec::with_capacity(left.len() + right.len());
    r.extend_from_slice(left);
    r.extend_from_slice(right);
    r
}

/// Nested loop join over the materialized right input.
struct NestedLoop {
    left: Box<dyn Operator>,
    right: Vec<Row>,
    right_width: usize,
    kind: JoinKind,
    on: Option<Expr>,
    current: Option<Row>,
    pos: usize,
    matched: bool,
}

impl Operator for NestedLoop {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let Some(l) = &self.current else {
                match self.left.next()? {
                    Some(r) => {
                        self.current = Some(r);
                        self.pos = 0;
                        self.matched = false;
                        continue;
                    }
                    None => return Ok(None),
                }
            };
            while self.pos < self.right.len() {
                let row = joined(l, &self.right[self.pos]);
                self.pos += 1;
                if passes(&self.on, &row)? {
                    self.matched = true;
                    return Ok(Some(row));
                }
            }
            let l = self.current.take().unwrap_or_default();
            if self.kind == JoinKind::Left && !self.matched {
                return Ok(Some(joined(&l, &vec![Value::Null; self.right_width])));
            }
        }
    }
}

/// Hash join: builds on the right input, probes with the left.
struct HashJoin {
    left: Box<dyn Operator>,
    table: HashMap<Vec<u8>, Vec<Row>>,
    right_width: usize,
    kind: JoinKind,
    left_keys: Vec<Expr>,
    residual: Option<Expr>,
    pending: std::vec::IntoIter<Row>,
}

impl HashJoin {
    fn new(
        left: Box<dyn Operator>,
        mut right: Box<dyn Operator>,
        right_width: usize,
        kind: JoinKind,
        left_keys: Vec<Expr>,
        right_keys: &[Expr],
        residual: Option<Expr>,
    ) -> Result<Self> {
        let mut table: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();
        while let Some(r) = right.next()? {
            let k = eval_all(right_keys, &r)?;
            if k.iter().any(Value::is_null) {
                continue; // NULL never equals anything
            }
            let mut key = Vec::new();
            group_key(&k, &mut key);
            table.entry(key).or_default().push(r);
        }
        Ok(Self {
            left,
            table,
            right_width,
            kind,
            left_keys,
            residual,
            pending: Vec::new().into_iter(),
        })
    }
}

impl Operator for HashJoin {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.pending.next() {
                return Ok(Some(r));
            }
            let Some(l) = self.left.next()? else {
                return Ok(None);
            };
            let k = eval_all(&self.left_keys, &l)?;
            let mut out = Vec::new();
            if !k.iter().any(Value::is_null) {
                let mut key = Vec::new();
                group_key(&k, &mut key);
                if let Some(matches) = self.table.get(&key) {
                    for r in matches {
                        let row = joined(&l, r);
                        if passes(&self.residual, &row)? {
                            out.push(row);
                        }
                    }
                }
            }
            if out.is_empty() && self.kind == JoinKind::Left {
                out.push(joined(&l, &vec![Value::Null; self.right_width]));
            }
            self.pending = out.into_iter();
        }
    }
}

/// Running state of one aggregate in one group.
#[derive(Debug, Clone)]
enum Acc {
    Count(i64),
    SumInt(Option<i64>),
    SumFloat(Option<f64>),
    Avg { sum: f64, n: i64 },
    MinMax(Option<Value>),
}

fn acc_init(call: &AggCall) -> Acc {
    match call.func {
        AggFunc::CountStar | AggFunc::Count => Acc::Count(0),
        AggFunc::Sum if call.ty == crate::types::DataType::Float8 => Acc::SumFloat(None),
        AggFunc::Sum => Acc::SumInt(None),
        AggFunc::Avg => Acc::Avg { sum: 0.0, n: 0 },
        AggFunc::Min | AggFunc::Max => Acc::MinMax(None),
    }
}

fn float_add(a: f64, b: f64) -> Result<f64> {
    let sum = Expr::Binary {
        op: BinOp::Add,
        l: Box::new(Expr::Const(Value::Float8(a))),
        r: Box::new(Expr::Const(Value::Float8(b))),
    };
    match sum.eval(&[])? {
        Value::Float8(x) => Ok(x),
        _ => Err(SqlError::internal("float addition")),
    }
}

fn acc_add(acc: &mut Acc, func: AggFunc, v: Value) -> Result<()> {
    if func != AggFunc::CountStar && v.is_null() {
        return Ok(()); // aggregates skip NULL inputs
    }
    match acc {
        Acc::Count(n) => *n += 1,
        Acc::SumInt(s) => {
            let x = match v {
                Value::Int4(x) => i64::from(x),
                Value::Int8(x) => x,
                _ => return Err(SqlError::internal("sum of a non-integer")),
            };
            let cur = s.unwrap_or(0);
            *s = Some(cur.checked_add(x).ok_or_else(|| {
                SqlError::new(
                    super::error::code::NUMERIC_VALUE_OUT_OF_RANGE,
                    "bigint out of range",
                )
            })?);
        }
        Acc::SumFloat(s) => {
            let Value::Float8(x) = v else {
                return Err(SqlError::internal("sum of a non-float"));
            };
            *s = Some(float_add(s.unwrap_or(0.0), x)?);
        }
        Acc::Avg { sum, n } => {
            let Value::Float8(x) = v else {
                return Err(SqlError::internal("avg of a non-float"));
            };
            *sum = float_add(*sum, x)?;
            *n += 1;
        }
        Acc::MinMax(cur) => {
            let replace = match cur {
                None => true,
                Some(c) => {
                    let o = compare(&v, c).unwrap_or(Ordering::Equal);
                    if func == AggFunc::Min {
                        o == Ordering::Less
                    } else {
                        o == Ordering::Greater
                    }
                }
            };
            if replace {
                *cur = Some(v);
            }
        }
    }
    Ok(())
}

fn acc_result(acc: Acc) -> Value {
    match acc {
        Acc::Count(n) => Value::Int8(n),
        Acc::SumInt(s) => s.map_or(Value::Null, Value::Int8),
        Acc::SumFloat(s) => s.map_or(Value::Null, Value::Float8),
        Acc::Avg { n: 0, .. } => Value::Null,
        Acc::Avg { sum, n } => Value::Float8(sum / n as f64),
        Acc::MinMax(v) => v.unwrap_or(Value::Null),
    }
}

struct Group {
    keys: Row,
    accs: Vec<Acc>,
    /// Seen argument values of DISTINCT aggregates.
    seen: Vec<HashSet<Vec<u8>>>,
}

/// Hash aggregation, computed fully when first pulled.
struct Aggregate {
    input: Option<Box<dyn Operator>>,
    group_by: Vec<Expr>,
    aggs: Vec<AggCall>,
    out: std::vec::IntoIter<Row>,
}

impl Aggregate {
    fn compute(&mut self, mut input: Box<dyn Operator>) -> Result<Vec<Row>> {
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut groups: Vec<Group> = Vec::new();
        let new_group = |keys: Row, aggs: &[AggCall]| Group {
            keys,
            accs: aggs.iter().map(acc_init).collect(),
            seen: vec![HashSet::new(); aggs.len()],
        };
        if self.group_by.is_empty() {
            groups.push(new_group(Vec::new(), &self.aggs));
            index.insert(Vec::new(), 0);
        }
        while let Some(row) = input.next()? {
            let keys = eval_all(&self.group_by, &row)?;
            let mut k = Vec::new();
            group_key(&keys, &mut k);
            let gi = match index.get(&k) {
                Some(&i) => i,
                None => {
                    groups.push(new_group(keys, &self.aggs));
                    index.insert(k, groups.len() - 1);
                    groups.len() - 1
                }
            };
            let g = &mut groups[gi];
            for (i, call) in self.aggs.iter().enumerate() {
                let v = match &call.arg {
                    Some(a) => a.eval(&row)?,
                    None => Value::Null,
                };
                if call.distinct {
                    if v.is_null() {
                        continue;
                    }
                    let mut dk = Vec::new();
                    group_key(std::slice::from_ref(&v), &mut dk);
                    if !g.seen[i].insert(dk) {
                        continue;
                    }
                }
                acc_add(&mut g.accs[i], call.func, v)?;
            }
        }
        Ok(groups
            .into_iter()
            .map(|g| {
                let mut r = g.keys;
                r.extend(g.accs.into_iter().map(acc_result));
                r
            })
            .collect())
    }
}

impl Operator for Aggregate {
    fn next(&mut self) -> Result<Option<Row>> {
        if let Some(input) = self.input.take() {
            self.out = self.compute(input)?.into_iter();
        }
        Ok(self.out.next())
    }
}

/// In-memory sort, done when first pulled.
struct Sort {
    input: Option<Box<dyn Operator>>,
    keys: Vec<SortKey>,
    out: std::vec::IntoIter<Row>,
}

fn sort_rows(rows: Vec<Row>, keys: &[SortKey]) -> Result<Vec<Row>> {
    let mut keyed = rows
        .into_iter()
        .map(|r| {
            Ok((
                keys.iter()
                    .map(|k| k.expr.eval(&r))
                    .collect::<Result<Vec<_>>>()?,
                r,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|(a, _), (b, _)| {
        for (k, (x, y)) in keys.iter().zip(a.iter().zip(b)) {
            let o = match (x.is_null(), y.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if k.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if k.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => {
                    let o = compare_nulls_last(x, y);
                    if k.desc { o.reverse() } else { o }
                }
            };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    Ok(keyed.into_iter().map(|(_, r)| r).collect())
}

impl Operator for Sort {
    fn next(&mut self) -> Result<Option<Row>> {
        if let Some(mut input) = self.input.take() {
            let rows = collect(input.as_mut())?;
            self.out = sort_rows(rows, &self.keys)?.into_iter();
        }
        Ok(self.out.next())
    }
}

struct Distinct {
    input: Box<dyn Operator>,
    seen: HashSet<Vec<u8>>,
}

impl Operator for Distinct {
    fn next(&mut self) -> Result<Option<Row>> {
        while let Some(row) = self.input.next()? {
            let mut k = Vec::new();
            group_key(&row, &mut k);
            if self.seen.insert(k) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct Limit {
    input: Box<dyn Operator>,
    remaining: Option<u64>,
    skip: u64,
}

impl Operator for Limit {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        while self.skip > 0 {
            if self.input.next()?.is_none() {
                return Ok(None);
            }
            self.skip -= 1;
        }
        let r = self.input.next()?;
        if r.is_some()
            && let Some(n) = &mut self.remaining
        {
            *n -= 1;
        }
        Ok(r)
    }
}

/// Builds the operator tree for `plan`.
pub fn build(plan: &Plan, env: &Arc<ExecEnv>) -> Result<Box<dyn Operator>> {
    Ok(match plan {
        Plan::Scan { .. } | Plan::IndexScan { .. } => match table_scan(plan, env)? {
            TableScan::Seq(s) => Box::new(s),
            TableScan::Index(s) => Box::new(s),
        },
        Plan::Values { rows, .. } => Box::new(Values {
            rows: rows.clone().into_iter(),
        }),
        Plan::Filter { input, pred } => Box::new(Filter {
            input: build(input, env)?,
            pred: pred.clone(),
        }),
        Plan::Project { input, exprs } => Box::new(Project {
            input: build(input, env)?,
            exprs: exprs.clone(),
        }),
        Plan::NestedLoop {
            left,
            right,
            kind,
            on,
        } => {
            let mut r = build(right, env)?;
            Box::new(NestedLoop {
                left: build(left, env)?,
                right: collect(r.as_mut())?,
                right_width: right.width(),
                kind: *kind,
                on: on.clone(),
                current: None,
                pos: 0,
                matched: false,
            })
        }
        Plan::HashJoin {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            residual,
        } => Box::new(HashJoin::new(
            build(left, env)?,
            build(right, env)?,
            right.width(),
            *kind,
            left_keys.clone(),
            right_keys,
            residual.clone(),
        )?),
        Plan::Aggregate {
            input,
            group_by,
            aggs,
        } => Box::new(Aggregate {
            input: Some(build(input, env)?),
            group_by: group_by.clone(),
            aggs: aggs.clone(),
            out: Vec::new().into_iter(),
        }),
        Plan::Sort { input, keys } => Box::new(Sort {
            input: Some(build(input, env)?),
            keys: keys.clone(),
            out: Vec::new().into_iter(),
        }),
        Plan::Distinct { input } => Box::new(Distinct {
            input: build(input, env)?,
            seen: HashSet::new(),
        }),
        Plan::Limit {
            input,
            limit,
            offset,
        } => Box::new(Limit {
            input: build(input, env)?,
            remaining: *limit,
            skip: *offset,
        }),
    })
}

/// Runs `plan` to completion.
pub fn run(plan: &Plan, env: &Arc<ExecEnv>) -> Result<Vec<Row>> {
    let mut op = build(plan, env)?;
    collect(op.as_mut())
}
