//! Rule-based planner (DESIGN r11): rewrites the binder's logical plan.
//!
//! 1. Push-down: WHERE and ON conjuncts move as close to the scans as is
//!    valid (through inner joins; into the preserved side of a left join
//!    only from WHERE, into the nullable side only from ON).
//! 2. Scans whose filter has an equality or range condition on the leading
//!    column(s) of an index become index scans (the whole filter is still
//!    rechecked on each row).
//! 3. Joins with an equality between the two sides become hash joins.

use std::ops::Bound;

use super::error::Result;
use super::expr::{BinOp, Expr};
use super::plan::{JoinKind, Plan};
use crate::access::btree::key::encode_key;
use crate::catalog::{IndexInfo, RelationInfo};
use crate::types::{DataType, Value};

/// Supplies the indexes of a table.
pub type IndexLookup<'a> = dyn FnMut(&RelationInfo) -> Result<Vec<IndexInfo>> + 'a;

/// Rewrites `plan` (see the module documentation).
pub fn optimize(plan: Plan, indexes: &mut IndexLookup<'_>) -> Result<Plan> {
    physical(push(plan, Vec::new()), indexes)
}

/// Splits an AND tree into its conjuncts.
fn conjuncts(e: Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(l, r) => {
            conjuncts(*l, out);
            conjuncts(*r, out);
        }
        other => out.push(other),
    }
}

fn and_all(mut preds: Vec<Expr>) -> Option<Expr> {
    let first = preds.pop()?;
    Some(
        preds
            .into_iter()
            .rev()
            .fold(first, |acc, p| Expr::And(Box::new(p), Box::new(acc))),
    )
}

/// Which input columns an expression reads: (min, max), `None` if none.
fn column_span(e: &Expr) -> Option<(usize, usize)> {
    let mut span: Option<(usize, usize)> = None;
    e.walk(&mut |x| {
        if let Expr::Column(c) = x {
            span = Some(match span {
                None => (*c, *c),
                Some((lo, hi)) => (lo.min(*c), hi.max(*c)),
            });
        }
    });
    span
}

#[derive(PartialEq)]
enum Side {
    Left,
    Right,
    Both,
    Neither,
}

fn side(e: &Expr, left_width: usize) -> Side {
    match column_span(e) {
        None => Side::Neither,
        Some((_, hi)) if hi < left_width => Side::Left,
        Some((lo, _)) if lo >= left_width => Side::Right,
        _ => Side::Both,
    }
}

fn shift_left(e: &Expr, by: usize) -> Expr {
    e.map_columns(&|c| c - by)
}

fn filtered(plan: Plan, preds: Vec<Expr>) -> Plan {
    match and_all(preds) {
        Some(pred) => Plan::Filter {
            input: Box::new(plan),
            pred,
        },
        None => plan,
    }
}

/// Pushes the conjuncts `preds` (over `plan`'s output) into `plan`.
fn push(plan: Plan, mut preds: Vec<Expr>) -> Plan {
    match plan {
        Plan::Filter { input, pred } => {
            conjuncts(pred, &mut preds);
            push(*input, preds)
        }
        Plan::Scan { table, filter } => {
            let mut all = Vec::new();
            if let Some(f) = filter {
                conjuncts(f, &mut all);
            }
            all.extend(preds);
            Plan::Scan {
                table,
                filter: and_all(all),
            }
        }
        Plan::NestedLoop {
            left,
            right,
            kind,
            on,
        } => {
            let lw = left.width();
            let mut on_preds = Vec::new();
            if let Some(o) = on {
                conjuncts(o, &mut on_preds);
            }
            let (mut to_left, mut to_right, mut keep_on, mut above) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            match kind {
                JoinKind::Inner => {
                    for p in preds.into_iter().chain(on_preds) {
                        match side(&p, lw) {
                            Side::Left => to_left.push(p),
                            Side::Right => to_right.push(shift_left(&p, lw)),
                            Side::Both | Side::Neither => keep_on.push(p),
                        }
                    }
                }
                JoinKind::Left => {
                    for p in preds {
                        match side(&p, lw) {
                            Side::Left => to_left.push(p),
                            _ => above.push(p),
                        }
                    }
                    for p in on_preds {
                        match side(&p, lw) {
                            Side::Right => to_right.push(shift_left(&p, lw)),
                            _ => keep_on.push(p),
                        }
                    }
                }
            }
            let join = Plan::NestedLoop {
                left: Box::new(push(*left, to_left)),
                right: Box::new(push(*right, to_right)),
                kind,
                on: and_all(keep_on),
            };
            filtered(join, above)
        }
        Plan::Project { input, exprs } => filtered(
            Plan::Project {
                input: Box::new(push(*input, Vec::new())),
                exprs,
            },
            preds,
        ),
        Plan::Aggregate {
            input,
            group_by,
            aggs,
        } => filtered(
            Plan::Aggregate {
                input: Box::new(push(*input, Vec::new())),
                group_by,
                aggs,
            },
            preds,
        ),
        Plan::Sort { input, keys } => filtered(
            Plan::Sort {
                input: Box::new(push(*input, Vec::new())),
                keys,
            },
            preds,
        ),
        Plan::Distinct { input } => filtered(
            Plan::Distinct {
                input: Box::new(push(*input, Vec::new())),
            },
            preds,
        ),
        Plan::Limit {
            input,
            limit,
            offset,
        } => filtered(
            Plan::Limit {
                input: Box::new(push(*input, Vec::new())),
                limit,
                offset,
            },
            preds,
        ),
        leaf @ (Plan::Values { .. } | Plan::IndexScan { .. } | Plan::HashJoin { .. }) => {
            filtered(leaf, preds)
        }
    }
}

fn boxed(p: Plan, indexes: &mut IndexLookup<'_>) -> Result<Box<Plan>> {
    Ok(Box::new(physical(p, indexes)?))
}

/// Chooses scan and join methods bottom-up.
fn physical(plan: Plan, indexes: &mut IndexLookup<'_>) -> Result<Plan> {
    Ok(match plan {
        Plan::Scan { table, filter } => {
            let candidates = match &filter {
                Some(_) => indexes(&table)?,
                None => Vec::new(),
            };
            match filter
                .as_ref()
                .and_then(|f| choose_index(&table, &candidates, f))
            {
                Some((index, lower, upper)) => Plan::IndexScan {
                    table,
                    index,
                    lower,
                    upper,
                    filter,
                },
                None => Plan::Scan { table, filter },
            }
        }
        Plan::NestedLoop {
            left,
            right,
            kind,
            on,
        } => {
            let lw = left.width();
            let left = boxed(*left, indexes)?;
            let right = boxed(*right, indexes)?;
            let mut preds = Vec::new();
            if let Some(o) = on {
                conjuncts(o, &mut preds);
            }
            let (mut left_keys, mut right_keys, mut residual) =
                (Vec::new(), Vec::new(), Vec::new());
            for p in preds {
                match equi_key(&p, lw) {
                    Some((l, r)) => {
                        left_keys.push(l);
                        right_keys.push(r);
                    }
                    None => residual.push(p),
                }
            }
            if left_keys.is_empty() {
                Plan::NestedLoop {
                    left,
                    right,
                    kind,
                    on: and_all(residual),
                }
            } else {
                Plan::HashJoin {
                    left,
                    right,
                    kind,
                    left_keys,
                    right_keys,
                    residual: and_all(residual),
                }
            }
        }
        Plan::Filter { input, pred } => Plan::Filter {
            input: boxed(*input, indexes)?,
            pred,
        },
        Plan::Project { input, exprs } => Plan::Project {
            input: boxed(*input, indexes)?,
            exprs,
        },
        Plan::HashJoin {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            residual,
        } => Plan::HashJoin {
            left: boxed(*left, indexes)?,
            right: boxed(*right, indexes)?,
            kind,
            left_keys,
            right_keys,
            residual,
        },
        Plan::Aggregate {
            input,
            group_by,
            aggs,
        } => Plan::Aggregate {
            input: boxed(*input, indexes)?,
            group_by,
            aggs,
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: boxed(*input, indexes)?,
            keys,
        },
        Plan::Distinct { input } => Plan::Distinct {
            input: boxed(*input, indexes)?,
        },
        Plan::Limit {
            input,
            limit,
            offset,
        } => Plan::Limit {
            input: boxed(*input, indexes)?,
            limit,
            offset,
        },
        leaf @ (Plan::IndexScan { .. } | Plan::Values { .. }) => leaf,
    })
}

/// `l = r` with one side over the left input and the other over the right:
/// (left key, right key shifted to the right input's columns).
fn equi_key(p: &Expr, lw: usize) -> Option<(Expr, Expr)> {
    let Expr::Binary {
        op: BinOp::Eq,
        l,
        r,
    } = p
    else {
        return None;
    };
    match (side(l, lw), side(r, lw)) {
        (Side::Left, Side::Right) => Some(((**l).clone(), shift_left(r, lw))),
        (Side::Right, Side::Left) => Some(((**r).clone(), shift_left(l, lw))),
        _ => None,
    }
}

/// A condition `column op constant` usable as an index bound.
struct Cond {
    col: usize,
    op: BinOp,
    value: Value,
}

fn index_cond(p: &Expr) -> Option<Cond> {
    let Expr::Binary { op, l, r } = p else {
        return None;
    };
    if !matches!(
        op,
        BinOp::Eq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
    ) {
        return None;
    }
    match (l.as_ref(), r.as_ref()) {
        (Expr::Column(c), Expr::Const(v)) if !v.is_null() => Some(Cond {
            col: *c,
            op: *op,
            value: v.clone(),
        }),
        (Expr::Const(v), Expr::Column(c)) if !v.is_null() => Some(Cond {
            col: *c,
            op: op.commute(),
            value: v.clone(),
        }),
        _ => None,
    }
}

/// The smallest key greater than every key starting with `prefix`.
fn successor(prefix: &[u8]) -> Bound<Vec<u8>> {
    let mut k = prefix.to_vec();
    while let Some(last) = k.pop() {
        if last < u8::MAX {
            k.push(last + 1);
            return Bound::Excluded(k);
        }
    }
    Bound::Unbounded
}

fn key_type(t: DataType) -> DataType {
    match t {
        // Same key encoding; avoids the length check on constants.
        DataType::Varchar(_) => DataType::Text,
        t => t,
    }
}

fn encode(types: &[DataType], values: &[Value]) -> Option<Vec<u8>> {
    let mut k = Vec::new();
    encode_key(types, values, &mut k).ok()?;
    Some(k)
}

type IndexChoice = (IndexInfo, Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Picks the index with the most leading columns fixed by equalities
/// (then one with a range on the next column) and builds its key bounds.
fn choose_index(table: &RelationInfo, indexes: &[IndexInfo], filter: &Expr) -> Option<IndexChoice> {
    let mut preds = Vec::new();
    conjuncts(filter.clone(), &mut preds);
    let conds: Vec<Cond> = preds.iter().filter_map(index_cond).collect();
    let mut best: Option<((usize, bool), IndexChoice)> = None;
    for idx in indexes {
        let cols: Vec<usize> = idx.columns.iter().map(|&c| usize::from(c) - 1).collect();
        let mut types = Vec::new();
        let mut values = Vec::new();
        for &c in &cols {
            let Some(eq) = conds.iter().find(|k| k.col == c && k.op == BinOp::Eq) else {
                break;
            };
            types.push(key_type(table.columns[c].ty));
            values.push(eq.value.clone());
        }
        let eqs = values.len();
        let Some(prefix) = encode(&types, &values) else {
            continue;
        };
        let (mut lower, mut upper) = if eqs > 0 {
            (Bound::Included(prefix.clone()), successor(&prefix))
        } else {
            (Bound::Unbounded, Bound::Unbounded)
        };
        let mut ranged = false;
        if let Some(&next) = cols.get(eqs) {
            let mut t = types.clone();
            t.push(key_type(table.columns[next].ty));
            for k in conds.iter().filter(|k| k.col == next && k.op != BinOp::Eq) {
                let mut v = values.clone();
                v.push(k.value.clone());
                let Some(key) = encode(&t, &v) else {
                    continue;
                };
                ranged = true;
                match k.op {
                    BinOp::Gt => lower = tighter_lower(lower, Bound::Included(after(&key))),
                    BinOp::GtEq => lower = tighter_lower(lower, Bound::Included(key)),
                    BinOp::Lt => upper = tighter_upper(upper, Bound::Excluded(key)),
                    _ => upper = tighter_upper(upper, successor(&key)),
                }
            }
        }
        if eqs == 0 && !ranged {
            continue;
        }
        let score = (eqs, ranged);
        if best.as_ref().is_none_or(|(s, _)| score > *s) {
            best = Some((score, (idx.clone(), lower, upper)));
        }
    }
    best.map(|(_, c)| c)
}

/// The smallest key greater than every key starting with `key` (as an
/// inclusive lower bound).
fn after(key: &[u8]) -> Vec<u8> {
    match successor(key) {
        Bound::Excluded(k) => k,
        // All 0xFF: nothing can follow; an impossible-to-reach bound.
        _ => vec![u8::MAX; key.len() + 1],
    }
}

fn tighter_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            if y > x { b } else { a }
        }
    }
}

fn tighter_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            if y < x { b } else { a }
        }
    }
}
