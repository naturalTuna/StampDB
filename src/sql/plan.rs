//! Query plans. The binder builds a logical tree from these nodes (scans
//! with no index, joins as nested loops, filters as separate nodes); the
//! planner rewrites it (filters pushed into scans, index scans, hash
//! joins). Every node outputs rows of a fixed width; expressions refer to
//! the columns of their input row (for joins, left columns then right).

use std::ops::Bound;

use super::expr::Expr;
use crate::catalog::{IndexInfo, RelationInfo};
use crate::types::DataType;

/// A result column: name and type (RowDescription).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutCol {
    pub name: String,
    pub ty: DataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    /// Left outer join: unmatched left rows get NULLs on the right.
    Left,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    CountStar,
    Count,
    /// sum(int4) is int8; sum(float8) is float8.
    Sum,
    /// avg(float8) only until NUMERIC exists.
    Avg,
    Min,
    Max,
}

/// One aggregate computed per group.
#[derive(Debug, Clone, PartialEq)]
pub struct AggCall {
    pub func: AggFunc,
    /// Argument over the aggregate's input row (`None` for count(*)).
    pub arg: Option<Expr>,
    pub distinct: bool,
    /// Result type.
    pub ty: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Sequential scan of a table's visible rows; output = its columns.
    Scan {
        table: RelationInfo,
        filter: Option<Expr>,
    },
    /// Rows found through `index` between the key bounds (encoded key
    /// prefixes); `filter` rechecks the whole predicate.
    IndexScan {
        table: RelationInfo,
        index: IndexInfo,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
        filter: Option<Expr>,
    },
    /// Constant rows (VALUES, or one empty row for SELECT without FROM).
    Values {
        rows: Vec<Vec<Expr>>,
        width: usize,
    },
    Filter {
        input: Box<Plan>,
        pred: Expr,
    },
    Project {
        input: Box<Plan>,
        exprs: Vec<Expr>,
    },
    /// `on` is evaluated over the joined row (left ++ right).
    NestedLoop {
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinKind,
        on: Option<Expr>,
    },
    /// Equi-join building a hash table on the right input. Keys are
    /// evaluated over each side's own row; `residual` over the joined row.
    HashJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinKind,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        residual: Option<Expr>,
    },
    /// Output = group-by values then aggregate results.
    Aggregate {
        input: Box<Plan>,
        group_by: Vec<Expr>,
        aggs: Vec<AggCall>,
    },
    Sort {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Distinct {
        input: Box<Plan>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<u64>,
        offset: u64,
    },
}

impl Plan {
    /// Number of columns in each output row.
    pub fn width(&self) -> usize {
        match self {
            Plan::Scan { table, .. } | Plan::IndexScan { table, .. } => table.columns.len(),
            Plan::Values { width, .. } => *width,
            Plan::Filter { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Distinct { input }
            | Plan::Limit { input, .. } => input.width(),
            Plan::Project { exprs, .. } => exprs.len(),
            Plan::NestedLoop { left, right, .. } | Plan::HashJoin { left, right, .. } => {
                left.width() + right.width()
            }
            Plan::Aggregate { group_by, aggs, .. } => group_by.len() + aggs.len(),
        }
    }
}
