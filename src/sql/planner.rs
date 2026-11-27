//! Rule-based planner (DESIGN r11): rewrites the binder's logical plan.
//! Filters directly above scans move into the scans.

use super::error::Result;
use super::plan::Plan;

fn boxed(p: Plan) -> Result<Box<Plan>> {
    Ok(Box::new(optimize(p)?))
}

/// Rewrites `plan` bottom-up.
pub fn optimize(plan: Plan) -> Result<Plan> {
    Ok(match plan {
        Plan::Filter { input, pred } => match optimize(*input)? {
            Plan::Scan {
                table,
                filter: None,
            } => Plan::Scan {
                table,
                filter: Some(pred),
            },
            other => Plan::Filter {
                input: Box::new(other),
                pred,
            },
        },
        Plan::Project { input, exprs } => Plan::Project {
            input: boxed(*input)?,
            exprs,
        },
        Plan::NestedLoop {
            left,
            right,
            kind,
            on,
        } => Plan::NestedLoop {
            left: boxed(*left)?,
            right: boxed(*right)?,
            kind,
            on,
        },
        Plan::HashJoin {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            residual,
        } => Plan::HashJoin {
            left: boxed(*left)?,
            right: boxed(*right)?,
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
            input: boxed(*input)?,
            group_by,
            aggs,
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: boxed(*input)?,
            keys,
        },
        Plan::Distinct { input } => Plan::Distinct {
            input: boxed(*input)?,
        },
        Plan::Limit {
            input,
            limit,
            offset,
        } => Plan::Limit {
            input: boxed(*input)?,
            limit,
            offset,
        },
        leaf @ (Plan::Scan { .. } | Plan::IndexScan { .. } | Plan::Values { .. }) => leaf,
    })
}
