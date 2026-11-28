//! SQL layer: parsing (sqlparser, PostgreSQL dialect), binding, planning,
//! and Volcano-style execution (DESIGN r11).

pub mod datum;
pub mod error;
pub mod expr;

pub use error::{Result, SqlError};
pub mod autovacuum;
pub mod binder;
pub mod exec;
pub mod plan;
pub mod planner;
pub mod ri;
pub mod session;
pub mod writer;

#[cfg(test)]
mod tests;

pub use autovacuum::{Autovacuum, AutovacuumConfig};
pub use session::{Database, QueryResult, Session};
