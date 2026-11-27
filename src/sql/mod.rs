//! SQL layer: parsing (sqlparser, PostgreSQL dialect), binding, planning,
//! and Volcano-style execution (DESIGN r11).

pub mod datum;
pub mod error;

pub use error::{Result, SqlError};
