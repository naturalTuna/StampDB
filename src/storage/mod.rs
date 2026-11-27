//! Storage layer: page format, aligned buffers, and file access.

mod aligned;
mod error;
pub mod page;

pub use aligned::{AlignedBuf, IO_ALIGN};
pub use error::{Result, StorageError};
