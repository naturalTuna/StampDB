//! Storage layer: page format, aligned buffers, and file access.

mod aligned;
pub mod backend;
pub mod checksum;
mod error;
pub mod local;
pub mod page;
pub mod slotted;

pub use aligned::{AlignedBuf, IO_ALIGN};
pub use error::{Result, StorageError};
