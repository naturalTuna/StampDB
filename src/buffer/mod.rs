//! Buffer pool (PostgreSQL style): pinned frames, content latches,
//! a partitioned mapping table, and clock-sweep replacement.
//!
//! # Latch order
//!
//! Latches are always acquired in this order, which rules out deadlocks
//! among them:
//!
//! 1. frame content lock (`RwLock<AlignedBuf>`)
//! 2. mapping table partition locks (ascending partition number)
//! 3. frame header lock (`Mutex<FrameHeader>`)
//! 4. free list lock
//!
//! A content lock is never acquired while holding a partition or header
//! lock, and no I/O is done while holding a partition or header lock.
//!
//! # Pins and content locks
//!
//! As in PostgreSQL, pinning and locking are separate: a [`PinGuard`] keeps
//! the page resident; [`PinGuard::read`] / [`PinGuard::write`] take the
//! content lock. Lock guards borrow the pin guard, so a lock is always
//! released before its pin.

mod control;
mod error;
mod guard;
mod pool;

pub use control::{FlushControl, SingleModeFlush};
pub use error::{BufferError, Result};
pub use guard::{PageMut, PageRef, PinGuard};
pub use pool::{BufferPool, BufferTag, FlushOutcome, MAX_USAGE_COUNT, NUM_PARTITIONS};
