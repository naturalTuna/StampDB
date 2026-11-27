//! RAII guards for pins and content locks.

use std::ops::{Deref, DerefMut};

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use super::pool::{BufferPool, BufferTag, Frame, FrameId};
use crate::storage::AlignedBuf;

/// A pinned page. The frame cannot be evicted while this guard lives.
/// Dropping it unpins the frame.
#[derive(Debug)]
pub struct PinGuard<'a> {
    pool: &'a BufferPool,
    fid: FrameId,
    tag: BufferTag,
}

impl<'a> PinGuard<'a> {
    /// Wraps a pin that has already been taken on `fid`.
    pub(super) fn new(pool: &'a BufferPool, fid: FrameId, tag: BufferTag) -> Self {
        Self { pool, fid, tag }
    }

    /// Identity of the pinned page.
    pub fn tag(&self) -> BufferTag {
        self.tag
    }

    /// Takes the shared content lock.
    ///
    /// Blocks while another thread holds the exclusive lock. Taking a second
    /// lock on the same page from the same thread may deadlock.
    pub fn read(&self) -> PageRef<'_> {
        PageRef {
            guard: self.pool.frame(self.fid).content.read(),
        }
    }

    /// Takes the exclusive content lock.
    ///
    /// Changes must be followed by [`PageMut::mark_dirty`] to be written back.
    pub fn write(&self) -> PageMut<'_> {
        let frame = self.pool.frame(self.fid);
        PageMut {
            guard: frame.content.write(),
            frame,
        }
    }

    /// Takes the exclusive content lock only if it is free right now.
    ///
    /// Used for optional work such as setting hint bits, which is skipped
    /// rather than waited for.
    pub fn try_write(&self) -> Option<PageMut<'_>> {
        let frame = self.pool.frame(self.fid);
        frame
            .content
            .try_write()
            .map(|guard| PageMut { guard, frame })
    }
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        self.pool.unpin(self.fid);
    }
}

/// Shared access to page bytes.
#[derive(Debug)]
pub struct PageRef<'a> {
    guard: RwLockReadGuard<'a, AlignedBuf>,
}

impl Deref for PageRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.guard
    }
}

/// Exclusive access to page bytes.
#[derive(Debug)]
pub struct PageMut<'a> {
    guard: RwLockWriteGuard<'a, AlignedBuf>,
    frame: &'a Frame,
}

impl PageMut<'_> {
    /// Marks the page dirty so it is written back before eviction
    /// (PostgreSQL's `MarkBufferDirty`).
    pub fn mark_dirty(&self) {
        // Latch order: content lock (held) -> header lock.
        self.frame.hdr.lock().dirty = true;
    }

    /// Marks the page dirty because only hint bits changed
    /// (PostgreSQL's `MarkBufferDirtyHint`).
    ///
    /// Kept separate from [`PageMut::mark_dirty`] because the WAL step hooks
    /// here: the first hint change after a checkpoint must log a full page
    /// image, so a torn write of a hint-only change stays recoverable.
    pub fn mark_dirty_hint(&self) {
        self.frame.hdr.lock().dirty = true;
    }
}

impl Deref for PageMut<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.guard
    }
}

impl DerefMut for PageMut<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.guard
    }
}
