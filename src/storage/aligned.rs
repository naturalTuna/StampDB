//! Heap buffers aligned for Direct I/O.
//!
//! `O_DIRECT` requires the buffer address, file offset and transfer length to
//! be multiples of the device logical block size. We fix that unit at
//! [`IO_ALIGN`] (4096) and refuse to start on devices with a larger block size.

use std::alloc::{self, Layout};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use super::{Result, StorageError};

/// Alignment unit for Direct I/O: buffer address, offset and length.
pub const IO_ALIGN: usize = 4096;

/// A zero-initialized, heap-allocated byte buffer whose address is aligned to
/// [`IO_ALIGN`] and whose length is a non-zero multiple of [`IO_ALIGN`].
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: `AlignedBuf` uniquely owns its allocation (like `Box<[u8]>`); no
// aliasing pointers exist, so moving it to another thread is sound.
unsafe impl Send for AlignedBuf {}
// SAFETY: shared access only yields `&[u8]`; mutation requires `&mut self`.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocates a zeroed buffer of `len` bytes.
    ///
    /// `len` must be a non-zero multiple of [`IO_ALIGN`].
    pub fn new(len: usize) -> Result<Self> {
        if len == 0 || !len.is_multiple_of(IO_ALIGN) {
            return Err(StorageError::InvalidBufferSize { len });
        }
        let layout = Layout::from_size_align(len, IO_ALIGN)
            .map_err(|_| StorageError::InvalidBufferSize { len })?;
        // SAFETY: `layout` has non-zero size (checked above).
        let raw = unsafe { alloc::alloc_zeroed(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            // Same behavior as `Vec`/`Box` on allocation failure.
            alloc::handle_alloc_error(layout);
        };
        Ok(Self { ptr, layout })
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.layout.size()
    }

    /// Always false: zero-length buffers cannot be constructed.
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: `ptr` points to `layout.size()` initialized (zeroed) bytes
        // owned by `self`, and the returned borrow is tied to `&self`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; `&mut self` guarantees exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr` was returned by `alloc_zeroed` with this exact `layout`
        // and has not been freed before.
        unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

impl fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_is_aligned_and_zeroed() {
        for pages in [1, 2, 3, 16] {
            let buf = AlignedBuf::new(pages * IO_ALIGN).unwrap();
            assert_eq!(buf.as_ptr() as usize % IO_ALIGN, 0);
            assert_eq!(buf.len(), pages * IO_ALIGN);
            assert!(buf.iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn rejects_invalid_sizes() {
        for len in [0, 1, 512, IO_ALIGN - 1, IO_ALIGN + 1] {
            assert!(matches!(
                AlignedBuf::new(len),
                Err(StorageError::InvalidBufferSize { len: l }) if l == len
            ));
        }
    }

    #[test]
    fn contents_are_writable() {
        let mut buf = AlignedBuf::new(IO_ALIGN).unwrap();
        buf[0] = 0xAB;
        buf[IO_ALIGN - 1] = 0xCD;
        assert_eq!(buf[0], 0xAB);
        assert_eq!(buf[IO_ALIGN - 1], 0xCD);
    }

    #[test]
    fn can_move_across_threads() {
        let mut buf = AlignedBuf::new(IO_ALIGN).unwrap();
        buf[7] = 7;
        let buf = std::thread::spawn(move || buf).join().unwrap();
        assert_eq!(buf[7], 7);
    }
}
