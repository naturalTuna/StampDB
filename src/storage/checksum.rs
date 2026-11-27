//! Page checksums (CRC32C).
//!
//! The checksum covers the whole page with the checksum field treated as
//! zero, followed by the block number. Mixing in the block number detects a
//! valid page written to the wrong location, as PostgreSQL does.
//!
//! Compared to PostgreSQL's 16-bit FNV-1a variant, CRC32C is 32 bits wide and
//! hardware-accelerated on x86_64 (SSE4.2).
//!
//! An all-zero page is a "new" page and passes verification without a
//! checksum (PostgreSQL's `PageIsNew` rule).

use super::page::{self, BlockNumber, is_new_page, off};
use super::{Result, StorageError};

const CHECKSUM_LEN: usize = 4;

/// Computes the checksum of `page` stored at `block`.
pub fn page_checksum(page: &[u8], block: BlockNumber) -> Result<u32> {
    page::check_page_len(page)?;
    let end = off::CHECKSUM + CHECKSUM_LEN;
    let mut crc = crc32c::crc32c(&page[..off::CHECKSUM]);
    crc = crc32c::crc32c_append(crc, &[0u8; CHECKSUM_LEN]);
    crc = crc32c::crc32c_append(crc, &page[end..]);
    crc = crc32c::crc32c_append(crc, &block.0.to_le_bytes());
    Ok(crc)
}

/// Computes and stores the checksum in the page header.
pub fn set_checksum(page: &mut [u8], block: BlockNumber) -> Result<()> {
    let crc = page_checksum(page, block)?;
    page[off::CHECKSUM..off::CHECKSUM + CHECKSUM_LEN].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Verifies the stored checksum. All-zero pages are accepted.
pub fn verify_checksum(page: &[u8], block: BlockNumber) -> Result<()> {
    page::check_page_len(page)?;
    if is_new_page(page) {
        return Ok(());
    }
    let stored = page::read_u32(page, off::CHECKSUM);
    let computed = page_checksum(page, block)?;
    if stored != computed {
        return Err(StorageError::ChecksumMismatch {
            block: block.0,
            stored,
            computed,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{PAGE_SIZE, PageKind};
    use crate::storage::slotted::SlottedPage;
    use proptest::prelude::*;

    fn sample_page() -> Vec<u8> {
        let mut p = SlottedPage::init(vec![0u8; PAGE_SIZE], PageKind::Heap, 0).unwrap();
        p.add_item(b"some tuple data").unwrap().unwrap();
        p.into_inner()
    }

    #[test]
    fn crc32c_known_vector() {
        // Standard CRC-32C check value.
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn set_then_verify() {
        let mut p = sample_page();
        set_checksum(&mut p, BlockNumber(7)).unwrap();
        verify_checksum(&p, BlockNumber(7)).unwrap();
        assert_ne!(page::read_u32(&p, off::CHECKSUM), 0);
    }

    #[test]
    fn checksum_ignores_stored_field() {
        let mut p = sample_page();
        let before = page_checksum(&p, BlockNumber(1)).unwrap();
        set_checksum(&mut p, BlockNumber(1)).unwrap();
        assert_eq!(page_checksum(&p, BlockNumber(1)).unwrap(), before);
    }

    #[test]
    fn wrong_block_is_detected() {
        let mut p = sample_page();
        set_checksum(&mut p, BlockNumber(3)).unwrap();
        assert!(matches!(
            verify_checksum(&p, BlockNumber(4)),
            Err(StorageError::ChecksumMismatch { block: 4, .. })
        ));
    }

    #[test]
    fn new_page_passes() {
        verify_checksum(&vec![0u8; PAGE_SIZE], BlockNumber(0)).unwrap();
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(matches!(
            page_checksum(&[0u8; 10], BlockNumber(0)),
            Err(StorageError::InvalidPageLength { len: 10 })
        ));
    }

    proptest! {
        #[test]
        fn any_bit_flip_is_detected(byte in 0usize..PAGE_SIZE, bit in 0u8..8, blk in any::<u32>()) {
            let mut p = sample_page();
            set_checksum(&mut p, BlockNumber(blk)).unwrap();
            p[byte] ^= 1 << bit;
            prop_assert!(verify_checksum(&p, BlockNumber(blk)).is_err());
        }
    }
}
