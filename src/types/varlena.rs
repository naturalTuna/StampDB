//! Variable-length value framing (varlena), shared by all variable-length
//! types (TEXT and VARCHAR now, NUMERIC later).
//!
//! The first byte carries the tag, so a decoder can tell the form from it:
//!
//! ```text
//! 0LLLLLLL                       short: 1-byte header, data length 0..=127
//! 10LLLLLL LLLLLLLL LLLLLLLL ... long: 4-byte big-endian header, 30-bit length
//! 11xxxxxx                       reserved: external (TOAST) pointer
//! ```
//!
//! Like PostgreSQL's short varlena, small values pay only one header byte.
//! The header is big-endian so the tag bits are in the first byte; all other
//! on-disk integers are little-endian.

use super::{Result, TypeError};

/// Largest data length a varlena can carry (same 1GB limit as PostgreSQL).
pub const MAX_VARLENA_LEN: usize = (1 << 30) - 1;

/// Largest data length that uses the 1-byte header.
pub const MAX_SHORT_LEN: usize = 0x7F;

const LONG_TAG: u8 = 0b10;
const EXTERNAL_TAG: u8 = 0b11;

/// Encoded size (header + data) for `len` data bytes.
pub fn encoded_len(len: usize) -> usize {
    if len <= MAX_SHORT_LEN {
        1 + len
    } else {
        4 + len
    }
}

/// Appends `data` with a varlena header.
pub fn encode(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let len = data.len();
    if len <= MAX_SHORT_LEN {
        out.push(len as u8);
    } else if len <= MAX_VARLENA_LEN {
        let header = (u32::from(LONG_TAG) << 30) | len as u32;
        out.extend_from_slice(&header.to_be_bytes());
    } else {
        return Err(TypeError::ValueTooLarge { len });
    }
    out.extend_from_slice(data);
    Ok(())
}

/// Decodes one varlena at the start of `buf`.
/// Returns the data and the total number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<(&[u8], usize)> {
    let first = *buf.first().ok_or(TypeError::Truncated)?;
    let (hdr, len) = match first >> 6 {
        0b00 | 0b01 => (1, usize::from(first)),
        LONG_TAG => {
            let b = buf.get(..4).ok_or(TypeError::Truncated)?;
            let header = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            (4, (header & 0x3FFF_FFFF) as usize)
        }
        EXTERNAL_TAG => return Err(TypeError::ExternalNotSupported),
        _ => return Err(TypeError::Corrupt("varlena tag")),
    };
    let data = buf.get(hdr..hdr + len).ok_or(TypeError::Truncated)?;
    Ok((data, hdr + len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn header_forms() {
        let mut out = Vec::new();
        encode(&[7u8; 127], &mut out).unwrap();
        assert_eq!(out[0], 127);
        assert_eq!(out.len(), 128);

        out.clear();
        encode(&[7u8; 128], &mut out).unwrap();
        assert_eq!(&out[..4], &[0x80, 0, 0, 128]);
        assert_eq!(out.len(), 132);
        assert_eq!(encoded_len(127), 128);
        assert_eq!(encoded_len(128), 132);
    }

    #[test]
    fn rejects_external_and_truncated() {
        assert!(matches!(
            decode(&[0xC0, 0, 0, 0]),
            Err(TypeError::ExternalNotSupported)
        ));
        assert!(matches!(decode(&[]), Err(TypeError::Truncated)));
        assert!(matches!(decode(&[5, 1, 2]), Err(TypeError::Truncated)));
        assert!(matches!(decode(&[0x80, 0]), Err(TypeError::Truncated)));
    }

    proptest! {
        #[test]
        fn roundtrip(data in prop::collection::vec(any::<u8>(), 0..600), tail in prop::collection::vec(any::<u8>(), 0..8)) {
            let mut out = Vec::new();
            encode(&data, &mut out).unwrap();
            prop_assert_eq!(out.len(), encoded_len(data.len()));
            out.extend_from_slice(&tail);
            let (got, used) = decode(&out).unwrap();
            prop_assert_eq!(got, &data[..]);
            prop_assert_eq!(used, encoded_len(data.len()));
        }
    }
}
