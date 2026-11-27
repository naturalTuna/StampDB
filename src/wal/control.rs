//! Control file: where recovery starts.
//!
//! Two 4KB blocks are written alternately, each with a sequence number and
//! a CRC; the valid block with the highest sequence number is current. A
//! torn write can only damage the block being written, so the other one is
//! still valid (DESIGN r6). No rename is needed, so this also works on raw
//! block device backends.
//!
//! ```text
//! 0 magic u32 | 4 version u32 | 8 seq u64 | 16 state u8 | 20 timeline u32
//! 24 checkpoint u64 | 32 redo u64 | 40 next_xid u64 | ... | 4092 crc u32
//! ```

use std::sync::Arc;

use super::{Result, WalError, fatal};
use crate::storage::backend::{FenceSource, FileKey, StorageBackend};
use crate::storage::page::Lsn;
use crate::storage::{AlignedBuf, IO_ALIGN};
use crate::txn::Xid;

const MAGIC: u32 = 0x4D59_4442; // "MYDB"
const VERSION: u32 = 1;
const CRC_OFF: usize = IO_ALIGN - 4;

/// Cluster state recorded in the control file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DbState {
    /// Cleanly shut down; no recovery needed.
    Shutdown = 1,
    /// Running (or crashed while running).
    InProduction = 2,
    /// Recovery was in progress.
    InRecovery = 3,
}

impl DbState {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Shutdown),
            2 => Some(Self::InProduction),
            3 => Some(Self::InRecovery),
            _ => None,
        }
    }
}

/// Control file contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlData {
    pub state: DbState,
    pub timeline: u32,
    /// Start of the latest checkpoint record.
    pub checkpoint: Lsn,
    /// Where redo starts for that checkpoint.
    pub redo: Lsn,
    /// Next transaction ID to assign (used from the transaction step).
    pub next_xid: Xid,
}

impl ControlData {
    fn encode(&self, seq: u64, block: &mut [u8]) {
        block.fill(0);
        block[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        block[4..8].copy_from_slice(&VERSION.to_le_bytes());
        block[8..16].copy_from_slice(&seq.to_le_bytes());
        block[16] = self.state as u8;
        block[20..24].copy_from_slice(&self.timeline.to_le_bytes());
        block[24..32].copy_from_slice(&self.checkpoint.0.to_le_bytes());
        block[32..40].copy_from_slice(&self.redo.0.to_le_bytes());
        block[40..48].copy_from_slice(&self.next_xid.0.to_le_bytes());
        let crc = crc32c::crc32c(&block[..CRC_OFF]);
        block[CRC_OFF..].copy_from_slice(&crc.to_le_bytes());
    }

    /// Returns `(seq, data)` if the block is valid.
    fn decode(block: &[u8]) -> Option<(u64, Self)> {
        let u32_at =
            |i: usize| u32::from_le_bytes([block[i], block[i + 1], block[i + 2], block[i + 3]]);
        let u64_at = |i: usize| {
            let mut a = [0u8; 8];
            a.copy_from_slice(&block[i..i + 8]);
            u64::from_le_bytes(a)
        };
        if u32_at(0) != MAGIC
            || u32_at(4) != VERSION
            || crc32c::crc32c(&block[..CRC_OFF]) != u32_at(CRC_OFF)
        {
            return None;
        }
        Some((
            u64_at(8),
            Self {
                state: DbState::from_u8(block[16])?,
                timeline: u32_at(20),
                checkpoint: Lsn(u64_at(24)),
                redo: Lsn(u64_at(32)),
                next_xid: Xid(u64_at(40)),
            },
        ))
    }
}

/// Handle for updating the control file.
#[derive(Debug)]
pub struct ControlFile {
    backend: Arc<dyn StorageBackend>,
    fence: Arc<dyn FenceSource>,
    seq: u64,
}

impl ControlFile {
    /// Creates the control file with initial contents (initdb).
    pub fn create(
        backend: Arc<dyn StorageBackend>,
        fence: Arc<dyn FenceSource>,
        data: &ControlData,
    ) -> Result<Self> {
        backend.create(FileKey::Control, fence.fence_token())?;
        let zeros = AlignedBuf::new(2 * IO_ALIGN)?;
        backend.write_at(FileKey::Control, 0, &zeros, fence.fence_token())?;
        let mut cf = Self {
            backend,
            fence,
            seq: 0,
        };
        cf.write(data);
        Ok(cf)
    }

    /// Reads the current contents.
    pub fn open(
        backend: Arc<dyn StorageBackend>,
        fence: Arc<dyn FenceSource>,
    ) -> Result<(Self, ControlData)> {
        let mut buf = AlignedBuf::new(2 * IO_ALIGN)?;
        backend.read_at(FileKey::Control, 0, &mut buf)?;
        let best = [&buf[..IO_ALIGN], &buf[IO_ALIGN..]]
            .into_iter()
            .filter_map(ControlData::decode)
            .max_by_key(|(seq, _)| *seq)
            .ok_or(WalError::ControlCorrupt)?;
        Ok((
            Self {
                backend,
                fence,
                seq: best.0,
            },
            best.1,
        ))
    }

    /// Durably replaces the contents by writing the other block. Aborts the
    /// process if the write or sync fails.
    pub fn write(&mut self, data: &ControlData) {
        let seq = self.seq + 1;
        let slot = seq % 2;
        let res = AlignedBuf::new(IO_ALIGN)
            .map_err(WalError::from)
            .and_then(|mut block| {
                data.encode(seq, &mut block);
                self.backend.write_at(
                    FileKey::Control,
                    slot * IO_ALIGN as u64,
                    &block,
                    self.fence.fence_token(),
                )?;
                self.backend.sync(FileKey::Control)?;
                Ok(())
            });
        if let Err(e) = res {
            fatal("control file update failed", &e);
        }
        self.seq = seq;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::{FenceToken, FixedFence};
    use crate::storage::mem::MemBackend;

    fn fence() -> Arc<dyn FenceSource> {
        Arc::new(FixedFence(FenceToken::new(1)))
    }

    fn data(n: u64) -> ControlData {
        ControlData {
            state: DbState::InProduction,
            timeline: 1,
            checkpoint: Lsn(n * 100),
            redo: Lsn(n * 90),
            next_xid: Xid(n),
        }
    }

    #[test]
    fn latest_write_wins() {
        let mem = Arc::new(MemBackend::default());
        let mut cf = ControlFile::create(mem.clone(), fence(), &data(1)).unwrap();
        for n in 2..6 {
            cf.write(&data(n));
            let (_, got) = ControlFile::open(mem.clone(), fence()).unwrap();
            assert_eq!(got, data(n));
        }
    }

    #[test]
    fn torn_latest_block_falls_back_to_previous() {
        let mem = Arc::new(MemBackend::default());
        let mut cf = ControlFile::create(mem.clone(), fence(), &data(1)).unwrap();
        cf.write(&data(2)); // seq 2 -> slot 0
        mem.corrupt(FileKey::Control, 30);
        let (mut cf2, got) = ControlFile::open(mem.clone(), fence()).unwrap();
        assert_eq!(got, data(1));
        // Writing again continues after the surviving sequence number.
        cf2.write(&data(3));
        assert_eq!(ControlFile::open(mem.clone(), fence()).unwrap().1, data(3));
    }

    #[test]
    fn both_blocks_bad_is_an_error() {
        let mem = Arc::new(MemBackend::default());
        let mut cf = ControlFile::create(mem.clone(), fence(), &data(1)).unwrap();
        cf.write(&data(2));
        mem.corrupt(FileKey::Control, 30);
        mem.corrupt(FileKey::Control, IO_ALIGN + 30);
        assert!(matches!(
            ControlFile::open(mem.clone(), fence()),
            Err(WalError::ControlCorrupt)
        ));
    }
}
