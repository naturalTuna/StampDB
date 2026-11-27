//! Buffer pool stress test over the real O_DIRECT backend.
//!
//! Several threads increment per-page counters through a pool much smaller
//! than the relation, forcing constant eviction and write-back. After a full
//! flush, a fresh pool must read back exactly the expected counts.

use std::sync::Arc;

use mydb::buffer::{BufferError, BufferPool, BufferTag, SingleModeFlush};
use mydb::storage::backend::{FenceToken, RelFileId};
use mydb::storage::disk::DiskManager;
use mydb::storage::local::LocalDirBackend;
use mydb::storage::page::{BlockNumber, PageKind};
use mydb::storage::slotted::{SlotId, SlottedPage};

const FENCE: FenceToken = FenceToken::new(1);
const REL: RelFileId = RelFileId(16384);
const PAGES: u32 = 64;
const THREADS: usize = 4;
/// Each fetch holds at most one pin at a time.
const MAX_PINS_PER_THREAD: usize = 1;
/// Pool sized above the maximum number of concurrent pins, so Exhausted
/// here is a bug. PAGES is 8x larger to force constant eviction.
const FRAMES: usize = THREADS * MAX_PINS_PER_THREAD + 4;
const OPS_PER_THREAD: usize = 2000;

fn open_pool(dir: &std::path::Path) -> (Arc<DiskManager>, BufferPool) {
    let backend = Arc::new(LocalDirBackend::open(dir).unwrap());
    let disk = Arc::new(DiskManager::new(backend));
    let pool =
        BufferPool::new(FRAMES, disk.clone(), Arc::new(SingleModeFlush::new(FENCE))).unwrap();
    (disk, pool)
}

fn tag(blk: u32) -> BufferTag {
    BufferTag {
        rel: REL,
        blk: BlockNumber(blk),
    }
}

fn counter(page: &[u8]) -> u64 {
    let item = SlottedPage::open(page).unwrap();
    u64::from_le_bytes(item.get_item(SlotId(0)).unwrap().try_into().unwrap())
}

/// Small deterministic PRNG (xorshift64) so failures are reproducible.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn concurrent_increments_survive_eviction_and_reload() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let expected: Vec<u64> = {
        let (disk, pool) = open_pool(dir.path());
        disk.create_relation(REL, FENCE).unwrap();
        for _ in 0..PAGES {
            let (_blk, pin) = pool.extend(REL).unwrap();
            let mut w = pin.write();
            let mut p = SlottedPage::init(&mut w[..], PageKind::Heap, 0).unwrap();
            p.add_item(&0u64.to_le_bytes()).unwrap().unwrap();
            w.mark_dirty();
        }

        let per_thread: Vec<Vec<u64>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..THREADS)
                .map(|t| {
                    let pool = &pool;
                    s.spawn(move || {
                        let mut counts = vec![0u64; PAGES as usize];
                        let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64 + 1);
                        for _ in 0..OPS_PER_THREAD {
                            let blk = (next(&mut rng) % u64::from(PAGES)) as u32;
                            let pin = match pool.fetch(tag(blk)) {
                                Ok(pin) => pin,
                                Err(BufferError::Exhausted) => {
                                    panic!("Exhausted with {FRAMES} frames for {THREADS} threads")
                                }
                                Err(e) => panic!("fetch {blk}: {e}"),
                            };
                            let mut w = pin.write();
                            let mut p = SlottedPage::open(&mut w[..]).unwrap();
                            let item = p.get_item_mut(SlotId(0)).unwrap();
                            let v = u64::from_le_bytes((&item[..]).try_into().unwrap());
                            item.copy_from_slice(&(v + 1).to_le_bytes());
                            w.mark_dirty();
                            counts[blk as usize] += 1;
                        }
                        counts
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        // Pages still resident must already show the in-memory totals.
        let expected: Vec<u64> = (0..PAGES as usize)
            .map(|b| per_thread.iter().map(|c| c[b]).sum())
            .collect();
        for b in 0..PAGES {
            let pin = pool.fetch(tag(b)).unwrap();
            assert_eq!(counter(&pin.read()), expected[b as usize], "block {b}");
        }

        let stats = pool.flush_all().unwrap();
        assert_eq!(stats.deferred, 0);
        assert_eq!(pool.flush_all().unwrap().written, 0);
        expected
    };

    // Fresh pool and backend: everything comes from disk (checksums verified).
    let (disk, pool) = open_pool(dir.path());
    assert_eq!(disk.nblocks(REL).unwrap(), PAGES);
    let mut total = 0;
    for b in 0..PAGES {
        let c = counter(&pool.fetch(tag(b)).unwrap().read());
        assert_eq!(c, expected[b as usize], "block {b} after reload");
        total += c;
    }
    assert_eq!(total, (THREADS * OPS_PER_THREAD) as u64);
}
