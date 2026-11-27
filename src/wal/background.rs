//! Background threads: the checkpointer and the background writer.
//!
//! The checkpointer runs a checkpoint when `checkpoint_timeout` has passed
//! since the last one, or when the WAL written since the last redo point
//! exceeds `checkpoint_wal_bytes`. The background writer periodically
//! writes dirty pages that are about to be replaced, so sessions rarely
//! have to write a page themselves to get a free frame.
//!
//! Errors are reported on stderr and retried on the next round (a failed
//! data sync inside a checkpoint aborts the process, as everywhere).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use super::recovery::Engine;

/// Tuning of the background threads.
#[derive(Debug, Clone, Copy)]
pub struct BackgroundConfig {
    pub checkpoint_timeout: Duration,
    /// WAL bytes since the last redo point that trigger a checkpoint.
    pub checkpoint_wal_bytes: u64,
    pub bgwriter_delay: Duration,
    /// Pages written per background writer round at most.
    pub bgwriter_max_pages: usize,
}

impl Default for BackgroundConfig {
    fn default() -> Self {
        Self {
            checkpoint_timeout: Duration::from_secs(300),
            checkpoint_wal_bytes: 256 << 20,
            bgwriter_delay: Duration::from_millis(200),
            bgwriter_max_pages: 100,
        }
    }
}

/// How often the checkpointer re-checks its triggers.
const CHECKPOINTER_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Default)]
struct StopSignal {
    stop: AtomicBool,
    lock: Mutex<()>,
    cv: Condvar,
}

impl StopSignal {
    /// Sleeps up to `d`; returns true if stop was requested.
    fn sleep(&self, d: Duration) -> bool {
        let mut g = self.lock.lock();
        if !self.stop.load(Ordering::SeqCst) {
            self.cv.wait_for(&mut g, d);
        }
        self.stop.load(Ordering::SeqCst)
    }
}

/// Running background threads; stop them with [`Background::stop`].
#[derive(Debug)]
pub struct Background {
    signal: Arc<StopSignal>,
    handles: Vec<JoinHandle<()>>,
}

impl Background {
    /// Starts the checkpointer and the background writer.
    pub fn start(engine: &Arc<Engine>, cfg: BackgroundConfig) -> std::io::Result<Self> {
        let signal = Arc::new(StopSignal::default());
        let ckpt = {
            let (engine, signal) = (engine.clone(), signal.clone());
            std::thread::Builder::new()
                .name("checkpointer".into())
                .spawn(move || checkpointer(&engine, &signal, cfg))?
        };
        let bgw = {
            let (engine, signal) = (engine.clone(), signal.clone());
            std::thread::Builder::new()
                .name("bgwriter".into())
                .spawn(move || bgwriter(&engine, &signal, cfg))?
        };
        Ok(Self {
            signal,
            handles: vec![ckpt, bgw],
        })
    }

    /// Wakes the threads, asks them to finish, and waits for them.
    pub fn stop(self) {
        self.signal.stop.store(true, Ordering::SeqCst);
        {
            let _g = self.signal.lock.lock();
            self.signal.cv.notify_all();
        }
        for h in self.handles {
            let _ = h.join();
        }
    }
}

fn checkpointer(engine: &Engine, signal: &StopSignal, cfg: BackgroundConfig) {
    let mut last = Instant::now();
    while !signal.sleep(CHECKPOINTER_POLL) {
        let wal = engine.wal();
        let since_redo = wal.insert_lsn().0.saturating_sub(wal.redo_ptr().0);
        if last.elapsed() < cfg.checkpoint_timeout && since_redo < cfg.checkpoint_wal_bytes {
            continue;
        }
        if let Err(e) = engine.checkpoint() {
            eprintln!("checkpoint failed: {e}");
        }
        last = Instant::now();
    }
}

fn bgwriter(engine: &Engine, signal: &StopSignal, cfg: BackgroundConfig) {
    while !signal.sleep(cfg.bgwriter_delay) {
        if let Err(e) = engine.pool().bgwriter_pass(cfg.bgwriter_max_pages) {
            eprintln!("background writer failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::SingleModeFlush;
    use crate::storage::backend::{FenceToken, FixedFence};
    use crate::storage::mem::MemBackend;
    use crate::wal::{WalConfig, initdb};

    fn engine() -> Arc<Engine> {
        let mem = Arc::new(MemBackend::default());
        let cfg = WalConfig {
            timeline: 1,
            segment_size: 64 * 1024,
        };
        initdb(mem.clone(), Arc::new(FixedFence(FenceToken::new(1))), cfg).unwrap();
        Arc::new(
            Engine::start(
                mem,
                Arc::new(SingleModeFlush::new(FenceToken::new(1))),
                cfg,
                16,
            )
            .unwrap(),
        )
    }

    fn wait_for(mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(Instant::now() < deadline, "condition not reached");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn checkpoint_on_timeout() {
        let e = engine();
        let redo = e.control_data().redo;
        let bg = Background::start(
            &e,
            BackgroundConfig {
                checkpoint_timeout: Duration::from_millis(20),
                ..BackgroundConfig::default()
            },
        )
        .unwrap();
        wait_for(|| e.control_data().redo > redo);
        bg.stop();
    }

    #[test]
    fn checkpoint_on_wal_volume() {
        use crate::wal::record::{Record, RmgrId};
        let e = engine();
        let redo = e.control_data().redo;
        let bg = Background::start(
            &e,
            BackgroundConfig {
                checkpoint_timeout: Duration::from_secs(3600),
                checkpoint_wal_bytes: 32 * 1024,
                ..BackgroundConfig::default()
            },
        )
        .unwrap();
        std::thread::sleep(CHECKPOINTER_POLL * 3);
        assert_eq!(e.control_data().redo, redo, "no trigger yet");
        let mut rec = Record::new(RmgrId::Heap, 0);
        rec.main = vec![0; 1000];
        for _ in 0..40 {
            e.wal().insert(&rec).unwrap();
        }
        wait_for(|| e.control_data().redo > redo);
        bg.stop();
    }

    #[test]
    fn stop_returns_promptly() {
        let e = engine();
        let bg = Background::start(&e, BackgroundConfig::default()).unwrap();
        let t = Instant::now();
        bg.stop();
        assert!(t.elapsed() < Duration::from_secs(2));
    }
}
