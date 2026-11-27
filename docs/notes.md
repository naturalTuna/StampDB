# Implementation notes

Items found during implementation that are not (yet) decisions in
`DESIGN.md`. Each one should be resolved in chat review and, once settled,
moved into `DESIGN.md`.

## Open

### Buffer pool: no victim because every flush was deferred (ha mode)

- Where: `BufferPool::get_victim` (src/buffer/pool.rs).
- Current behavior: when `FlushControl::may_flush` refuses, the frame is
  skipped and counts as unusable in the clock-sweep. After `n` consecutive
  unusable frames, `BufferError::Exhausted` is returned (PostgreSQL's
  `StrategyGetBuffer` give-up rule).
- Single mode: `SingleModeFlush` never defers, so Exhausted only comes from
  pins. This behavior is kept.
- ha mode (flush constraint, step 20): deferral happens when a page's LSN is
  ahead of some reader's replay LSN. Open question: on "no victim because of
  deferral", should the writer
  - (a) return Exhausted (current),
  - (b) wait for readers to catch up (needs a wait bound / timeout policy), or
  - (c) drop the lagging reader from the cluster and retry (ties into the
    "reader lag limit" in `ha_and_backup_design`)?
- Decide before step 20.

## Constraints to enforce later

### Buffer pool size vs. concurrent pins

- The clock hand is shared by all sweepers, so a thread's consecutive steps
  are not consecutive frames. With a pool barely larger than the number of
  concurrent pins, the PG give-up rule can report Exhausted while an
  unpinned frame exists (observed in tests with frames == threads).
- PostgreSQL has the same property and relies on shared_buffers being much
  larger than the number of concurrent pins.
- Plan: when `max_connections` exists (step 13), validate at startup that
  the pool size is well above `max_connections × max pins per session`.
  Tests size pools as `threads × max pins per thread + slack` and treat
  Exhausted under that sizing as a failure.
