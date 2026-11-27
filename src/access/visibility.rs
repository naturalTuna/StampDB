//! MVCC visibility of heap tuples (PostgreSQL's HeapTupleSatisfiesMVCC).
//!
//! The snapshot is consulted before hint bits (DESIGN: hint bit policy): a
//! reader may see a "committed" hint set after its snapshot was taken (on a
//! reader node, a hint newer than its replay point), and that transaction
//! must still look in progress to it. Hint bits only save commit log
//! lookups for transactions the snapshot already considers finished.
//!
//! The result carries the hint bits that could be set. They are only
//! suggested when the transaction manager reports a final outcome, which
//! for a commit happens only after its commit record is durable and the
//! transaction has left the running set.

use super::tuple::{TupleHeader, infomask};
use crate::txn::{CommandId, Snapshot, TransactionManager, Xid, XidStatus};

/// Result of a visibility check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Visibility {
    pub visible: bool,
    /// Hint bits that may be set on the tuple (opportunistically).
    pub hints: u16,
}

/// The checking transaction's own identity, for tuples it wrote itself.
#[derive(Debug, Clone, Copy)]
pub struct Me {
    /// Its xid, if it has written anything yet.
    pub xid: Option<Xid>,
    /// The current command: changes made by earlier commands are visible.
    pub cid: CommandId,
}

/// Outcome of a transaction as far as a snapshot is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// The checking transaction itself.
    Mine,
    /// Running (or not yet started) for this snapshot.
    InProgress,
    Committed,
    Aborted,
}

/// Which infomask bits describe one of xmin/xmax.
#[derive(Debug, Clone, Copy)]
struct HintBits {
    committed: u16,
    invalid: u16,
}

const XMIN_BITS: HintBits = HintBits {
    committed: infomask::XMIN_COMMITTED,
    invalid: infomask::XMIN_INVALID,
};

const XMAX_BITS: HintBits = HintBits {
    committed: infomask::XMAX_COMMITTED,
    invalid: infomask::XMAX_INVALID,
};

fn classify(
    xid: Xid,
    me: &Me,
    snap: &Snapshot,
    mask: u16,
    bits: HintBits,
    tm: &dyn TransactionManager,
    hints: &mut u16,
) -> crate::txn::Result<Seen> {
    if Some(xid) == me.xid {
        return Ok(Seen::Mine);
    }
    if mask & bits.invalid != 0 {
        return Ok(Seen::Aborted);
    }
    // Snapshot first, even if a hint says committed.
    if snap.is_running(xid) {
        return Ok(Seen::InProgress);
    }
    if mask & bits.committed != 0 {
        return Ok(Seen::Committed);
    }
    Ok(match tm.status(xid)? {
        XidStatus::Committed => {
            *hints |= bits.committed;
            Seen::Committed
        }
        XidStatus::Aborted => {
            *hints |= bits.invalid;
            Seen::Aborted
        }
        // Finished for the snapshot but not according to the manager:
        // cannot happen for a consistent snapshot; treat as invisible.
        XidStatus::InProgress => Seen::InProgress,
    })
}

/// Whether the tuple version is visible to `snap` (and `me`).
pub fn satisfies_mvcc(
    h: &TupleHeader,
    snap: &Snapshot,
    me: &Me,
    tm: &dyn TransactionManager,
) -> crate::txn::Result<Visibility> {
    let mut hints = 0;
    let mask = h.infomask;
    let xmin = classify(h.xmin, me, snap, mask, XMIN_BITS, tm, &mut hints)?;
    let inserted = match xmin {
        Seen::Mine => h.cmin < me.cid,
        Seen::Committed => true,
        Seen::InProgress | Seen::Aborted => false,
    };
    if !inserted {
        return Ok(Visibility {
            visible: false,
            hints,
        });
    }

    if !h.xmax.is_valid() || mask & infomask::XMAX_INVALID != 0 {
        return Ok(Visibility {
            visible: true,
            hints,
        });
    }
    let xmax = classify(h.xmax, me, snap, mask, XMAX_BITS, tm, &mut hints)?;
    let deleted = match xmax {
        Seen::Mine => h.cmax < me.cid,
        Seen::Committed => true,
        Seen::InProgress | Seen::Aborted => false,
    };
    Ok(Visibility {
        visible: !deleted,
        hints,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tuple::Tid;
    use crate::storage::page::BlockNumber;
    use crate::storage::slotted::SlotId;
    use crate::txn::{TxnId, XidStatus};
    use std::collections::HashMap;

    /// Fixed statuses; xids not listed are in progress.
    #[derive(Debug)]
    struct FakeTm(HashMap<Xid, XidStatus>);

    impl TransactionManager for FakeTm {
        fn begin(&self) -> TxnId {
            TxnId(0)
        }
        fn assign_xid(&self) -> crate::txn::Result<Xid> {
            Ok(Xid::INVALID)
        }
        fn snapshot(&self) -> Snapshot {
            unreachable!()
        }
        fn status(&self, xid: Xid) -> crate::txn::Result<XidStatus> {
            Ok(self.0.get(&xid).copied().unwrap_or(XidStatus::InProgress))
        }
        fn commit(&self, _: Xid) -> crate::txn::Result<()> {
            Ok(())
        }
        fn abort(&self, _: Xid) -> crate::txn::Result<()> {
            Ok(())
        }
    }

    // Cast: me = 20; committed before the snapshot = 10; aborted = 11;
    // running at snapshot time = 12 (since committed: a hint may say so);
    // started after the snapshot = 30 (committed now).
    const ME: Xid = Xid(20);
    const COMMITTED: Xid = Xid(10);
    const ABORTED: Xid = Xid(11);
    const RUNNING: Xid = Xid(12);
    const LATER: Xid = Xid(30);

    fn tm() -> FakeTm {
        FakeTm(HashMap::from([
            (COMMITTED, XidStatus::Committed),
            (ABORTED, XidStatus::Aborted),
            (RUNNING, XidStatus::Committed),
            (LATER, XidStatus::Committed),
        ]))
    }

    fn snap() -> Snapshot {
        Snapshot {
            xmin: Xid(12),
            xmax: Xid(21),
            running: vec![RUNNING, ME],
        }
    }

    fn me() -> Me {
        Me {
            xid: Some(ME),
            cid: CommandId(5),
        }
    }

    fn header(xmin: Xid, cmin: u32, xmax: Xid, cmax: u32, mask: u16) -> TupleHeader {
        TupleHeader {
            xmin,
            xmax,
            cmin: CommandId(cmin),
            cmax: CommandId(cmax),
            ctid: Tid {
                blk: BlockNumber(0),
                slot: SlotId(0),
            },
            infomask: mask,
            natts: 0,
            hoff: 35,
        }
    }

    fn vis(xmin: Xid, cmin: u32, xmax: Xid, cmax: u32, mask: u16) -> Visibility {
        satisfies_mvcc(&header(xmin, cmin, xmax, cmax, mask), &snap(), &me(), &tm()).unwrap()
    }

    fn visible(xmin: Xid, xmax: Xid) -> bool {
        vis(xmin, 0, xmax, 0, 0).visible
    }

    #[test]
    fn inserter_outcomes() {
        assert!(visible(COMMITTED, Xid::INVALID));
        assert!(!visible(ABORTED, Xid::INVALID));
        assert!(!visible(RUNNING, Xid::INVALID), "running at snapshot time");
        assert!(!visible(LATER, Xid::INVALID), "started after the snapshot");
    }

    #[test]
    fn deleter_outcomes() {
        assert!(!visible(COMMITTED, COMMITTED));
        assert!(visible(COMMITTED, ABORTED));
        assert!(
            visible(COMMITTED, RUNNING),
            "delete not yet committed for us"
        );
        assert!(visible(COMMITTED, LATER));
    }

    #[test]
    fn own_changes_follow_command_ids() {
        assert!(vis(ME, 4, Xid::INVALID, 0, 0).visible, "earlier command");
        assert!(!vis(ME, 5, Xid::INVALID, 0, 0).visible, "current command");
        assert!(
            !vis(ME, 1, ME, 3, 0).visible,
            "deleted by an earlier command"
        );
        assert!(
            vis(ME, 1, ME, 5, 0).visible,
            "deleted by the current command"
        );
        assert!(!vis(COMMITTED, 0, ME, 4, 0).visible);
        assert!(vis(COMMITTED, 0, ME, 6, 0).visible);
    }

    #[test]
    fn snapshot_wins_over_committed_hint() {
        let v = vis(RUNNING, 0, Xid::INVALID, 0, infomask::XMIN_COMMITTED);
        assert!(!v.visible, "hint set after our snapshot must not leak");
        let v = vis(COMMITTED, 0, RUNNING, 0, infomask::XMAX_COMMITTED);
        assert!(v.visible);
    }

    #[test]
    fn hints_are_suggested_only_for_final_outcomes() {
        assert_eq!(
            vis(COMMITTED, 0, Xid::INVALID, 0, 0).hints,
            infomask::XMIN_COMMITTED
        );
        assert_eq!(
            vis(ABORTED, 0, Xid::INVALID, 0, 0).hints,
            infomask::XMIN_INVALID
        );
        assert_eq!(
            vis(COMMITTED, 0, ABORTED, 0, 0).hints,
            infomask::XMIN_COMMITTED | infomask::XMAX_INVALID
        );
        assert_eq!(
            vis(COMMITTED, 0, COMMITTED, 0, 0).hints,
            infomask::XMIN_COMMITTED | infomask::XMAX_COMMITTED
        );
        assert_eq!(vis(RUNNING, 0, Xid::INVALID, 0, 0).hints, 0);
        assert_eq!(
            vis(COMMITTED, 0, RUNNING, 0, 0).hints,
            infomask::XMIN_COMMITTED
        );
        assert_eq!(vis(ME, 1, Xid::INVALID, 0, 0).hints, 0);
        // Existing hints avoid lookups and are not suggested again.
        assert_eq!(
            vis(COMMITTED, 0, Xid::INVALID, 0, infomask::XMIN_COMMITTED).hints,
            0
        );
    }

    #[test]
    fn invalid_hints_short_circuit() {
        let unknown = Xid(15); // not in the fake manager: would be "in progress"
        assert!(!vis(unknown, 0, Xid::INVALID, 0, infomask::XMIN_INVALID).visible);
        assert!(vis(COMMITTED, 0, unknown, 0, infomask::XMAX_INVALID).visible);
    }

    #[test]
    fn transaction_without_xid() {
        let reader = Me {
            xid: None,
            cid: CommandId(0),
        };
        let v = satisfies_mvcc(
            &header(COMMITTED, 0, Xid::INVALID, 0, 0),
            &snap(),
            &reader,
            &tm(),
        )
        .unwrap();
        assert!(v.visible);
    }
}
