//! MVCC snapshots.

use super::Xid;

/// Which transactions were running when the snapshot was taken.
///
/// An xid is treated as running ("not yet committed for this snapshot") if
/// it is at or above `xmax` (assigned after the snapshot) or listed in
/// `running`. Everything below `xmin` had finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Smallest xid still running (or `xmax` if none).
    pub xmin: Xid,
    /// First xid not yet assigned.
    pub xmax: Xid,
    /// Running xids in `[xmin, xmax)`, sorted.
    pub running: Vec<Xid>,
}

impl Snapshot {
    /// Whether `xid` must be treated as in progress by this snapshot.
    pub fn is_running(&self, xid: Xid) -> bool {
        if xid >= self.xmax {
            return true;
        }
        if xid < self.xmin {
            return false;
        }
        self.running.binary_search(&xid).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_rules() {
        let s = Snapshot {
            xmin: Xid(5),
            xmax: Xid(10),
            running: vec![Xid(5), Xid(8)],
        };
        assert!(!s.is_running(Xid(4)));
        assert!(s.is_running(Xid(5)));
        assert!(!s.is_running(Xid(6)));
        assert!(s.is_running(Xid(8)));
        assert!(!s.is_running(Xid(9)));
        assert!(s.is_running(Xid(10)));
        assert!(s.is_running(Xid(100)));
    }
}
