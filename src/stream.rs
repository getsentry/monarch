//! Stream: consume logical-replication changes from the slot's consistent point and apply the
//! in-scope ones to the sink, keeping it current until cutover. The slot is created before the
//! snapshot, which pins its read to the slot's exported snapshot -- so the two phases meet at
//! one WAL position, with no row missed or applied twice.

use std::collections::{HashMap, HashSet};

/// WAL position. The slot's consistent point is the seam: the snapshot reads up to it, the
/// stream consumes from it -- so the two phases meet with no row missed or applied twice.
pub type Lsn = u64;

/// Per-table in-scope row ids. The snapshot seeds it; the stream grows it as rows come into scope.
pub type Membership = HashMap<String, HashSet<i64>>;

/// Create the slot before the snapshot: its consistent point is the `lsn` the stream later
/// resumes from, and its exported snapshot is what the snapshot phase pins its read to.
pub async fn create_slot(name: &str) -> Result<Lsn, Box<dyn std::error::Error>> {
    todo!("CREATE_REPLICATION_SLOT {name} LOGICAL pgoutput; return its consistent point")
}

/// Resume from `lsn` (the slot's consistent point), keep the in-scope changes, and apply them
/// to the sink until cutover.
pub async fn run_stream(slot: &str, lsn: Lsn, membership: Membership) -> Result<(), Box<dyn std::error::Error>> {
    todo!("START_REPLICATION SLOT {slot} at {lsn}; loop: decode -> in-scope? -> apply -> advance")
}
