//! The replication slot: created strictly before the snapshot, so any change the snapshot can't
//! see commits after the slot's consistent point and is delivered by the stream. Its consistent
//! point is the LSN the stream resumes from.

use tokio_postgres::Client;

/// WARNING: tokio-postgres 0.7 has no replication mode, so this SQL variant of CREATE_REPLICATION_SLOT is used,
/// which does not export a snapshot. This means the seam is at least once not exactly once.
pub async fn create_replication_slot(client: &Client, name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let row = client
        .query_one("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'wal2json')", &[&name])
        .await?;
    Ok(row.get(0))
}

/// Drop the slot after cutover so retained WAL can be reclaimed.
pub async fn drop_replication_slot(client: &Client, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&name],
        )
        .await?;
    Ok(())
}
