//! The replication slot: created strictly before the snapshot, so any change the snapshot can't
//! see commits after the slot's consistent point and is delivered by the stream. Its consistent
//! point is the LSN the stream resumes from.

use tokio_postgres::Client;

/// The publication pgoutput decodes through. FOR ALL TABLES: org filtering is consumer-side
/// anyway (PG14 has no publisher row filters).
pub const PUBLICATION: &str = "monarch";

/// WARNING: tokio-postgres 0.7 has no replication mode, so this SQL variant of CREATE_REPLICATION_SLOT is used,
/// which does not export a snapshot. This means the seam is at least once not exactly once.
pub async fn create_replication_slot(client: &Client, name: &str) -> Result<String, Box<dyn std::error::Error>> {
    ensure_publication(client).await?;
    let row = client
        .query_one("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')", &[&name])
        .await?;
    Ok(row.get(0))
}

/// Create the publication if missing (CREATE PUBLICATION has no IF NOT EXISTS).
async fn ensure_publication(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let exists = client.query_opt("SELECT 1 FROM pg_publication WHERE pubname = $1", &[&PUBLICATION]).await?;
    if exists.is_none() {
        client.execute(&format!("CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES"), &[]).await?;
    }
    Ok(())
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
