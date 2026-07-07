mod slot;
mod snapshot;
mod stream;

use std::fs;

use clap::{Parser, Subcommand};
use tokio_postgres::NoTls;

const SOURCE_DSN: &str = "host=127.0.0.1 port=5432 user=monarch password=monarch dbname=source";
const SINK_DSN: &str = "host=127.0.0.1 port=5432 user=monarch password=monarch dbname=sink";
// pinned pre-multi-store copy: the frozen reference doesn't track the manifest's store schema
const CONFIG: &str = "postgres_config.rust.yaml";

#[derive(Parser)]
#[command(name = "monarch", about = "Move an organization's data between Sentry cells")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Snapshot {
        #[arg(long)]
        org_id: i64,
    },
    /// Stream the org's changes from its slot to the sink until cutover. Requires a prior
    /// snapshot, which creates the slot.
    Stream {
        #[arg(long)]
        org_id: i64,
    },
    /// Create the org's replication slot and print its consistent point.
    CreateSlot {
        #[arg(long)]
        org_id: i64,
    },
    /// Drop the org's replication slot.
    DropSlot {
        #[arg(long)]
        org_id: i64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().cmd {
        Cmd::Snapshot { org_id } => run_snapshot(org_id).await?,
        Cmd::Stream { org_id } => run_stream(org_id).await?,
        Cmd::CreateSlot { org_id } => create_slot(org_id).await?,
        Cmd::DropSlot { org_id } => drop_slot(org_id).await?,
    }
    Ok(())
}

/// Connect to a database, spawning the connection driver task.
async fn connect(dsn: &str) -> Result<tokio_postgres::Client, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    Ok(client)
}

fn slot_name(org_id: i64) -> String {
    format!("monarch_org_{org_id}")
}

async fn create_slot(org_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect(SOURCE_DSN).await?;
    let slot = slot_name(org_id);
    let lsn = slot::create_replication_slot(&client, &slot).await?;
    println!("slot {slot} created at LSN {lsn} (stream resumes here)");
    Ok(())
}

async fn drop_slot(org_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect(SOURCE_DSN).await?;
    let slot = slot_name(org_id);
    slot::drop_replication_slot(&client, &slot).await?;
    println!("slot {slot} dropped");
    Ok(())
}

/// The file carrying membership from `snapshot` to `stream`. It must reflect what the snapshot
/// saw (i.e. what the sink holds), so the stream can route deletes of rows that later vanished
/// from the source. Stands in for deriving membership from the sink itself.
fn membership_path(org_id: i64) -> String {
    format!("membership_org_{org_id}.json")
}

async fn run_snapshot(org_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    // Slot strictly before snapshot: nothing is missed, but gap changes are seen by both phases
    // and may apply twice (see slot.rs) -- the at-least-once seam a regular connection allows.
    let mut source = connect(SOURCE_DSN).await?;
    let mut sink = connect(SINK_DSN).await?;
    let slot = slot_name(org_id);
    let lsn = slot::create_replication_slot(&source, &slot).await?;
    println!("slot {slot} created at LSN {lsn}\n");

    let cfg: snapshot::Config = serde_yaml::from_str(&fs::read_to_string(CONFIG)?)?;
    let membership = snapshot::run_snapshot(&mut source, &mut sink, &cfg, org_id).await?;
    fs::write(membership_path(org_id), serde_json::to_string_pretty(&membership)?)?;
    println!("\nmembership saved to {}", membership_path(org_id));
    Ok(())
}

async fn run_stream(org_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    // Resumes the slot the snapshot created -- the stream never creates one, so it can restart
    // freely without disturbing the seam.
    let membership = serde_json::from_str(&fs::read_to_string(membership_path(org_id))
        .map_err(|_| format!("no {} -- run `snapshot --org-id {org_id}` first", membership_path(org_id)))?)?;
    let source = connect(SOURCE_DSN).await?;
    let sink = connect(SINK_DSN).await?;
    let cfg: snapshot::Config = serde_yaml::from_str(&fs::read_to_string(CONFIG)?)?;
    stream::run_stream(&source, &sink, &slot_name(org_id), &cfg, membership).await?;
    Ok(())
}
