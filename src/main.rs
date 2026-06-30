mod snapshot;
mod stream;

use std::fs;

use clap::{Parser, Subcommand};
use tokio_postgres::NoTls;

const DSN: &str = "host=127.0.0.1 port=5432 user=monarch password=monarch dbname=source";
const CONFIG: &str = "postgres_config.yaml";

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().cmd {
        Cmd::Snapshot { org_id } => run_snapshot(org_id).await?,
    }
    Ok(())
}

async fn run_snapshot(org_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, connection) = tokio_postgres::connect(DSN, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let cfg: snapshot::Config = serde_yaml::from_str(&fs::read_to_string(CONFIG)?)?;
    snapshot::run_snapshot(&mut client, &cfg, org_id).await?;
    Ok(())
}
