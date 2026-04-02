mod dump;
mod format;
mod pwire;
mod restore;

use anyhow::Result;
use clap::Parser;
use std::io::{self, BufWriter};

/// PyroSQL database backup and restore tool.
///
/// Dump mode (default):
///   pyrosql-dump --host H --port P --user U --password P --database D > backup.sql
///
/// Restore mode:
///   pyrosql-dump --restore --host H --port P --user U --password P --database D < backup.sql
#[derive(Parser, Debug)]
#[command(name = "pyrosql-dump", version = "1.0.0", about = "Backup/restore tool for PyroSQL")]
struct Args {
    /// PyroSQL server host
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// PyroSQL server port
    #[arg(long, default_value_t = 3807)]
    port: u16,

    /// Username for authentication
    #[arg(long, short = 'u', default_value = "root")]
    user: String,

    /// Password for authentication
    #[arg(long, short = 'p', default_value = "")]
    password: String,

    /// Database name
    #[arg(long, short = 'd')]
    database: String,

    /// Restore mode: read SQL from stdin and execute it
    #[arg(long)]
    restore: bool,

    /// Only dump specific tables (comma-separated)
    #[arg(long, value_delimiter = ',')]
    tables: Option<Vec<String>>,

    /// Dump only schema (CREATE TABLE), no data
    #[arg(long)]
    schema_only: bool,

    /// Dump only data (INSERT), no schema
    #[arg(long)]
    data_only: bool,

    /// Number of rows per INSERT statement batch
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.schema_only && args.data_only {
        eprintln!("Error: --schema-only and --data-only are mutually exclusive");
        std::process::exit(1);
    }

    let mut client = pwire::PwireClient::connect(&args.host, args.port, &args.user, &args.password)
        .await?;

    if args.restore {
        // Restore mode: read from stdin
        let mut stdin = io::stdin().lock();
        let config = restore::RestoreConfig {
            database: args.database,
        };
        restore::restore_database(&mut client, &config, &mut stdin).await?;
    } else {
        // Dump mode: write to stdout
        let stdout = io::stdout().lock();
        let mut writer = BufWriter::new(stdout);
        let config = dump::DumpConfig {
            database: args.database,
            tables: args.tables,
            schema_only: args.schema_only,
            data_only: args.data_only,
            batch_size: args.batch_size,
        };
        dump::dump_database(&mut client, &config, &mut writer).await?;
    }

    client.close().await?;
    Ok(())
}
