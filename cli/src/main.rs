//! `pyrosql` — a zero-tokio, psql-style REPL client for PyroSQL.
//!
//! Single binary that talks the PWire protocol to any PyroSQL server.
//! Everything is synchronous from the caller's viewpoint: the underlying
//! [`pyrosql::Client`] drives its own pyro-runtime on a dedicated OS
//! thread, so we can just `futures::executor::block_on` the async
//! entry points without a Tokio runtime.
//!
//! Packaged as `pyrosql-cli` (deb / rpm / apk) — installs the binary to
//! `/usr/bin/pyrosql`.  Coexists with the `pyrosql-driver` package which
//! ships `libpyrosql_ffi_pwire.so`.

#![deny(unsafe_code)]

mod args;
mod format;
mod meta;
mod repl;
mod session;
mod sqltok;

use std::process::ExitCode;

use args::Cli;
use clap::Parser;

fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("pyrosql: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    // Decide what to do based on args.  Mutually exclusive (clap enforces
    // it via the `conflicts_with` attributes on the Cli struct).
    if let Some(sql) = cli.command.clone() {
        session::run_one_shot(&cli, &sql)
    } else if let Some(path) = cli.file.clone() {
        session::run_file(&cli, &path)
    } else {
        repl::run_repl(&cli)
    }
}
