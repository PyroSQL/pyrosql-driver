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
mod complete;
mod format;
mod meta;
mod repl;
mod session;
mod sqltok;
mod vars;

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
    // Completion-script generator: prints and exits BEFORE any
    // connection attempt, so packagers can call it without a running
    // server.
    if let Some(shell) = cli.completions {
        use clap::CommandFactory;
        let mut cmd = args::Cli::command();
        let name = cmd.get_name().to_string();
        clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
        return Ok(());
    }
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
