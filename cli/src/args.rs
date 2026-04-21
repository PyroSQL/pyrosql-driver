//! Clap argument definitions for `pyrosql`.
//!
//! The UX mirrors `psql` closely: `-h/--host`, `-p/--port`, `-U/--user`,
//! `-d/--database`, `-W` to prompt for password, `-c/--command` for
//! one-shot, `-f/--file` for scripts.  A positional URL (`pyrosql://`,
//! `vsql://`, `postgres://`, …) is accepted as an alternative; when
//! present it wins over the individual flags.

use clap::{Parser, ValueEnum};
use clap_complete::Shell;

/// Output format — table (pretty), json (machine-readable), or csv.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Pretty table via `comfy-table`.
    Table,
    /// Newline-delimited JSON objects, one per row.
    Json,
    /// RFC 4180 CSV (quoted when needed).
    Csv,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}

/// SQL dialect selector — the server applies the matching parser mode to
/// the whole session. Default is `pyro` (native); `pg` / `mysql` let users
/// migrating from those ecosystems keep their existing SQL dialect while
/// still using PWire-only features like LiveSync.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum Dialect {
    /// PyroSQL native syntax (accepts PG-compatible SQL too).
    Pyro,
    /// Strict PostgreSQL 15 compatibility.
    Pg,
    /// MySQL 8 compatibility (backticks, LIMIT x,y, etc.).
    Mysql,
}

impl Default for Dialect {
    fn default() -> Self {
        Self::Pyro
    }
}

impl Dialect {
    /// Map to the driver's SyntaxMode enum. Keeping the mapping here
    /// lets the CLI surface friendly short names (`pyro` / `pg` /
    /// `mysql`) without coupling the rest of the CLI to the driver's
    /// internal variants.
    pub fn to_syntax_mode(self) -> pyrosql::SyntaxMode {
        match self {
            Self::Pyro => pyrosql::SyntaxMode::PyroSQL,
            Self::Pg => pyrosql::SyntaxMode::PostgreSQL,
            Self::Mysql => pyrosql::SyntaxMode::MySQL,
        }
    }
}

/// `pyrosql` — interactive SQL REPL for PyroSQL servers.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "pyrosql",
    version,
    about = "PyroSQL command-line REPL (psql-equivalent)",
    long_about = None,
    // psql convention owns `-h` (host), so clap's auto `-h` for --help
    // would clash.  Turn off the auto help flag entirely and re-add a
    // long-only `--help` below.  `--version` keeps its auto `-V`.
    disable_help_flag = true,
)]
pub struct Cli {
    /// Connection URL.  When provided, overrides the -h/-p/-U/-d flags.
    ///
    /// Supported schemes: `pyrosql://`, `vsql://`, `postgres://`,
    /// `pg://`, `mysql://`, `unix://`, `auto://`.
    #[arg(value_name = "URL")]
    pub url: Option<String>,

    /// Print help (long-only — psql uses `-h` for host).
    #[arg(long, action = clap::ArgAction::Help)]
    pub help: Option<bool>,

    /// Server hostname or IP.
    #[arg(short = 'h', long, default_value = "localhost")]
    pub host: String,

    /// Server port (default: 12520 for PWire).
    #[arg(short = 'p', long, default_value_t = 12520_u16)]
    pub port: u16,

    /// Username.
    #[arg(short = 'U', long, default_value = "pyrosql")]
    pub user: String,

    /// Database name.
    #[arg(short = 'd', long, default_value = "pyrosql")]
    pub database: String,

    /// Password passed on the command line. INSECURE — visible in
    /// `ps aux`, shell history, process env.  Requires the
    /// `--i-know-password-in-cli-is-insecure` opt-in so nobody reaches
    /// for it by reflex.  Prefer `-W` (interactive prompt) or
    /// `PYROSQL_PASSWORD` environment variable.
    #[arg(long, requires = "i_know_password_in_cli_is_insecure")]
    pub password: Option<String>,

    /// Opt-in confirming you understand `--password` leaks the password
    /// to the process list.
    #[arg(long)]
    pub i_know_password_in_cli_is_insecure: bool,

    /// Prompt for password on stdin (no echo).
    #[arg(short = 'W', long = "password-prompt")]
    pub password_prompt: bool,

    /// Attempt a TLS handshake before PWire bringup.  (Not yet wired
    /// through the driver — see `rust/src/pwire.rs` TODO.)
    #[arg(long)]
    pub tls: bool,

    /// Disable TLS even if the server advertises it.
    #[arg(long, conflicts_with = "tls")]
    pub no_tls: bool,

    /// Execute a single SQL string and exit.
    #[arg(short = 'c', long, conflicts_with = "file")]
    pub command: Option<String>,

    /// Execute SQL script from file and exit.
    #[arg(short = 'f', long, conflicts_with = "command")]
    pub file: Option<String>,

    /// SQL dialect the session uses: `pyro` (default) / `pg` / `mysql`.
    /// Picks the parser mode the server applies to every statement for
    /// this connection; orthogonal to the wire protocol (always PWire).
    /// A URL that already carries `?dialect=…` wins over this flag.
    #[arg(short = 'D', long, value_enum, default_value_t = Dialect::Pyro)]
    pub dialect: Dialect,

    /// Output format (default: table).
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,

    /// Expanded display — one column per line, psql's `\x`.
    #[arg(short = 'x', long)]
    pub expanded: bool,

    /// Print per-statement wall-clock time (psql's `\timing`).
    #[arg(long)]
    pub timing: bool,

    /// Verbose error output — print full error chains.
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Quiet mode — suppress informational banners.
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Print a shell completion script to stdout and exit.
    /// Example:  `pyrosql --completions bash > /etc/bash_completion.d/pyrosql`
    #[arg(long, value_enum, value_name = "SHELL", hide_default_value = true)]
    pub completions: Option<Shell>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_validates() {
        // Fail early if the derive generates an invalid spec.
        Cli::command().debug_assert();
    }

    #[test]
    fn parse_host_port() {
        let cli = Cli::try_parse_from([
            "pyrosql", "-h", "db.example.com", "-p", "5555", "-U", "alice", "-d", "prod",
        ])
        .unwrap();
        assert_eq!(cli.host, "db.example.com");
        assert_eq!(cli.port, 5555);
        assert_eq!(cli.user, "alice");
        assert_eq!(cli.database, "prod");
        assert!(cli.url.is_none());
        assert!(cli.command.is_none());
        assert!(cli.file.is_none());
    }

    #[test]
    fn parse_url_positional() {
        let cli =
            Cli::try_parse_from(["pyrosql", "pyrosql://bob:pw@myhost:9999/mydb"]).unwrap();
        assert_eq!(cli.url.as_deref(), Some("pyrosql://bob:pw@myhost:9999/mydb"));
    }

    #[test]
    fn command_and_file_conflict() {
        let res =
            Cli::try_parse_from(["pyrosql", "-c", "SELECT 1", "-f", "/tmp/x.sql"]);
        assert!(res.is_err());
    }

    #[test]
    fn tls_and_no_tls_conflict() {
        let res = Cli::try_parse_from(["pyrosql", "--tls", "--no-tls"]);
        assert!(res.is_err());
    }

    #[test]
    fn format_default_is_table() {
        let cli = Cli::try_parse_from(["pyrosql"]).unwrap();
        assert_eq!(cli.format, OutputFormat::Table);
    }

    #[test]
    fn format_json_parses() {
        let cli = Cli::try_parse_from(["pyrosql", "--format", "json"]).unwrap();
        assert_eq!(cli.format, OutputFormat::Json);
    }

    #[test]
    fn expanded_short_flag() {
        let cli = Cli::try_parse_from(["pyrosql", "-x"]).unwrap();
        assert!(cli.expanded);
    }

    #[test]
    fn dialect_defaults_to_pyro() {
        let cli = Cli::try_parse_from(["pyrosql"]).unwrap();
        assert_eq!(cli.dialect, Dialect::Pyro);
    }

    #[test]
    fn dialect_short_flag_pg() {
        let cli = Cli::try_parse_from(["pyrosql", "-D", "pg"]).unwrap();
        assert_eq!(cli.dialect, Dialect::Pg);
        assert_eq!(cli.dialect.to_syntax_mode(), pyrosql::SyntaxMode::PostgreSQL);
    }

    #[test]
    fn dialect_long_flag_mysql() {
        let cli = Cli::try_parse_from(["pyrosql", "--dialect", "mysql"]).unwrap();
        assert_eq!(cli.dialect, Dialect::Mysql);
        assert_eq!(cli.dialect.to_syntax_mode(), pyrosql::SyntaxMode::MySQL);
    }

    #[test]
    fn dialect_rejects_unknown_value() {
        let res = Cli::try_parse_from(["pyrosql", "--dialect", "oracle"]);
        assert!(res.is_err());
    }
}
