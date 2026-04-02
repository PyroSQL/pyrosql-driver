use clap::Parser;
use std::fmt;
use std::time::Duration;

/// Pool mode determines when a backend connection is returned to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMode {
    /// Connection returned after each transaction (COMMIT/ROLLBACK) or standalone query.
    Transaction,
    /// Connection held for the entire client session until disconnect.
    Session,
}

impl fmt::Display for PoolMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolMode::Transaction => write!(f, "transaction"),
            PoolMode::Session => write!(f, "session"),
        }
    }
}

impl std::str::FromStr for PoolMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "transaction" => Ok(PoolMode::Transaction),
            "session" => Ok(PoolMode::Session),
            other => Err(format!(
                "invalid pool mode '{}': expected 'transaction' or 'session'",
                other
            )),
        }
    }
}

/// CLI arguments for pyrosql-pooler.
#[derive(Parser, Debug, Clone)]
#[command(name = "pyrosql-pooler", about = "PWire connection pooler for PyroSQL")]
pub struct CliArgs {
    /// Address to listen on for client connections.
    #[arg(long, default_value = "127.0.0.1:12521")]
    pub listen: String,

    /// Upstream PyroSQL server address.
    #[arg(long, default_value = "127.0.0.1:12520")]
    pub upstream: String,

    /// Maximum number of pooled connections to the upstream server.
    #[arg(long, default_value_t = 20)]
    pub pool_size: usize,

    /// Pool mode: 'transaction' or 'session'.
    #[arg(long, default_value = "transaction")]
    pub pool_mode: PoolMode,

    /// Maximum time (in milliseconds) a client will wait for a connection from the pool.
    #[arg(long, default_value_t = 5000)]
    pub max_wait_ms: u64,

    /// Interval (in seconds) between health-check pings to idle connections.
    #[arg(long, default_value_t = 30)]
    pub health_check_interval_secs: u64,
}

/// Resolved configuration derived from CLI args.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub upstream_addr: String,
    pub pool_size: usize,
    pub pool_mode: PoolMode,
    pub max_wait: Duration,
    pub health_check_interval: Duration,
}

impl From<CliArgs> for Config {
    fn from(args: CliArgs) -> Self {
        Config {
            listen_addr: args.listen,
            upstream_addr: args.upstream,
            pool_size: args.pool_size,
            pool_mode: args.pool_mode,
            max_wait: Duration::from_millis(args.max_wait_ms),
            health_check_interval: Duration::from_secs(args.health_check_interval_secs),
        }
    }
}
