mod config;
mod pool;
mod proxy;
mod stats;

use crate::config::{CliArgs, Config};
use crate::pool::ConnectionPool;
use crate::stats::PoolStats;
use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let config: Config = args.into();

    info!(
        "pyrosql-pooler starting: listen={} upstream={} pool_size={} mode={}",
        config.listen_addr, config.upstream_addr, config.pool_size, config.pool_mode
    );

    let stats = PoolStats::new();
    let pool = ConnectionPool::new(
        config.upstream_addr.clone(),
        config.pool_size,
        config.max_wait,
        Arc::clone(&stats),
    );

    // Start the health-check background task.
    let health_pool = Arc::clone(&pool);
    let health_stats = Arc::clone(&stats);
    let health_interval = config.health_check_interval;
    let health_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(health_interval);
        loop {
            interval.tick().await;
            health_pool.health_check_idle(&health_stats).await;
        }
    });

    // Bind the listener.
    let listener = match TcpListener::bind(&config.listen_addr).await {
        Ok(l) => {
            info!("listening on {}", config.listen_addr);
            l
        }
        Err(e) => {
            error!("failed to bind {}: {}", config.listen_addr, e);
            std::process::exit(1);
        }
    };

    let pool_mode = config.pool_mode;
    let shutdown = signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _addr)) => {
                        let pool = Arc::clone(&pool);
                        let stats = Arc::clone(&stats);
                        tokio::spawn(async move {
                            proxy::handle_client(stream, pool, pool_mode, stats).await;
                        });
                    }
                    Err(e) => {
                        error!("accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, draining pool...");
                health_handle.abort();
                pool.drain().await;
                info!("pyrosql-pooler stopped");
                break;
            }
        }
    }
}
