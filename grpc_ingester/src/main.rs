//! das-grpc-ingester: index a local Yellowstone gRPC feed straight into the DAS
//! database through `program_transformers` - no Redis, no plerkle, no nft_ingester.
//!
//! Meant to run one per validator, next to it: the firehose stays on loopback and
//! only the resulting writes leave the box.

mod convert;
mod cursor;
mod metadata;
mod retry;
mod stats;
mod stream;

use {
    anyhow::Context,
    clap::Parser,
    std::{path::PathBuf, sync::Arc, time::Duration},
    tokio::{signal::unix::SignalKind, sync::Semaphore},
    tracing::{error, info},
};

#[derive(Debug, Clone, Parser)]
#[command(version, about = "Index a Yellowstone gRPC feed into the DAS database")]
pub struct Config {
    /// Yellowstone gRPC endpoint.
    #[arg(long, env = "INGEST_GRPC_ENDPOINT", default_value = "http://127.0.0.1:10000")]
    pub grpc_endpoint: String,

    #[arg(long, env = "INGEST_GRPC_X_TOKEN")]
    pub grpc_x_token: Option<String>,

    /// Postgres-protocol URL of the DAS database (CockroachDB or Postgres).
    #[arg(long, env = "INGEST_DATABASE_URL")]
    pub database_url: String,

    #[arg(long, env = "INGEST_DATABASE_MAX_CONNECTIONS", default_value_t = 64)]
    pub database_max_connections: u32,

    /// Holds the resume cursor and the gap log.
    #[arg(long, env = "INGEST_STATE_DIR", default_value = "/var/lib/das-grpc-ingester")]
    pub state_dir: PathBuf,

    /// Writes in flight at once.
    #[arg(long, env = "INGEST_CONCURRENCY", default_value_t = 64)]
    pub concurrency: usize,

    /// Start from this slot, ignoring any saved cursor.
    #[arg(long, env = "INGEST_START_SLOT")]
    pub start_slot: Option<u64>,

    /// Attempts for a write the database asks us to retry.
    #[arg(long, env = "INGEST_MAX_WRITE_ATTEMPTS", default_value_t = 10)]
    pub max_write_attempts: u32,

    #[arg(long, env = "INGEST_STATS_INTERVAL_SECS", default_value_t = 10)]
    pub stats_interval_secs: u64,

    #[command(flatten)]
    pub metadata: metadata::MetadataArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config::parse();
    anyhow::ensure!(config.concurrency > 0, "--concurrency must be at least 1");

    tokio::fs::create_dir_all(&config.state_dir)
        .await
        .with_context(|| format!("creating {}", config.state_dir.display()))?;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
        .context("connecting to the database")?;

    let stats = Arc::new(stats::Stats::default());
    let notifier = metadata::spawn(
        config.metadata.clone(),
        pool.clone(),
        Arc::clone(&stats),
        config.max_write_attempts,
    )?;
    let transformer = Arc::new(program_transformers::ProgramTransformer::new(pool, notifier));
    let cursor = Arc::new(cursor::Cursor::load(&config.state_dir, config.start_slot).await?);
    let writers = Arc::new(Semaphore::new(config.concurrency));

    spawn_reporter(
        Duration::from_secs(config.stats_interval_secs.max(1)),
        Arc::clone(&stats),
        Arc::clone(&cursor),
        Arc::clone(&writers),
        config.concurrency,
    );

    stream::Ingest {
        config,
        transformer,
        cursor,
        stats,
        writers,
    }
    .run(shutdown_signal())
    .await
}

async fn shutdown_signal() {
    match tokio::signal::unix::signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        Err(e) => {
            error!(error = %e, "cannot listen for SIGTERM; only Ctrl-C will stop cleanly");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

fn spawn_reporter(
    interval: Duration,
    stats: Arc<stats::Stats>,
    cursor: Arc<cursor::Cursor>,
    writers: Arc<Semaphore>,
    concurrency: usize,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        let mut previous = stats.snapshot();
        loop {
            ticker.tick().await;
            let now = stats.snapshot();
            let d = now.since(&previous);
            previous = now;
            let secs = interval.as_secs_f64();
            let rate = |n: u64| format!("{:.0}", n as f64 / secs);
            let avg_write_ms = if d.write_count == 0 {
                0.0
            } else {
                d.write_micros as f64 / d.write_count as f64 / 1000.0
            };
            let max_write_ms = stats.write_max_micros.swap(0, std::sync::atomic::Ordering::Relaxed) as f64 / 1000.0;
            let position = cursor.position();
            let lag = match (position.highest_finalized, position.watermark) {
                (Some(f), Some(w)) => f.saturating_sub(w),
                _ => 0,
            };
            info!(
                accounts_per_s = %rate(d.accounts_ok),
                txs_per_s = %rate(d.txs_ok),
                skipped = d.accounts_skipped + d.txs_skipped,
                failed = d.accounts_failed + d.txs_failed,
                retries = d.write_retries,
                held = d.writes_held,
                avg_write_ms = %format!("{avg_write_ms:.1}"),
                max_write_ms = %format!("{max_write_ms:.1}"),
                in_flight = concurrency.saturating_sub(writers.available_permits()),
                slots_in_flight = position.slots_in_flight,
                cursor = ?position.watermark,
                finalized = ?position.highest_finalized,
                lag_slots = lag,
                reconnects = d.reconnects,
                gaps = d.gaps,
                meta_ok = d.metadata_ok,
                meta_failed = d.metadata_failed,
                meta_blocked = d.metadata_blocked,
                meta_dropped = d.metadata_dropped,
                "ingest"
            );
            if let Err(e) = cursor.save().await {
                error!(error = %format!("{e:#}"), "failed to persist cursor");
            }
        }
    });
}
