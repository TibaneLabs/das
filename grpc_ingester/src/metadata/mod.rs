//! Off-chain JSON metadata download, replacing nft_ingester's DownloadMetadata task.
//!
//! Every write that makes an asset's metadata stale sets `asset_data.reindex = true`
//! and asks for a download; a stored download sets it back to `false`. So:
//!
//! - failures that may clear up (HTTP 429/5xx, timeouts, IPFS content not found yet) are
//!   retried with backoff, and a host that rate-limits is paused as a whole;
//! - once attempts run out the row keeps `reindex = true`, and the re-drive loop picks
//!   it up again later - e.g. after switching to a better IPFS gateway;
//! - failures that won't clear up (404 from a web server, not JSON, blocked address)
//!   set `reindex = false` so the row stops being re-driven.

mod guard;
mod hosts;
mod ipfs;

use {
    crate::{
        retry,
        stats::{inc, Stats},
    },
    digital_asset_types::dao::asset_data,
    hosts::{Hosts, Slot},
    program_transformers::DownloadMetadataNotifier,
    sea_orm::{
        sea_query::Expr, ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, DbErr,
        EntityTrait, QueryFilter, SqlxPostgresConnector, Statement,
    },
    sqlx::PgPool,
    std::{
        collections::HashSet,
        error::Error as _,
        future::Future,
        sync::{atomic::Ordering::Relaxed, Arc, Mutex},
        time::Duration,
    },
    tokio::sync::mpsc,
    tracing::{debug, info, warn},
    url::Url,
};

#[derive(Debug, Clone, clap::Args)]
pub struct MetadataArgs {
    /// Do not download off-chain metadata.
    #[arg(long, env = "INGEST_METADATA_DISABLE")]
    pub metadata_disable: bool,

    /// Concurrent downloads, across all hosts.
    #[arg(long, env = "INGEST_METADATA_CONCURRENCY", default_value_t = 32)]
    pub metadata_concurrency: usize,

    /// Concurrent downloads from any single host.
    #[arg(long, env = "INGEST_METADATA_PER_HOST_CONCURRENCY", default_value_t = 8)]
    pub metadata_per_host_concurrency: usize,

    /// Downloads waiting to start. When full, new requests are skipped (and counted)
    /// rather than slowing indexing; the re-drive loop catches them up later.
    #[arg(long, env = "INGEST_METADATA_QUEUE", default_value_t = 100_000)]
    pub metadata_queue: usize,

    #[arg(long, env = "INGEST_METADATA_TIMEOUT_SECS", default_value_t = 15)]
    pub metadata_timeout_secs: u64,

    /// Largest metadata document accepted.
    #[arg(long, env = "INGEST_METADATA_MAX_BYTES", default_value_t = 1_048_576)]
    pub metadata_max_bytes: usize,

    /// Attempts per download before leaving it to the re-drive loop.
    #[arg(long, env = "INGEST_METADATA_MAX_ATTEMPTS", default_value_t = 5)]
    pub metadata_max_attempts: u32,

    /// How often to look for downloads still owed (`asset_data.reindex = true`). 0 disables.
    #[arg(long, env = "INGEST_METADATA_REDRIVE_INTERVAL_SECS", default_value_t = 300)]
    pub metadata_redrive_interval_secs: u64,

    #[arg(long, env = "INGEST_METADATA_REDRIVE_BATCH", default_value_t = 2_000)]
    pub metadata_redrive_batch: u64,

    /// Gateway that serves IPFS content (`ipfs://` URIs are always sent here). Trusted:
    /// exempt from the public-address check, so it may be on a private network.
    #[arg(long, env = "INGEST_IPFS_GATEWAY", default_value = "https://ipfs.io")]
    pub ipfs_gateway: String,

    /// Also send content from well-known public gateways (pinata, dweb.link, ...) to
    /// `--ipfs-gateway`. Enable once that gateway is our own.
    #[arg(long, env = "INGEST_IPFS_REWRITE_PUBLIC_GATEWAYS")]
    pub ipfs_rewrite_public_gateways: bool,
}

struct Job {
    asset_data_id: Vec<u8>,
    /// Exactly as stored in `asset_data.metadata_url`, so the write-back matches.
    uri: String,
    attempt: u32,
}

struct Shared {
    args: MetadataArgs,
    stats: Arc<Stats>,
    public: reqwest::Client,
    trusted: reqwest::Client,
    gateway: ipfs::Gateway,
    hosts: Hosts,
    /// asset_data ids queued, in flight, or waiting to retry
    pending: Mutex<HashSet<Vec<u8>>>,
    tx: mpsc::Sender<Job>,
    max_write_attempts: u32,
}

pub fn spawn(
    args: MetadataArgs,
    pool: PgPool,
    stats: Arc<Stats>,
    max_write_attempts: u32,
) -> anyhow::Result<DownloadMetadataNotifier> {
    if args.metadata_disable {
        return Ok(Box::new(|_| Box::pin(futures::future::ready(Ok(())))));
    }

    let timeout = Duration::from_secs(args.metadata_timeout_secs.max(1));
    let gateway = ipfs::Gateway::new(&args.ipfs_gateway, args.ipfs_rewrite_public_gateways)?;
    let (tx, rx) = mpsc::channel::<Job>(args.metadata_queue.max(1));
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let shared = Arc::new(Shared {
        public: guard::public_client(timeout)?,
        trusted: trusted_client(timeout, &args.ipfs_gateway)?,
        gateway,
        hosts: Hosts::new(args.metadata_per_host_concurrency),
        pending: Mutex::new(HashSet::new()),
        tx,
        stats,
        max_write_attempts,
        args,
    });
    info!(
        gateway = %shared.args.ipfs_gateway,
        rewrite_public = shared.args.ipfs_rewrite_public_gateways,
        "metadata downloader started"
    );

    for _ in 0..shared.args.metadata_concurrency.max(1) {
        let shared = Arc::clone(&shared);
        let rx = Arc::clone(&rx);
        let db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
        tokio::spawn(async move {
            loop {
                let next = rx.lock().await.recv().await;
                let Some(job) = next else { break };
                process(&shared, &db, job).await;
            }
        });
    }

    if shared.args.metadata_redrive_interval_secs > 0 {
        let shared = Arc::clone(&shared);
        let db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool);
        tokio::spawn(async move { redrive(&shared, &db).await });
    }

    Ok(Box::new(move |info| {
        let (asset_data_id, uri) = info.into_inner();
        shared.enqueue(asset_data_id, uri);
        Box::pin(futures::future::ready(Ok(())))
    }))
}

impl Shared {
    /// Queue a download unless one is already pending for this asset.
    fn enqueue(&self, asset_data_id: Vec<u8>, uri: String) -> bool {
        if !self.lock_pending().insert(asset_data_id.clone()) {
            return false;
        }
        let job = Job { asset_data_id, uri, attempt: 0 };
        match self.tx.try_send(job) {
            Ok(()) => {
                inc(&self.stats.metadata_queued);
                true
            }
            Err(e) => {
                self.lock_pending().remove(&e.into_inner().asset_data_id);
                inc(&self.stats.metadata_dropped);
                false
            }
        }
    }

    fn finish(&self, asset_data_id: &[u8]) {
        self.lock_pending().remove(asset_data_id);
    }

    fn later(self: &Arc<Self>, job: Job, delay: Duration) {
        let shared = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let id = job.asset_data_id.clone();
            if shared.tx.send(job).await.is_err() {
                shared.finish(&id);
            }
        });
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, HashSet<Vec<u8>>> {
        self.pending.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn subdomain_gateways_share_a_host_key() {
        let key = |u: &str| super::host_key(&url::Url::parse(u).unwrap());
        assert_eq!(key("https://bafyabc.ipfs.w3s.link/0.json"), key("https://bafyxyz.ipfs.w3s.link/1.json"));
        assert_eq!(key("https://bafyabc.ipfs.w3s.link/0.json"), "w3s.link:443");
        assert_eq!(key("https://ipfs.io/ipfs/Qm"), "ipfs.io:443");
        assert_eq!(key("http://10.1.2.3:8080/ipfs/Qm"), "10.1.2.3:8080");
    }
}

enum Failure {
    /// The host asked us to slow down.
    RateLimited(Option<Duration>),
    /// Might work if tried again.
    Transient(String),
    /// Won't work no matter how often it's tried.
    Permanent(String),
}

async fn process(shared: &Arc<Shared>, db: &DatabaseConnection, mut job: Job) {
    let stats = &shared.stats;
    let cleaned = job.uri.trim().replace('\0', "");

    let url = match Url::parse(&cleaned) {
        Ok(url) => url,
        Err(_) => {
            // Same marker upstream stores for an unparseable URI.
            let body = serde_json::Value::String("Invalid Uri".to_owned());
            store(shared, db, &job, Some(body)).await;
            inc(&stats.metadata_failed);
            return shared.finish(&job.asset_data_id);
        }
    };

    let (url, rewritten) = match shared.gateway.rewrite(&url) {
        Some(gateway_url) => (gateway_url, true),
        None => (url, false),
    };
    if rewritten {
        inc(&stats.metadata_rewritten);
    }
    let via_gateway = shared.gateway.is_gateway(&url);

    if !matches!(url.scheme(), "http" | "https") {
        inc(&stats.metadata_unsupported);
        store(shared, db, &job, None).await;
        return shared.finish(&job.asset_data_id);
    }
    if !via_gateway && !guard::host_allowed(&url) {
        inc(&stats.metadata_blocked);
        store(shared, db, &job, None).await;
        return shared.finish(&job.asset_data_id);
    }

    let host = host_key(&url);
    let permit = match shared.hosts.acquire(&host).await {
        Slot::Go(permit) => permit,
        Slot::Wait(delay) => {
            // Not the asset's fault: doesn't count as an attempt.
            inc(&stats.metadata_deferred);
            return shared.later(job, delay);
        }
    };
    let client = if via_gateway { &shared.trusted } else { &shared.public };
    let result = fetch(client, url, shared.args.metadata_max_bytes, via_gateway).await;
    drop(permit);

    let (delay, reason) = match result {
        Ok(body) => {
            shared.hosts.succeeded(&host);
            if store(shared, db, &job, Some(body)).await {
                inc(&stats.metadata_ok);
            }
            return shared.finish(&job.asset_data_id);
        }
        Err(Failure::Permanent(reason)) => {
            inc(&stats.metadata_failed);
            debug!(uri = %job.uri, %reason, "metadata download failed permanently");
            store(shared, db, &job, None).await;
            return shared.finish(&job.asset_data_id);
        }
        Err(Failure::RateLimited(retry_after)) => (shared.hosts.back_off(&host, retry_after), "rate limited".to_owned()),
        Err(Failure::Transient(reason)) => (Duration::from_secs(15 << job.attempt.min(6)), reason),
    };

    job.attempt += 1;
    if job.attempt >= shared.args.metadata_max_attempts {
        // Leave reindex = true; the re-drive loop will try again later.
        inc(&stats.metadata_gave_up);
        debug!(uri = %job.uri, %reason, attempts = job.attempt, "metadata download deferred to re-drive");
        return shared.finish(&job.asset_data_id);
    }
    inc(&stats.metadata_retried);
    debug!(uri = %job.uri, %reason, attempt = job.attempt, retry_in = ?delay, "metadata download will retry");
    shared.later(job, delay);
}

/// With a body: store it and clear `reindex`. Without: only clear `reindex`, so a
/// download that will never work stops being re-driven. Either way only if the asset
/// still points at this URI - an update may have replaced it meanwhile.
async fn store(shared: &Shared, db: &DatabaseConnection, job: &Job, body: Option<serde_json::Value>) -> bool {
    let result = with_db_retry(shared, || {
        let mut update = asset_data::Entity::update_many().col_expr(asset_data::Column::Reindex, Expr::value(false));
        if let Some(body) = body.clone() {
            update = update.col_expr(asset_data::Column::Metadata, Expr::value(body));
        }
        update
            .filter(asset_data::Column::Id.eq(job.asset_data_id.clone()))
            .filter(asset_data::Column::MetadataUrl.eq(job.uri.clone()))
            .exec(db)
    })
    .await;
    match result {
        Ok(r) if r.rows_affected == 0 => {
            inc(&shared.stats.metadata_stale);
            false
        }
        Ok(_) => true,
        Err(e) => {
            warn!(uri = %job.uri, error = %e, "failed to store metadata result");
            false
        }
    }
}

async fn with_db_retry<T, F, Fut>(shared: &Shared, mut op: F) -> Result<T, DbErr>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DbErr>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(e) if retry::is_retryable(&e.to_string()) && attempt + 1 < shared.max_write_attempts => {
                shared.stats.write_retries.fetch_add(1, Relaxed);
                tokio::time::sleep(retry::backoff(attempt)).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

async fn fetch(
    client: &reqwest::Client,
    url: Url,
    max_bytes: usize,
    via_gateway: bool,
) -> Result<serde_json::Value, Failure> {
    let mut response = match client.get(url).send().await {
        Ok(response) => response,
        Err(e) => {
            let chain = error_chain(&e);
            return Err(if chain.contains(guard::BLOCKED) || chain.contains(guard::TOO_MANY_REDIRECTS) {
                Failure::Permanent(chain)
            } else {
                Failure::Transient(chain)
            });
        }
    };

    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    match status {
        200 => {}
        429 => return Err(Failure::RateLimited(retry_after)),
        503 if retry_after.is_some() => return Err(Failure::RateLimited(retry_after)),
        // IPFS content that isn't found may simply not have been provided yet.
        404 | 410 if via_gateway => return Err(Failure::Transient(format!("HTTP {status} from gateway"))),
        // our gateway redirecting off-host (the trusted client doesn't follow that)
        300..=399 if via_gateway => return Err(Failure::Transient(format!("HTTP {status} redirect from gateway"))),
        408 | 425 | 500..=599 => return Err(Failure::Transient(format!("HTTP {status}"))),
        _ => return Err(Failure::Permanent(format!("HTTP {status}"))),
    }

    if response.content_length().is_some_and(|len| len > max_bytes as u64) {
        return Err(Failure::Permanent("body exceeds size limit".to_owned()));
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > max_bytes {
                    return Err(Failure::Permanent("body exceeds size limit".to_owned()));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) => return Err(Failure::Transient(error_chain(&e))),
        }
    }
    serde_json::from_slice(&body).map_err(|e| Failure::Permanent(format!("not JSON: {e}")))
}

/// Rate limits apply per gateway, not per CID: `<cid>.ipfs.w3s.link` counts as `w3s.link`.
fn host_key(url: &Url) -> String {
    let host = url.host_str().unwrap_or("");
    let host = host
        .split_once(".ipfs.")
        .or_else(|| host.split_once(".ipns."))
        .map_or(host, |(_, gateway)| gateway);
    format!("{host}:{}", url.port_or_known_default().unwrap_or(0))
}

fn error_chain(e: &reqwest::Error) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        out.push_str(": ");
        out.push_str(&s.to_string());
        source = s.source();
    }
    out
}

/// Client for our own gateway: no public-address restriction (it may be on a private
/// network), but it may only redirect within itself.
fn trusted_client(timeout: Duration, gateway: &str) -> anyhow::Result<reqwest::Client> {
    let base = Url::parse(gateway)?;
    Ok(reqwest::Client::builder()
        .no_proxy()
        .user_agent("das-grpc-ingester")
        .connect_timeout(timeout)
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            let url = attempt.url();
            let same = url.scheme() == base.scheme()
                && url.host_str() == base.host_str()
                && url.port_or_known_default() == base.port_or_known_default();
            if attempt.previous().len() >= 5 {
                attempt.error(guard::TOO_MANY_REDIRECTS)
            } else if same {
                attempt.follow()
            } else {
                attempt.stop()
            }
        }))
        .build()?)
}

/// Periodically queue downloads still owed. Uses the partial index on
/// `asset_data (id) WHERE reindex = true`, walking it in id order.
async fn redrive(shared: &Arc<Shared>, db: &DatabaseConnection) {
    let interval = Duration::from_secs(shared.args.metadata_redrive_interval_secs);
    let batch = shared.args.metadata_redrive_batch.max(1);
    let mut after: Vec<u8> = Vec::new();
    loop {
        tokio::time::sleep(interval).await;
        let mut queued = 0u64;
        loop {
            // Leave room for downloads requested by live indexing.
            if shared.tx.capacity() < shared.args.metadata_queue / 2 {
                break;
            }
            let rows = match db
                .query_all(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT id, metadata_url FROM asset_data \
                     WHERE reindex = true AND metadata_url <> '' AND id > $1 \
                     ORDER BY id LIMIT $2",
                    [after.clone().into(), (batch as i64).into()],
                ))
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    warn!(error = %e, "metadata re-drive query failed");
                    break;
                }
            };
            let full = rows.len() as u64 == batch;
            for row in rows {
                let (Ok(id), Ok(uri)) = (row.try_get::<Vec<u8>>("", "id"), row.try_get::<String>("", "metadata_url")) else {
                    continue;
                };
                after.clone_from(&id);
                if shared.enqueue(id, uri) {
                    queued += 1;
                }
            }
            if !full {
                after.clear();
                break;
            }
        }
        shared.stats.metadata_redriven.fetch_add(queued, Relaxed);
        if queued > 0 {
            info!(queued, "metadata re-drive queued owed downloads");
        }
    }
}
