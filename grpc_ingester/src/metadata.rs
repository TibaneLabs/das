//! Off-chain JSON metadata download, replacing nft_ingester's DownloadMetadata task.
//!
//! Metadata URIs are arbitrary on-chain data, so anyone can mint an asset whose URI
//! points at a loopback or private address. On a DAS node that reaches the private
//! validator RPC, the database console, or anything else on the internal network.
//! Upstream's downloader has no guard (`// Need to check for malicious sites ?`).
//! Here every connection - including each redirect hop - may only reach globally
//! routable addresses, and proxies are disabled so the check can't be bypassed.

use {
    crate::{
        retry,
        stats::{inc, Stats},
    },
    anyhow::{ensure, Context},
    digital_asset_types::dao::asset_data,
    program_transformers::{DownloadMetadataInfo, DownloadMetadataNotifier},
    sea_orm::{
        sea_query::Expr, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter,
        SqlxPostgresConnector,
    },
    sqlx::PgPool,
    std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::{atomic::Ordering::Relaxed, Arc},
        time::Duration,
    },
    tokio::sync::{mpsc, Mutex},
    tracing::{debug, warn},
    url::{Host, Url},
};

#[derive(Debug, Clone, clap::Args)]
pub struct MetadataArgs {
    /// Do not download off-chain metadata.
    #[arg(long, env = "INGEST_METADATA_DISABLE")]
    pub metadata_disable: bool,

    /// Concurrent downloads.
    #[arg(long, env = "INGEST_METADATA_CONCURRENCY", default_value_t = 32)]
    pub metadata_concurrency: usize,

    /// Downloads waiting to start. When full, new requests are dropped (and counted)
    /// rather than slowing down indexing.
    #[arg(long, env = "INGEST_METADATA_QUEUE", default_value_t = 100_000)]
    pub metadata_queue: usize,

    #[arg(long, env = "INGEST_METADATA_TIMEOUT_SECS", default_value_t = 5)]
    pub metadata_timeout_secs: u64,

    /// Largest metadata document accepted.
    #[arg(long, env = "INGEST_METADATA_MAX_BYTES", default_value_t = 1_048_576)]
    pub metadata_max_bytes: usize,
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

    let (tx, rx) = mpsc::channel::<DownloadMetadataInfo>(args.metadata_queue.max(1));
    let rx = Arc::new(Mutex::new(rx));
    let client = http_client(&args)?;

    for _ in 0..args.metadata_concurrency.max(1) {
        let rx = Arc::clone(&rx);
        let client = client.clone();
        let db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
        let stats = Arc::clone(&stats);
        let max_bytes = args.metadata_max_bytes;
        tokio::spawn(async move {
            loop {
                let next = rx.lock().await.recv().await;
                let Some(info) = next else { break };
                download(&client, &db, &stats, info, max_bytes, max_write_attempts).await;
            }
        });
    }

    Ok(Box::new(move |info| {
        match tx.try_send(info) {
            Ok(()) => inc(&stats.metadata_queued),
            Err(_) => inc(&stats.metadata_dropped),
        }
        Box::pin(futures::future::ready(Ok(())))
    }))
}

async fn download(
    client: &reqwest::Client,
    db: &DatabaseConnection,
    stats: &Stats,
    info: DownloadMetadataInfo,
    max_bytes: usize,
    max_write_attempts: u32,
) {
    let (asset_data_id, uri) = info.into_inner();
    let uri = uri.trim().replace('\0', "");

    let body = match Url::parse(&uri) {
        // Same marker upstream stores for an unparseable URI.
        Err(_) => serde_json::Value::String("Invalid Uri".to_owned()),
        Ok(url) if !matches!(url.scheme(), "http" | "https") => {
            inc(&stats.metadata_unsupported);
            return;
        }
        Ok(url) if !host_allowed(&url) => {
            inc(&stats.metadata_blocked);
            debug!(%uri, "metadata URI targets a non-global address, not fetching");
            return;
        }
        Ok(url) => match fetch(client, url, max_bytes).await {
            Ok(body) => body,
            Err(e) => {
                if format!("{e:#}").contains(BLOCKED) {
                    inc(&stats.metadata_blocked);
                } else {
                    inc(&stats.metadata_failed);
                }
                debug!(%uri, error = %format!("{e:#}"), "metadata download failed");
                return;
            }
        },
    };

    // Only write if the asset still points at this URI: an update may have replaced
    // it while the download was in progress.
    let mut attempt = 0;
    loop {
        let result = asset_data::Entity::update_many()
            .col_expr(asset_data::Column::Metadata, Expr::value(body.clone()))
            .col_expr(asset_data::Column::Reindex, Expr::value(false))
            .filter(asset_data::Column::Id.eq(asset_data_id.clone()))
            .filter(asset_data::Column::MetadataUrl.eq(uri.clone()))
            .exec(db)
            .await;
        match result {
            Ok(r) if r.rows_affected == 0 => return inc(&stats.metadata_stale),
            Ok(_) => return inc(&stats.metadata_ok),
            Err(e) if retry::is_retryable(&e.to_string()) && attempt + 1 < max_write_attempts => {
                stats.write_retries.fetch_add(1, Relaxed);
                tokio::time::sleep(retry::backoff(attempt)).await;
                attempt += 1;
            }
            Err(e) => {
                inc(&stats.metadata_failed);
                warn!(%uri, error = %e, "failed to store downloaded metadata");
                return;
            }
        }
    }
}

async fn fetch(client: &reqwest::Client, url: Url, max_bytes: usize) -> anyhow::Result<serde_json::Value> {
    let mut response = client.get(url).send().await?;
    ensure!(response.status() == reqwest::StatusCode::OK, "HTTP {}", response.status());
    if let Some(length) = response.content_length() {
        ensure!(length <= max_bytes as u64, "body of {length} bytes exceeds limit");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(body.len() + chunk.len() <= max_bytes, "body exceeds {max_bytes} bytes");
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).context("metadata is not valid JSON")
}

const BLOCKED: &str = "resolves only to non-global addresses";

fn http_client(args: &MetadataArgs) -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .user_agent("das-grpc-ingester")
        .connect_timeout(Duration::from_secs(args.metadata_timeout_secs))
        .timeout(Duration::from_secs(args.metadata_timeout_secs))
        .dns_resolver(Arc::new(GlobalOnlyResolver))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 {
                attempt.error("too many redirects")
            } else if !matches!(attempt.url().scheme(), "http" | "https") || !host_allowed(attempt.url()) {
                attempt.error("redirect to a disallowed address")
            } else {
                attempt.follow()
            }
        }))
        .build()
        .context("building metadata HTTP client")
}

/// Literal IP hosts never reach the DNS resolver, so they are checked here. Domain
/// names are checked by [`GlobalOnlyResolver`] when they are actually resolved.
fn host_allowed(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => is_global(IpAddr::V4(ip)),
        Some(Host::Ipv6(ip)) => is_global(IpAddr::V6(ip)),
        Some(Host::Domain(_)) => true,
        None => false,
    }
}

struct GlobalOnlyResolver;

impl reqwest::dns::Resolve for GlobalOnlyResolver {
    fn resolve(&self, name: hyper::client::connect::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let allowed: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|addr| is_global(addr.ip()))
                .collect();
            if allowed.is_empty() {
                return Err(format!("{host} {BLOCKED}").into());
            }
            let addrs: reqwest::dns::Addrs = Box::new(allowed.into_iter());
            Ok(addrs)
        })
    }
}

const fn is_global(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_global_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4() {
            // IPv4-mapped and IPv4-compatible forms reach the embedded IPv4 address
            Some(v4) => is_global_v4(v4),
            None => is_global_v6(v6),
        },
    }
}

const fn is_global_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || o[0] == 0                                // 0.0.0.0/8
        || (o[0] == 100 && (o[1] & 0xc0) == 64)     // 100.64.0.0/10 carrier-grade NAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)  // 192.0.0.0/24 protocol assignments
        || (o[0] == 198 && (o[1] & 0xfe) == 18)     // 198.18.0.0/15 benchmarking
        || o[0] >= 240) // 240.0.0.0/4 reserved
}

const fn is_global_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (s[0] & 0xfe00) == 0xfc00                // fc00::/7 unique local
        || (s[0] & 0xffc0) == 0xfe80                // fe80::/10 link local
        || (s[0] == 0x2001 && s[1] == 0x0db8)       // 2001:db8::/32 documentation
        || (s[0] == 0x0064 && s[1] == 0xff9b)) // 64:ff9b::/96 NAT64 reaches IPv4 space
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed(url: &str) -> bool {
        host_allowed(&Url::parse(url).unwrap())
    }

    #[test]
    fn blocks_internal_literals() {
        for url in [
            "http://127.0.0.1:8899/",
            "http://localhost.:8899/", // domain: left to the resolver
            "http://10.0.0.1/",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]:26257/",
            "http://[::ffff:127.0.0.1]/",
            "http://[fd00::1]/",
            "http://100.64.1.1/",
            "http://0.0.0.0:8090/",
            "http://2130706433/", // 127.0.0.1 as an integer, normalised by the URL parser
            "http://0x7f.1/",
        ] {
            let expect_allowed = url.starts_with("http://localhost.");
            assert_eq!(allowed(url), expect_allowed, "{url}");
        }
    }

    #[test]
    fn allows_public() {
        assert!(allowed("https://arweave.net/abc"));
        assert!(allowed("https://1.1.1.1/"));
        assert!(allowed("https://[2606:4700:4700::1111]/"));
    }

    #[tokio::test]
    async fn resolver_refuses_loopback_names() {
        use reqwest::dns::Resolve;
        let name: hyper::client::connect::dns::Name = "localhost".parse().unwrap();
        let err = GlobalOnlyResolver.resolve(name).await.err().expect("must refuse");
        assert!(err.to_string().contains(BLOCKED));
    }
}
