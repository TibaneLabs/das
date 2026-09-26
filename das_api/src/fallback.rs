//! Serve assets this node hasn't indexed from an upstream DAS provider, and keep what
//! comes back.
//!
//! Ingestion started mid-chain and has gaps, so plenty of assets are simply absent
//! locally. Rather than answering "not found", ask a provider that does have them, cache
//! the answer in `das_fallback_cache`, and return it. The local index stays authoritative:
//! once real data arrives for an asset, the local answer is used and the cache is ignored.
//!
//! The upstream URL carries an API key, so it is never logged - only method names,
//! statuses and durations.

use {
    sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, Value},
    serde::de::DeserializeOwned,
    std::time::{Duration, Instant},
    tracing::{debug, warn},
};

pub struct Fallback {
    client: reqwest::Client,
    /// Contains the API key. Never log this.
    url: String,
    ttl: Duration,
}

impl Fallback {
    pub fn new(url: String, timeout: Duration, ttl: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .connect_timeout(timeout)
                .user_agent("tibane-das")
                .build()?,
            url,
            ttl,
        })
    }

    /// Cached upstream answer, or `None` if the cache is empty/stale.
    async fn cached<T: DeserializeOwned>(
        &self,
        db: &DatabaseConnection,
        method: &str,
        key: &str,
    ) -> Option<T> {
        let row = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT response FROM das_fallback_cache \
                 WHERE method = $1 AND cache_key = $2 \
                   AND fetched_at > now() - ($3::INT8 * INTERVAL '1 second')",
                [
                    method.into(),
                    key.into(),
                    Value::BigInt(Some(self.ttl.as_secs() as i64)),
                ],
            ))
            .await
            .ok()??;
        let json: serde_json::Value = row.try_get("", "response").ok()?;
        serde_json::from_value(json).ok()
    }

    async fn store(&self, db: &DatabaseConnection, method: &str, key: &str, value: &serde_json::Value) {
        let result = db
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPSERT INTO das_fallback_cache (method, cache_key, response, fetched_at) \
                 VALUES ($1, $2, $3, now())",
                [method.into(), key.into(), value.clone().into()],
            ))
            .await;
        if let Err(e) = result {
            warn!(method, error = %e, "could not cache the upstream DAS answer");
        }
    }

    /// Ask upstream for `method`/`params`, using and filling the cache. `Ok(None)` means
    /// upstream has nothing either.
    pub async fn get<T: DeserializeOwned>(
        &self,
        db: &DatabaseConnection,
        method: &str,
        cache_key: &str,
        params: serde_json::Value,
    ) -> Option<T> {
        if let Some(hit) = self.cached::<T>(db, method, cache_key).await {
            debug!(method, "served from the fallback cache");
            return Some(hit);
        }

        let started = Instant::now();
        let response = self
            .client
            .post(&self.url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": "das-fallback", "method": method, "params": params,
            }))
            .send()
            .await;

        // Deliberately not logging the URL: it carries the API key.
        let body: serde_json::Value = match response {
            Ok(r) if r.status().is_success() => match r.json().await {
                Ok(body) => body,
                Err(e) => {
                    warn!(method, error = %e, "upstream DAS returned an unreadable body");
                    return None;
                }
            },
            Ok(r) => {
                warn!(method, status = %r.status(), "upstream DAS rejected the request");
                return None;
            }
            Err(e) => {
                warn!(method, error = %e.without_url(), "upstream DAS request failed");
                return None;
            }
        };

        if let Some(error) = body.get("error") {
            debug!(method, %error, "upstream DAS has no answer either");
            return None;
        }
        let result = body.get("result")?;
        if result.is_null() {
            return None;
        }
        debug!(method, ms = started.elapsed().as_millis(), "fetched from upstream DAS");
        self.store(db, method, cache_key, result).await;
        serde_json::from_value(result.clone()).ok()
    }
}
