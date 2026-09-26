//! The owner's SOL balance, for Helius's `showNativeBalance` extension.
//!
//! Not part of the DAS spec, but clients written against Helius send it, and rejecting the
//! parameter fails the whole call. Filled from this node's own RPC, so it costs one cheap
//! `getBalance` (measured at ~20ms locally) and only when the caller asks for it.

use {
    digital_asset_types::rpc::response::NativeBalance,
    std::time::Duration,
    tracing::{debug, warn},
};

pub struct NativeBalanceFetcher {
    client: reqwest::Client,
    rpc_url: String,
}

impl NativeBalanceFetcher {
    pub fn new(rpc_url: String, timeout: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .connect_timeout(timeout)
                .build()?,
            rpc_url,
        })
    }

    /// `None` when the balance cannot be fetched: the asset list is still worth returning,
    /// so a balance lookup never fails the request.
    pub async fn lamports(&self, owner: &str) -> Option<NativeBalance> {
        let body = self
            .client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": "native-balance",
                "method": "getBalance", "params": [owner],
            }))
            .send()
            .await;
        let value: serde_json::Value = match body {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "getBalance returned an unreadable body");
                    return None;
                }
            },
            Err(e) => {
                warn!(error = %e.without_url(), "getBalance request failed");
                return None;
            }
        };
        let lamports = value.get("result")?.get("value")?.as_u64()?;
        debug!(owner, lamports, "native balance");
        Some(NativeBalance {
            lamports,
            // We run no price feed; Helius fills these from theirs.
            price_per_sol: None,
            total_price: None,
        })
    }
}
