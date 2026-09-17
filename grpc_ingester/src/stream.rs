//! Subscribe, dispatch writes, and survive disconnects without losing data silently.

use {
    crate::{
        convert,
        cursor::Cursor,
        retry,
        stats::{inc, Stats},
        Config,
    },
    anyhow::Context,
    futures::StreamExt,
    program_transformers::{error::ProgramTransformerError, ProgramTransformer},
    std::{
        collections::HashMap,
        future::Future,
        sync::{atomic::Ordering::Relaxed, Arc},
        time::{Duration, Instant},
    },
    tokio::sync::Semaphore,
    tracing::{debug, error, info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        prelude::{
            subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
        tonic::Code,
    },
};

/// Identical to upstream's plerkle selectors
/// (solana-test-validator-geyser-config/accountsdb-plugin-config.json), so this sees
/// exactly what nft_ingester would have been sent.
const ACCOUNT_OWNERS: &[&str] = &[
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s", // token metadata
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // token-2022
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // spl token
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", // associated token account
    "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY", // bubblegum
    "CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d", // mpl core
    "1DREGFgysWYxLnRnKQnwrxnJQeSMk2HmGaC6whw2B2p",  // agent registry
    "inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp",  // token inscriptions
];
const TRANSACTION_MENTIONS: &[&str] = &["BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"];

fn request(from_slot: Option<u64>) -> SubscribeRequest {
    let strings = |list: &[&str]| list.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    SubscribeRequest {
        accounts: HashMap::from([(
            "das".to_owned(),
            SubscribeRequestFilterAccounts {
                owner: strings(ACCOUNT_OWNERS),
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([(
            "das".to_owned(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                // failed transactions change no state
                failed: Some(false),
                account_include: strings(TRANSACTION_MENTIONS),
                ..Default::default()
            },
        )]),
        slots: HashMap::from([(
            "das".to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        // Finalized only: at processed/confirmed an update from a slot that is later
        // orphaned would be written and never rolled back - the seq guards don't help,
        // because an orphaned transaction carries a perfectly valid sequence number.
        commitment: Some(CommitmentLevel::Finalized as i32),
        from_slot,
        ..Default::default()
    }
}

pub struct Ingest {
    pub config: Config,
    pub transformer: Arc<ProgramTransformer>,
    pub cursor: Arc<Cursor>,
    pub stats: Arc<Stats>,
    pub writers: Arc<Semaphore>,
}

enum Ended {
    /// The server no longer holds the slot we asked to resume from.
    OutOfRange { requested: u64, available: u64 },
    Error { error: anyhow::Error, received_data: bool },
}

impl Ingest {
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
        tokio::pin!(shutdown);
        let mut backoff = Duration::from_secs(1);
        loop {
            let from_slot = self.cursor.resume_slot();
            let ended = tokio::select! {
                ended = self.subscribe(from_slot) => ended,
                () = &mut shutdown => break,
            };
            match ended {
                Ended::OutOfRange { requested, available } => {
                    inc(&self.stats.gaps);
                    self.stats
                        .gap_slots
                        .fetch_add(available.saturating_sub(requested), Relaxed);
                    self.cursor.record_gap(requested, available).await?;
                    backoff = Duration::from_secs(1);
                }
                Ended::Error { error, received_data } => {
                    inc(&self.stats.reconnects);
                    if received_data {
                        backoff = Duration::from_secs(1);
                    }
                    warn!(error = %format!("{error:#}"), ?from_slot, retry_in = ?backoff, "stream ended");
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = &mut shutdown => break,
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }

        info!("shutting down: waiting for in-flight writes");
        let all = u32::try_from(self.config.concurrency).unwrap_or(u32::MAX);
        if tokio::time::timeout(Duration::from_secs(60), self.writers.acquire_many(all))
            .await
            .is_err()
        {
            warn!("in-flight writes did not finish within 60s; cursor stays behind them");
        }
        self.cursor.save().await.context("saving cursor on shutdown")?;
        info!(position = ?self.cursor.position().watermark, "cursor saved");
        Ok(())
    }

    async fn subscribe(&self, from_slot: Option<u64>) -> Ended {
        let mut received_data = false;
        // Ok(slot): OUT_OF_RANGE, the oldest slot the plugin still holds. Anything else is Err.
        let result: anyhow::Result<u64> = async {
            let mut client = GeyserGrpcClient::build_from_shared(self.config.grpc_endpoint.clone())?
                .x_token(self.config.grpc_x_token.clone())?
                .connect_timeout(Duration::from_secs(10))
                .max_decoding_message_size(64 * 1024 * 1024)
                .connect()
                .await
                .context("connecting")?;
            let (_sink, mut stream) = client
                .subscribe_with_request(Some(request(from_slot)))
                .await
                .context("subscribing")?;
            info!(?from_slot, "subscribed at finalized commitment");

            while let Some(message) = stream.next().await {
                let update = match message {
                    Ok(update) => update,
                    Err(status) if status.code() == Code::OutOfRange => {
                        return parse_last_available(status.message());
                    }
                    Err(status) => return Err(status).context("stream error"),
                };
                received_data = true;
                match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let slot = update.slot;
                        match convert::account(update) {
                            Ok(info) => {
                                let info = Arc::new(info);
                                self.dispatch(slot, Kind::Account, move |t| {
                                    let info = Arc::clone(&info);
                                    async move { t.handle_account_update(&info).await }
                                })
                                .await;
                            }
                            Err(e) => {
                                inc(&self.stats.accounts_failed);
                                warn!(slot, error = %e, "unconvertible account update");
                            }
                        }
                    }
                    Some(UpdateOneof::Transaction(update)) => {
                        let slot = update.slot;
                        match convert::transaction(update) {
                            Ok(info) => {
                                let info = Arc::new(info);
                                self.dispatch(slot, Kind::Transaction, move |t| {
                                    let info = Arc::clone(&info);
                                    async move { t.handle_transaction(&info).await }
                                })
                                .await;
                            }
                            Err(e) => {
                                inc(&self.stats.txs_failed);
                                warn!(slot, error = %e, "unconvertible transaction update");
                            }
                        }
                    }
                    Some(UpdateOneof::Slot(update)) => {
                        if update.status == SlotStatus::SlotFinalized as i32 {
                            self.cursor.finalized(update.slot);
                        }
                    }
                    _ => {}
                }
            }
            anyhow::bail!("server closed the stream")
        }
        .await;

        match result {
            Ok(available) => Ended::OutOfRange {
                requested: from_slot.unwrap_or(available),
                available,
            },
            Err(error) => Ended::Error {
                error,
                received_data,
            },
        }
    }

    /// Write one update with bounded concurrency. Waiting for a permit stops reading
    /// the stream, which is the backpressure: if we fall far enough behind the plugin
    /// disconnects us and we resume from the cursor.
    async fn dispatch<F, Fut>(&self, slot: u64, kind: Kind, write: F)
    where
        F: Fn(Arc<ProgramTransformer>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ProgramTransformerError>> + Send,
    {
        let permit = Arc::clone(&self.writers)
            .acquire_owned()
            .await
            .expect("writer semaphore is never closed");
        self.cursor.begin(slot);
        let transformer = Arc::clone(&self.transformer);
        let cursor = Arc::clone(&self.cursor);
        let stats = Arc::clone(&self.stats);
        let max_attempts = self.config.max_write_attempts;

        tokio::spawn(async move {
            let started = Instant::now();
            let mut attempt = 0;
            let outcome = loop {
                match write(Arc::clone(&transformer)).await {
                    Ok(()) => break Ok(()),
                    Err(ProgramTransformerError::NotImplemented) => break Err(None),
                    Err(e) if retry::is_retryable(&e.to_string()) => {
                        attempt += 1;
                        if attempt >= max_attempts {
                            break Err(Some((e, true)));
                        }
                        inc(&stats.write_retries);
                        tokio::time::sleep(retry::backoff(attempt)).await;
                    }
                    Err(e) => break Err(Some((e, false))),
                }
            };
            stats.record_write(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));

            let (ok, skipped, failed) = match kind {
                Kind::Account => (&stats.accounts_ok, &stats.accounts_skipped, &stats.accounts_failed),
                Kind::Transaction => (&stats.txs_ok, &stats.txs_skipped, &stats.txs_failed),
            };
            match outcome {
                Ok(()) => inc(ok),
                Err(None) => inc(skipped),
                Err(Some((e, true))) => {
                    // The database said "try again" every time. The data is fine, so
                    // moving the cursor past it would lose it for good. Hold the
                    // cursor here: a restart replays from this slot.
                    inc(failed);
                    inc(&stats.writes_held);
                    error!(slot, ?kind, error = %e, attempts = max_attempts, "write kept failing; holding cursor at this slot");
                    drop(permit);
                    return;
                }
                Err(Some((e, false))) => {
                    // Rejected on its content (unparseable account and similar).
                    // Retrying cannot help; same handling as upstream: log and move on.
                    inc(failed);
                    debug!(slot, ?kind, error = %e, "update rejected");
                }
            }
            cursor.end(slot);
            drop(permit);
        });
    }
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Account,
    Transaction,
}

/// "broadcast from 443174956 is not available, last available: 443180000"
fn parse_last_available(message: &str) -> anyhow::Result<u64> {
    message
        .rsplit_once("last available:")
        .and_then(|(_, slot)| slot.trim().parse().ok())
        .with_context(|| format!("unexpected OUT_OF_RANGE message: {message}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_plugin_out_of_range_message() {
        let msg = "broadcast from 443174956 is not available, last available: 443180000";
        assert_eq!(super::parse_last_available(msg).unwrap(), 443_180_000);
        assert!(super::parse_last_available("something else").is_err());
    }
}
