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
    tracing::{debug, info, warn},
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
    /// Bounds how many writes run at once.
    writers: Arc<Semaphore>,
    /// Striped locks: a key always maps to the same lock, so writes to one key are
    /// serialised, while any worker can still pick up any update.
    ///
    /// Per-key *queues* were tried first and were worse: the hottest mints landed on one
    /// queue, it filled, and the read loop blocked on it while the other workers idled -
    /// zero writes for minutes. A lock only delays the contended key.
    locks: Arc<Vec<tokio::sync::Mutex<()>>>,
}

const LOCK_STRIPES: usize = 4096;

impl Ingest {
    pub fn new(
        config: Config,
        transformer: Arc<ProgramTransformer>,
        cursor: Arc<Cursor>,
        stats: Arc<Stats>,
    ) -> Self {
        let writers = Arc::new(Semaphore::new(config.concurrency.max(1)));
        let locks = Arc::new((0..LOCK_STRIPES).map(|_| tokio::sync::Mutex::new(())).collect());
        Self { config, transformer, cursor, stats, writers, locks }
    }
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
                                let key = partition_key(&info);
                                self.dispatch(slot, &key, Update::Account(Arc::new(info))).await;
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
                                // Bubblegum transactions are low volume; the signature
                                // spreads them while keeping one signature on one worker.
                                let key = info.signature.as_ref().to_vec();
                                self.dispatch(slot, &key, Update::Transaction(Arc::new(info))).await;
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

    /// Queue one write. Waiting for a permit is the backpressure: if we fall far enough
    /// behind, the plugin disconnects us and we resume from the cursor.
    async fn dispatch(&self, slot: u64, key: &[u8], update: Update) {
        let permit = Arc::clone(&self.writers)
            .acquire_owned()
            .await
            .expect("writer semaphore is never closed");
        self.cursor.begin(slot);
        let stripe = partition(key, LOCK_STRIPES);
        let (transformer, cursor, stats, locks) = (
            Arc::clone(&self.transformer),
            Arc::clone(&self.cursor),
            Arc::clone(&self.stats),
            Arc::clone(&self.locks),
        );
        let max_attempts = self.config.max_write_attempts;
        tokio::spawn(async move {
            let _ordered = locks[stripe].lock().await;
            write(&transformer, &cursor, &stats, max_attempts, slot, &update).await;
            drop(permit);
        });
    }
}

/// Write one update, retrying as long as the database says the failure is retryable.
async fn write(
    transformer: &ProgramTransformer,
    cursor: &Cursor,
    stats: &Stats,
    max_attempts: u32,
    slot: u64,
    update: &Update,
) {
    let started = Instant::now();
    let kind = match update {
        Update::Account(_) => Kind::Account,
        Update::Transaction(_) => Kind::Transaction,
    };
    let mut attempt = 0;
    let outcome = loop {
        let result = match update {
            Update::Account(info) => transformer.handle_account_update(info).await,
            Update::Transaction(info) => transformer.handle_transaction(info).await,
        };
        match result {
            Ok(()) => break Ok(()),
            Err(ProgramTransformerError::NotImplemented) => break Err(None),
            // Retryable means retry. The data is fine and the database is asking us to try
            // again (SERIALIZABLE aborts, dropped connections). Giving up used to hold the
            // cursor at this slot forever, which made a gap inevitable.
            Err(e) if retry::is_retryable(&e.to_string()) => {
                attempt += 1;
                if attempt == max_attempts {
                    inc(&stats.writes_held);
                    warn!(slot, ?kind, error = %e, attempts = attempt, "write still failing; retrying");
                }
                inc(&stats.write_retries);
                tokio::time::sleep(retry::backoff(attempt)).await;
            }
            Err(e) => break Err(Some(e)),
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
        // Rejected on its content (unparseable account and similar); retrying cannot help.
        Err(Some(e)) => {
            inc(failed);
            debug!(slot, ?kind, error = %e, "update rejected");
        }
    }
    cursor.end(slot);
}

/// What to serialise an account update on: its own address.
///
/// Keying token accounts by their *mint* was tried, to stop token accounts of one mint
/// colliding on that mint's `asset` row. It backfired: the busiest mints then serialised
/// on a single stripe while holding write permits, so the permits filled with waiters for
/// one mint and throughput collapsed. Cross-key conflicts are cheaper to absorb through
/// retries than to prevent by serialising a hot key.
const fn partition_key(info: &program_transformers::AccountInfo) -> [u8; 32] {
    info.pubkey.to_bytes()
}

/// Which worker owns a key. FNV-1a: no dependency, and good enough to spread pubkeys.
fn partition(key: &[u8], workers: usize) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % workers as u64) as usize
}

pub enum Update {
    Account(Arc<program_transformers::AccountInfo>),
    Transaction(Arc<program_transformers::TransactionInfo>),
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
