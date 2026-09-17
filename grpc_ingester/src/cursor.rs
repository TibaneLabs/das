//! Where to resume, and a record of what was permanently missed.
//!
//! The cursor is the newest slot below which every update has been written. Updates
//! are written concurrently, so it trails the stream: it may not pass a slot that
//! still has writes in flight, nor the newest finalized slot (whose updates may
//! still be arriving). Replaying from it after a restart re-applies some updates,
//! which is harmless because every DAS write is guarded by slot/seq.

use {
    anyhow::Context,
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        sync::{Mutex, MutexGuard},
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::io::AsyncWriteExt,
    tracing::{error, info, warn},
};

#[derive(Default)]
struct Inner {
    /// slot -> writes started but not finished
    pending: BTreeMap<u64, u64>,
    highest_finalized: Option<u64>,
    watermark: Option<u64>,
}

impl Inner {
    fn advance(&mut self) {
        let Some(finalized) = self.highest_finalized else {
            return;
        };
        let mut candidate = finalized.saturating_sub(1);
        if let Some((&lowest_pending, _)) = self.pending.first_key_value() {
            candidate = candidate.min(lowest_pending.saturating_sub(1));
        }
        if self.watermark.is_none_or(|w| candidate > w) {
            self.watermark = Some(candidate);
        }
    }
}

pub struct Position {
    pub watermark: Option<u64>,
    pub highest_finalized: Option<u64>,
    pub slots_in_flight: usize,
}

pub struct Cursor {
    inner: Mutex<Inner>,
    cursor_path: PathBuf,
    gaps_path: PathBuf,
}

impl Cursor {
    pub async fn load(state_dir: &Path, start_slot: Option<u64>) -> anyhow::Result<Self> {
        let cursor_path = state_dir.join("cursor");
        let gaps_path = state_dir.join("gaps.jsonl");
        let saved = match tokio::fs::read_to_string(&cursor_path).await {
            Ok(text) => Some(text.trim().parse::<u64>().with_context(|| {
                format!("cursor file {} is corrupt", cursor_path.display())
            })?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e).context("reading cursor file"),
        };
        let watermark = match (start_slot, saved) {
            (Some(slot), _) => {
                info!(slot, "starting from --start-slot, ignoring any saved cursor");
                Some(slot.saturating_sub(1))
            }
            (None, Some(slot)) => {
                info!(slot, "resuming after saved cursor");
                Some(slot)
            }
            (None, None) => {
                warn!("no saved cursor: starting at the live tip, nothing earlier will be indexed");
                None
            }
        };
        Ok(Self {
            inner: Mutex::new(Inner {
                watermark,
                ..Default::default()
            }),
            cursor_path,
            gaps_path,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Value for `SubscribeRequest.from_slot`.
    pub fn resume_slot(&self) -> Option<u64> {
        self.lock().watermark.map(|w| w + 1)
    }

    pub fn begin(&self, slot: u64) {
        *self.lock().pending.entry(slot).or_default() += 1;
    }

    pub fn end(&self, slot: u64) {
        let mut inner = self.lock();
        let finished = match inner.pending.get_mut(&slot) {
            Some(count) => {
                *count -= 1;
                *count == 0
            }
            None => false,
        };
        if finished {
            inner.pending.remove(&slot);
        }
        inner.advance();
    }

    pub fn finalized(&self, slot: u64) {
        let mut inner = self.lock();
        inner.highest_finalized = Some(inner.highest_finalized.map_or(slot, |h| h.max(slot)));
        inner.advance();
    }

    pub fn position(&self) -> Position {
        let inner = self.lock();
        Position {
            watermark: inner.watermark,
            highest_finalized: inner.highest_finalized,
            slots_in_flight: inner.pending.len(),
        }
    }

    /// Record that `[from, to_exclusive)` will never reach this node's index through the
    /// stream, and move the cursor past it so ingestion can continue.
    pub async fn record_gap(&self, from: u64, to_exclusive: u64) -> anyhow::Result<()> {
        error!(
            from,
            to_exclusive,
            slots = to_exclusive.saturating_sub(from),
            "GAP: these slots are missing from this node's index and need repair"
        );
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let line = format!(
            "{{\"detected_at\":{now},\"from\":{from},\"to_exclusive\":{to_exclusive},\"reason\":\"out_of_range\"}}\n"
        );
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.gaps_path)
            .await
            .context("opening gap log")?;
        file.write_all(line.as_bytes()).await?;
        file.sync_all().await?;

        let mut inner = self.lock();
        let past_gap = to_exclusive.saturating_sub(1);
        if inner.watermark.is_none_or(|w| past_gap > w) {
            inner.watermark = Some(past_gap);
        }
        Ok(())
    }

    /// Persist atomically: write, fsync, rename.
    pub async fn save(&self) -> anyhow::Result<()> {
        let Some(watermark) = self.lock().watermark else {
            return Ok(());
        };
        let tmp = self.cursor_path.with_extension("tmp");
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(format!("{watermark}\n").as_bytes()).await?;
        file.sync_all().await?;
        tokio::fs::rename(&tmp, &self.cursor_path).await?;
        Ok(())
    }
}
