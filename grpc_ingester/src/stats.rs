//! Counters for the periodic throughput report.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default)]
        pub struct Stats {
            $(pub $name: AtomicU64,)*
            /// Slowest single write since the last report, in microseconds.
            pub write_max_micros: AtomicU64,
        }

        #[derive(Clone, Default)]
        pub struct Snapshot {
            $(pub $name: u64,)*
        }

        impl Stats {
            pub fn snapshot(&self) -> Snapshot {
                Snapshot { $($name: self.$name.load(Relaxed),)* }
            }
        }

        impl Snapshot {
            pub const fn since(&self, earlier: &Self) -> Self {
                Self { $($name: self.$name.saturating_sub(earlier.$name),)* }
            }
        }
    };
}

counters!(
    accounts_ok,
    accounts_skipped,
    accounts_failed,
    txs_ok,
    txs_skipped,
    txs_failed,
    write_retries,
    writes_held,
    write_micros,
    write_count,
    reconnects,
    gaps,
    gap_slots,
    metadata_queued,
    metadata_dropped,
    metadata_ok,
    metadata_stale,
    metadata_failed,
    metadata_blocked,
    metadata_unsupported,
);

pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Relaxed);
}

impl Stats {
    pub fn record_write(&self, micros: u64) {
        self.write_micros.fetch_add(micros, Relaxed);
        self.write_count.fetch_add(1, Relaxed);
        self.write_max_micros.fetch_max(micros, Relaxed);
    }
}
