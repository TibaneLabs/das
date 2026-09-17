# das-grpc-ingester

Indexes a local [Yellowstone gRPC](https://github.com/rpcpool/yellowstone-grpc) feed
straight into the DAS database through `program_transformers`. It replaces the
upstream pipeline of plerkle plugin -> Redis streams -> `nft_ingester`.

```
agave-validator + yellowstone-grpc-geyser ──gRPC (loopback)──> das-grpc-ingester ──SQL──> CockroachDB/Postgres <── das_api
```

## Why no Redis

Upstream uses Redis consumer groups to spread one feed across a pool of ingesters.
We run one ingester per validator, next to it, so there is nothing to spread: the
firehose stays on loopback and only the resulting writes leave the machine. Several
nodes can write the same database - every DAS write is a slot/seq-guarded upsert, so
duplicates are no-ops.

## Behaviour

- **Selectors** are identical to upstream's plerkle config: accounts owned by token
  metadata, SPL token, token-2022, ATA, bubblegum, MPL core, agent registry and token
  inscriptions; non-vote, non-failed transactions mentioning bubblegum.
- **Finalized commitment only.** At processed/confirmed, state from a slot that later
  gets orphaned would be written and never rolled back; the seq guards can't catch it.
- **Resume.** `cursor` in the state directory is the newest slot below which everything
  has been written. On restart the ingester subscribes with `from_slot = cursor + 1` and
  the plugin replays from its in-memory buffer (`replay_stored_slots`).
- **Gaps.** If the plugin no longer holds that slot it answers `OUT_OF_RANGE` with the
  oldest slot it has. The ingester appends the missed range to `gaps.jsonl`, logs an
  `ERROR`, and continues from there. Those slots need repair from another source.
- **Retries.** CockroachDB is SERIALIZABLE, so contending upserts fail with SQLSTATE
  40001 and must be retried; so are dropped connections. If a write keeps failing for
  such a reason, the cursor is *held* at that slot rather than moving past data that was
  never stored. Updates rejected on their content (e.g. an unparseable account) are
  logged and skipped, as upstream does.
- **Backpressure.** At most `INGEST_CONCURRENCY` writes run at once; beyond that the
  ingester stops reading. If it falls far enough behind, the plugin disconnects it and it
  resumes from the cursor.
- **Off-chain metadata** is downloaded by a bounded worker pool, at most
  `INGEST_METADATA_PER_HOST_CONCURRENCY` requests per host (subdomain gateways such as
  `<cid>.ipfs.w3s.link` count as one host).
  - HTTP 429 (or 503 with `Retry-After`) pauses the whole host, honouring `Retry-After`,
    otherwise doubling from 5 s to 10 min. Jobs waiting on a paused host don't lose attempts.
  - Other transient failures (5xx, timeouts, IPFS content not found yet) retry with
    backoff up to `INGEST_METADATA_MAX_ATTEMPTS`.
  - Permanent failures (404 from a web server, not JSON, too large) clear
    `asset_data.reindex` so they stop being retried.
  - `reindex = true` means "download owed". Every `INGEST_METADATA_REDRIVE_INTERVAL_SECS`
    the ingester walks a partial index over those rows and queues what isn't already
    pending - so downloads that ran out of attempts are picked up again later, e.g. after
    switching gateways.
  - **IPFS:** `ipfs://` URIs always go to `INGEST_IPFS_GATEWAY` (default `https://ipfs.io`).
    With `INGEST_IPFS_REWRITE_PUBLIC_GATEWAYS=true`, content addressed through well-known
    public gateways (pinata, dweb.link, w3s.link, ...) goes there too - turn this on once
    the gateway is your own. The stored URI is never changed. Public gateways rate-limit
    hard: ipfs.io answers a mainnet firehose with 429 and `Retry-After: 600`.
  - Metadata URIs are attacker-controlled on-chain data, so outside the configured
    gateway, connections (including every redirect hop) may only reach globally routable
    addresses and proxies are ignored. Without that, anyone could mint an asset pointing
    at `http://127.0.0.1:8899` and make the node fetch from its private RPC. The
    configured gateway is trusted and may be on a private network.

## Running

```sh
cargo build --release -p grpc_ingester -p das_api
install -m 755 target/release/das-grpc-ingester target/release/das_api /usr/local/bin/
install -d /etc/das
install -m 640 grpc_ingester/deploy/grpc-ingester.env.example /etc/das/grpc-ingester.env
install -m 640 grpc_ingester/deploy/api.env.example /etc/das/api.env
install -m 644 grpc_ingester/deploy/*.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now das-grpc-ingester das-api
```

Every 10 seconds it logs one `ingest` line: accounts/s, txs/s, skipped, failed, retries,
held writes, average and maximum write time, in-flight writes, cursor, finalized slot,
lag, reconnects, gaps and metadata counters.
