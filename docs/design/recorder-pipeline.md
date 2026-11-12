# Recorder pipeline: design (Phase 0, Stage 1)

Status: **designed and agreed** (decisions marked ⚖️ were made 2026-11-10). Last updated 2026-11-11.

## 1. Problem and "done"

We need weeks of aligned snapshots from Deribit, Kalshi, Polymarket and reference price sources. Historical order books can't be bought or backfilled, so **a minute we fail to record is lost forever.** The pipeline must keep recording when any single part fails, and it must never write wrong or duplicated rows that would bias the research.

**Done (Stage 1)** means:
- 48 h of clean, aligned snapshots on disk.
- Kill tests pass with zero lost and zero duplicated rows.
- Recovery time measured and recorded.

## 2. Architecture

```text
             every 60 s, aligned to the minute (snapshot_ts = floor(now, 1 min), UTC)
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ deribit recorder │ │ kalshi recorder  │ │ polymarket rec.  │ │ reference rec.   │   4 OS processes
│ REST: options,   │ │ REST: 4 series   │ │ Gamma discovery  │ │ Binance 1m kline │   (one failing never
│ futures, index   │ │ of markets       │ │ + CLOB /books    │ │ Coinbase ticker  │    stops the others)
└────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
         │ XADD               │                    │                    │
         ▼                    ▼                    ▼                    ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│ Redis (Docker, AOF persistence)   streams: ppm:{venue}   consumer group: writer   │
└───────────────────────────────────────┬───────────────────────────────────────────┘
                                        │ XREADGROUP → process → XACK
                                        ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│ writer: parse → validate (pyarrow schema) → quarantine bad rows                   │
│         → data/staging/{table}/date=D/{HHMM}_{venue}_{key}.parquet (atomic)      │
│ daily:  merge staging → data/{table}/date=D/part.parquet (dedup) → hash manifest  │
└───────────────────────────────────────────────────────────────────────────────────┘
```

### Why each piece exists

| Piece | Failure it handles | Guarantee it gives | What it does *not* guarantee |
|---|---|---|---|
| Separate processes | A crash or hang in one venue's code (bad JSON, API change) | Other venues keep recording | Doesn't help if the whole machine is down (that's Stage 2's VM) |
| Redis Streams | The writer crashing, a full disk, a slow write | Messages wait in Redis until the writer **acknowledges** them (`XACK`) | Redis itself can lose ≤1 s on a hard crash (AOF `everysec`) |
| Consumer group + `XACK` | The writer dying mid-batch | **At-least-once:** an unacknowledged message is redelivered after a restart | It may be delivered **twice**, hence the next row |
| Idempotent writes | Redelivery after a crash | File names come from (tick, venue, key), so the same message → the same file → an overwrite, never a duplicate | — |
| Atomic rename | The writer dying halfway through writing a file | A file on disk is either complete or absent, never half-written | — |
| Schema + quarantine | A venue changing a field's type or meaning | Bad rows never enter `data/`; they're kept for inspection | Doesn't catch values that are wrong but well-typed (that's the data-quality checks in Stage 1.5) |
| Hash chain | Silent edits, deletions or reordering of past files | Verification fails loudly before any analysis runs | Doesn't prevent tampering, only detects it |

**At-least-once + idempotent consumer = effectively-once.** This is the core idea. Getting a message *exactly* once across a crash is very hard. Instead we accept that it may arrive twice and make processing it twice harmless.

## 3. Queue message format

⚖️ **Decision A: what goes on the queue?** → **A1 chosen (raw responses).**

- **A1 (recommended): raw API responses.** The recorder publishes the venue's JSON exactly as received, plus metadata. The writer does all the parsing.
  - ✅ Recorders stay tiny and rarely need changing, so they almost never crash.
  - ✅ The raw data is archived. If a parsing bug is found in week 3, we **replay the raw archive** and regenerate correct tables. That's the plan's "keep raw responses" mitigation.
  - ❌ Messages are bigger (~0.5 MB per Deribit tick). The writer is more complex.
- **A2: normalized rows.** Each recorder parses into the final columns before publishing.
  - ✅ Smaller messages; each venue's parsing lives next to its fetch code.
  - ❌ A parsing bug means those rows are wrong forever (no raw to replay), and parsing errors happen inside the recorder, the part that must never fail.

⚖️ **Decision B: message granularity** → **one message per natural unit of the source.**

- **REST (Stage 1):** one message per API call per tick. For example, all 878 BTC option summaries from one Deribit call make one message.
  - A snapshot is atomic: it lands whole or not at all, so there's never a "half of the chain" minute.
  - Recorders don't need to parse to split rows, which is consistent with A1.
- **Websocket (Stage 2):** one message per pushed update, in the same envelope and through the same writer.
  - The writer will rebuild minute-aligned snapshots from the updates; that gets designed in Stage 2.
- **Rejected:** splitting REST snapshots into per-row messages. It forces parsing in recorders, makes partial snapshots possible after a crash, and adds ~3,000 messages/min with no new information.

Envelope. These are Redis stream fields; all are strings, as Redis requires:

| Field | Example | Meaning |
|---|---|---|
| `v` | `1` | Envelope schema version |
| `venue` | `deribit` | `deribit` · `kalshi` · `polymarket` · `reference` |
| `endpoint` | `book_summary:BTC:option` | Which call produced it; selects the parser |
| `snapshot_ts` | `1794330000000` | Scheduled tick, ms since epoch UTC; **the alignment key across venues** |
| `request_ts` | `1794330000012` | Local time the request was sent |
| `recv_ts` | `1794330000233` | Local time the response arrived (latency = recv − request) |
| `status` | `200` | HTTP status code, or `error` for network failures |
| `params` | `{"currency":"BTC","kind":"option"}` | Request parameters; with `endpoint` they form the envelope **key** (`book_summary-BTC-option`) |
| `enc` | `gzip` | `gzip` for every non-empty body (since 2026-11-11; before that only bodies over 64 KB), `identity` for empty bodies and older files |
| `body` | bytes | Raw response body, stored as binary (Redis values are binary-safe, so no base64), or the error text when `status` is not 200 |

## 4. Parquet tables (the data contract)

These columns appear on every table:

| Column | Type | Meaning |
|---|---|---|
| `snapshot_ts` | timestamp[ms, UTC] | Scheduled tick (join key across venues) |
| `recv_ts` | timestamp[ms, UTC] | Local receive time |
| `exchange_ts` | timestamp[ms, UTC], nullable | The venue's own timestamp for the data |
| `asset` | string | `BTC` / `ETH` |

The `exchange_ts` source differs by venue: Deribit `creation_timestamp` per row; Kalshi `updated_time`; Polymarket book `timestamp`; Coinbase `time`; Binance kline close time.
Clock skew per venue = `recv_ts − exchange_ts` (an upper bound that includes network latency); it's logged and charted.

| Table | Key columns (dedup key in **bold**) | Notes |
|---|---|---|
| `deribit_options` | **snapshot_ts, instrument**, expiry_ts, strike, option_type, bid, ask, mark, mark_iv, underlying_price, underlying_index, index_price, open_interest, volume, interest_rate | Prices in **coin** (inverse); USD conversion happens in analysis, not here |
| `deribit_futures` | **snapshot_ts, instrument**, expiry_ts (null for perp), bid, ask, mark, last, funding_8h, open_interest, index_price | |
| `kalshi_markets` | **snapshot_ts, ticker**, series, event_ticker, strike_type, floor_strike, cap_strike, settle_ts, yes_bid, yes_ask, yes_bid_size, yes_ask_size, last_price, volume, open_interest, status | `settle_ts` = `close_time` (not `expiration_time`, which is a week later); prices in dollars; `exchange_ts` is null (see §10) |
| `polymarket_contracts` ⚖️ | **yes_token_id**, event_slug, condition_id, asset, contract_type (`above`/`below`/`range`), strike_low, strike_high, settle_ts, fees_enabled, fee fields, first_seen_ts | Dimension table: static per contract. Touch markets ("what price will BTC hit") are excluded: they pay on a touch, not at expiry |
| `polymarket_books` ⚖️ | **snapshot_ts, yes_token_id**, best_bid, best_ask, bid_size, ask_size, bids_top5, asks_top5, last_trade, book_hash | Fact table, one row per contract per minute; join to `polymarket_contracts` on `yes_token_id` |
| `reference_prices` | **snapshot_ts, source, symbol**, price, bid, ask | `binance_vision`: close of the 1m candle that closed just before the tick, so the row at noon ET is Polymarket's settlement candle; `coinbase`: last trade + bid/ask (BRTI proxy) |
| `settlements` | **venue, contract_id**, settle_ts, result, settlement_value, collected_ts | From a daily collector; Kalshi gives the actual BRTI average |

## 5. Backpressure

**Backpressure** is what a system does when data arrives faster than it can be processed: buffer, slow down, or drop. The rule here is **never drop silently.**

- **Redis down (recorder side):** each recorder writes envelopes to a bounded on-disk spool (`spool/{venue}/`, one file per envelope, default cap 2 GiB) and replays it oldest-first before its next successful publish. Drops happen, and are logged as `DATA LOSS`, only when the spool is full.
  - *Changed during implementation (2026-11-10):* the design first said "memory first, then disk". Going straight to disk is simpler and also survives the recorder process restarting while Redis is down.
- **Writer behind (Redis side):** streams are created with a large length cap (`MAXLEN ~` ≈ 3 days of messages). ⚠️ Redis trims the oldest entries **even if unacknowledged**, so the cap is a last-resort safety valve against filling memory. Writer lag (pending entries plus age of the oldest) is a metric with an alert well before the cap.
- **Test:** artificially slow the writer and confirm that lag grows, the alert fires and no data is lost once it catches up.

## 6. Hash chain

*Built 2026-11-11; decisions ⚖️ by the owner.*

**Daily merge** (`ppm merge --loop`, hourly at :07; a day is merged from D+1 02:00 UTC once the writer has nothing pending or unread):
- `staging/{table}/date=D/*.parquet` → `tables/{table}/date=D/part-N.parquet`, deduplicated on the table's key and sorted. Tables keyed by tick are merged one hour at a time (bounded memory). Keep rule: the first received copy, or the latest collection for `settlements` and `polymarket_contracts`.
- ⚖️ `raw/{venue}/date=D/*.env` → `archive/{venue}/date=D/raw-N.tar`: an uncompressed tar of the already-gzipped responses, with fixed metadata so the bytes are reproducible. Raw is hash-chained too.
- ⚖️ Inputs are deleted only after the output is re-read and checked, and its manifest entry is written.
- Crash-safe re-runs: an unregistered output is overwritten; leftover staging re-merges to identical bytes (hash compare) and adds no entry; raw already in an archive isn't archived twice.

**Manifest** `data/manifest.jsonl`, one line per output file:
`{seq, date, kind, name, path, count, bytes, sha256, created_ts, supersedes, reason, prev, chain}` with
`chain = sha256(prev + canonical JSON of every other field)`. The chain covers the metadata too, so forging a file *and* its recorded hash still breaks the chain.

⚖️ **Append-only amendments:** late data for a merged day writes `part-(N+1)` with `supersedes` and a `reason`. Old parts are kept and still verified. Analysis reads the latest part per (table, day).

**`ppm verify`:** recomputes every hash and the chain; also flags files under `tables/` or `archive/` that aren't in the manifest. It prints the head (the last chain value). Truncating the manifest's tail is only detectable against a recorded head (`--expect-head`), so heads should be recorded off-machine (planned: alongside the S3 backup).

Tests: modify one byte, delete, swap, insert an extra file, edit an entry, forge a file plus its hash, reorder, delete a middle entry, truncate the tail (caught only with the recorded head).

## 7. Failure tests (each one gives a measured number for the WorkLog)

Run with `uv run ppm faulttest [--scenario …] [--outage-s 300]` from the repo root. It starts an isolated `ppmtest` Compose stack (Redis, writer, deribit, kalshi, polymarket, reference; settlements is left out so a second copy can't get the live stack rate-limited), injects each fault, tears the stack down and writes `.faulttest/run-<ts>/report.json`. A test-only override (`docker-compose.faulttest.yml`) puts Redis on an internal network and gives recorders a second `egress` network, so the internet can be cut without cutting Redis. The verdict comes from three independent witnesses: the recorders' per-tick log line (`N published, M spooled`), a census of every message on the Redis streams (taken inside the writer container), and the `fetch_log` rows in staging. Staging files are named by (tick, venue, key), so a redelivery overwrites instead of adding a row; the census is what shows whether duplicate deliveries happened and were absorbed. Code: `python/ppm/faulttest/` (`analysis.py` is pure and unit-tested).

| Test | Pass condition | Measured (2026-11-12, laptop, final full run `run-20261112T162258Z`) |
|---|---|---|
| Kill the writer mid-batch: freeze it (SIGSTOP) just after a tick until it holds delivered-but-unacked messages, sample the queue, SIGKILL | ≥ 1 message pending at the kill; 0 lost, 0 duplicated rows | Restarted in **0.2 s**, queue drained **1.1 s** after the kill; **1 redelivered**, 25 queued while down (first attempt, frozen at tick + 0.4 s). A kill at a fixed offset proved unreliable: the writer drains a tick in 0.3–2 s |
| Kill each recorder mid-fetch (tick + 0.2 s), restart | Only the ticks while it was down are missing | Down **0.3–0.4 s** each; gap of **1 tick** (deribit, polymarket, reference); kalshi **0** (it had already published, ~0.3 s after the tick, before the signal landed; same in all three runs). Since 2026-11-12 `ppm monitor` logs each gap live as a `GAP` line (§11), and the harness checks those lines against its own count |
| Stop Redis for 5 min | Recorders spool to disk and replay; 0 lost | **170 envelopes / 1.16 MB** spooled; replayed at the next tick: spool empty **1.0 s** after that tick, queue drained 2.5 s after it (51 s after Redis came back, which was 10 s past a tick) |
| Cut the internet for 5 min | Every tick still leaves rows with a cause; recording resumes on its own | **5 error ticks** per venue with the cause stored, **0 silent gaps**; every venue OK again on the first tick after reconnecting (~50.3 s later, i.e. bounded by the 60 s tick) |
| Whole run | 0 lost, 0 duplicated; census agrees both ways | **759 sent, 759 delivered, 759 stored: 0 lost, 0 duplicated, 0 dropped**, 3 allowed gaps (the recorder kills), 0 unexpected or unlogged. 0 extra stream deliveries (no crash landed mid spool-replay, so that duplicate path is covered by unit tests only) |
| Monitor (`ppm monitor`, added 2026-11-12, run `run-20261112T185848Z`) | GAP lines equal the harness's gaps; every ALERT explained by a fault | **6/6** gap ticks matched (0 missed, 0 invented), gaps logged **60.5–60.9 s** after the missing tick (one tick: the earliest possible); **6 alerts**, all explained, no false `silent` after the Redis outage |
| Stop kalshi cleanly for 200 s (`recorder-stop`) | ALERT silent (not before 2.5 ticks), RECOVERED, GAP | ALERT **151.2 s** after its last tick (141.2 s after the stop); RECOVERED + `GAP ticks=3` **30.4 s** after the restart (its first tick) |
| Monitor during the Redis outage / internet cut | ALERT + RECOVERED of the right kind | Redis `unreachable` ALERT **64.6 s** into the outage (60 s threshold + ≤ 10 s backoff); `failing` ALERT for all 4 venues **231.5 s** after the cut (3 closed error ticks), RECOVERED on the first good tick |
| Slow writer | Lag alert fires; 0 lost after catch-up | Not built yet (backpressure item) |

Recovery after Redis or network outages is bounded by the tick: recorders only publish (and so replay the spool) and fetch on a tick. A 2 GiB spool at the measured ~0.24 MB/min for four venues lasts roughly 6 days of Redis downtime.

The harness's own guards caught two of its bugs during development (a writer kill that often missed the batch, and a census not wired into the verdict). Details are in the project log.

## 8. Code layout (Stage 1)

```text
python/ppm/
  pipeline/
    envelope.py      # Envelope dataclass: encode/decode Redis fields
    bus.py           # RedisBus: publish (with spool fallback), consume, ack
    clock.py         # tick scheduling aligned to the minute
  recorders/
    base.py          # Recorder base class: loop, timeouts, backoff, publish
    deribit.py  kalshi.py  polymarket.py  reference.py
  writer/
    parsers.py       # endpoint → rows (one parser per endpoint, pure functions)
    schemas.py       # pyarrow schemas = the table contract above
    writer.py        # consume → parse → validate → quarantine → atomic write → ack
    merge.py         # daily merge + dedup + manifest
  cli.py             # `ppm record deribit`, `ppm writer`, `ppm merge`, `ppm verify`
docker-compose.yml   # Stage 1: Redis only; Stage 1.5 adds every service
```

Parsers are **pure functions** (JSON in, rows out) tested against saved real responses (`python/ppm/tests/fixtures/`). That makes the riskiest code the easiest to test.

## 9. New dependencies

`httpx` (async HTTP), `pyarrow` (Parquet + schemas), `redis` (official client, asyncio support). Dev: `fakeredis` (in-memory Redis for unit tests, so tests don't need Docker).

## 10. Venue recorders (decided 2026-11-11)

APIs were re-checked live on 2026-11-11 before building.

| Recorder | Calls per tick | Notes |
|---|---|---|
| `kalshi` | 4 × `GET /markets?series_ticker=S&status=open&limit=1000` (KXBTCD, KXBTC, KXETHD, KXETH) | 318–390 markets per series, one page. Prices are dollar strings, sizes `_fp` strings. `KXBTCD` has hourly events plus the 5 pm daily. Top of book only. |
| `reference` | 2 × Binance vision klines (`limit=2`), 2 × Coinbase ticker | The parser keeps only the candle that closed before the tick, never the forming one. |
| `polymarket` | **One** Gamma call `/events?slug=…&slug=…` for `{bitcoin,ethereum}-{above,price}-on-{date}` over the next 7 ET dates, refreshed **every 10 min** and cached ⚖️ (retried every tick until it succeeds); one CLOB `POST /books` per event (ladder) every minute, skipped once the ladder's settle time has passed | ⚠️ CLOB returns bids ascending and asks descending: best price is the **last** element. The parser sorts. |
| `settlements` | Hourly tick ⚖️, plus once at start-up: Kalshi `status=settled` markets closed in the last 48 h (`result` + `expiration_value`, the real BRTI average), paged with `cursor`, one series at a time with 0.25 s between pages; one Gamma call for resolved Polymarket events over the last 3 ET dates; Binance 1m candle closing at noon ET for each of those dates | 48 h refetch kept after re-weighing it against a 6 h window (2026-11-11): simplest, and it survives laptop sleeps without a restart. The `(venue, contract_id)` dedup key makes the overlap harmless. ~46k rows, 53 requests, ~22 s, ~1.6 MB gzipped raw per run. |

⚖️ **Polymarket tables split** into a dimension (`polymarket_contracts`) and a fact table (`polymarket_books`), joined on `yes_token_id` in analysis. Alternatives rejected: a stateful writer remembering discovery (breaks order-independent replay of the raw archive), and recorders merging metadata before publish (breaks decision A1).

**Kalshi `exchange_ts` is null** (changed during implementation): `updated_time` is when the market's metadata last changed (often hours old), not a quote time, so using it would give a meaningless clock-skew figure. Kalshi REST gives no quote timestamp.

**Staleness after sleep:** if the machine sleeps *during* a fetch, the response can arrive long after `snapshot_ts`. `recv_ts` stays truthful, so the Stage 1.5 data-quality check flags rows with `recv_ts − snapshot_ts` above a threshold.

**Polymarket details (built 2026-11-11):**
- Bracket semantics, from the market rules: `above` pays if close > strike_low; `below` if close < strike_high; `range` if strike_low ≤ close < strike_high ("exactly between two brackets → the higher bracket").
- Fees: `feesEnabled` with `feeType: crypto_fees_v2`, `feeSchedule {rate: 0.07, takerOnly: true, rebateRate: 0.2}`. Stored raw as JSON in `polymarket_contracts.fee_schedule` for the backtest.
- `/books` silently omits tokens that have resolved (HTTP 200, fewer rows), so settled contracts cause no errors.
- The recorder reads only token ids and end times from Gamma (to know what to fetch); all row parsing is in the writer. `fetch_specs` now receives `snapshot_ts`, and `FetchSpec` supports POST bodies and repeated query parameters.

**Settlement details (built 2026-11-11):**
- The `settlements` table: COMMON columns + `venue` (kalshi / polymarket / binance), `contract_id` (ticker / yes_token_id / `SYMBOL@date`), `event_id`, `settle_ts`, `result` (yes/no), `settlement_value` (BRTI average for Kalshi, candle close for Binance). `exchange_ts` = when the venue settled.
- Kalshi's `status=settled` filter returns markets whose status is `finalized`. Markets closed but not yet settled are skipped and picked up by a later run.
- Binance returns the next existing candle if the requested minute has none, so the parser checks the candle's open time and quarantines a mismatch.
- The recorder base gained `tick_ms` (hourly here) and `run_on_start`. Settlements also allow a wake-up up to an hour late, since their rows carry their own timestamps and can't be mislabelled.

## 11. Monitoring: the venue monitor (built 2026-11-12)

**Problem.** The failure test showed that a recorder dead for 0.3 s leaves a 1-tick hole that only `ppm faulttest` finds afterwards. Overnight on the laptop a venue could be dead or failing for hours unnoticed. Done = one alert when a venue goes silent or only fails, one when it recovers, and a log line for every missing tick as soon as it can be detected.

```text
recorders --XADD--> Redis ppm:{venue} --XREADGROUP--> writer      (unchanged)
                          \--XREAD (read-only peek)--> ppm monitor
                                   VenueWatch (pure state machine, one per venue)
                                   └─> events --> Notifier: LogNotifier [+ NtfyNotifier]
```

- **Separate process, plain `XREAD`.** A dead process can't report its own death, and inside the writer, writer lag would look like every venue going silent. `XREAD` (unlike `XREADGROUP`) doesn't touch the writer's consumer group: no pending entries, no acks (tested against `XINFO GROUPS`). At start-up it seeds each venue from the newest message (`XREVRANGE … COUNT 1`), so a gap that spans a monitor restart is still found.
- **Conditions** (`python/ppm/monitor/watch.py`, pure and unit-tested):
  - **GAP**: tick T followed by T + k·tick (k > 1) → ticks T+tick … T+(k−1)·tick are missing. Older or repeated ticks never make a gap (several messages per tick, spool replay, settlements re-collecting the current hour on restart).
  - **silent**: no message for 2.5 ticks after the last tick (150 s for minute venues, 2.5 h for settlements). Edge-triggered: one ALERT, then one RECOVERED on the next message.
  - **failing**: 3 ticks in a row whose messages are all errors. A failed fetch still publishes an error envelope, so an internet cut never looks silent; this is what catches it. A tick is judged when a newer tick arrives or one tick length after it started.
  - **redis unreachable**: the monitor can't read for ≥ 60 s → one ALERT, RECOVERED on reconnect. After any Redis failure, silence is not judged for one tick + 30 s, because recorders replay their spool only at their next tick (otherwise every venue would raise a false `silent`).
- **Channels** (`notify.py`): every event is a log line (`GAP` WARNING, `ALERT` ERROR, `RECOVERED` WARNING) in key=value form, e.g. `GAP venue=deribit first=2026-11-12T17:27:00Z last=2026-11-12T17:27:00Z ticks=1 detected_after_s=61.2`. ALERT and RECOVERED also go to an ntfy.sh phone push when `PPM_NTFY_TOPIC` is set (the topic acts as a password, so it is never committed). A failed push is logged, never raised. The `Notifier` interface lets Prometheus Alertmanager replace ntfy later.
- **Gaps are log lines only.** `fetch_log` stays the coverage source of truth; a second table could disagree and would be a data-contract change.
- **Thresholds** come from each recorder's `tick_ms`; overrides `PPM_MONITOR_SILENT_TICKS`, `PPM_MONITOR_FAILING_TICKS`.

**Failure tests** (in `ppm faulttest`, monitor added to the `ppmtest` stack): run-wide, the monitor's GAP lines must equal the harness's gaps exactly and every ALERT must be explained by an injected fault; new `recorder-stop` (stop kalshi 200 s) must give ALERT silent → RECOVERED → GAP; net-cut must give ALERT/RECOVERED `failing` per venue (no `silent`); redis-outage must give ALERT/RECOVERED `unreachable` and no false GAP after the spool replay. Measured numbers: see §7.
