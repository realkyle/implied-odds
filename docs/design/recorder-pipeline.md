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
| `enc` | `gzip` | `identity`, or `gzip` when the body is over 64 KB |
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

After the daily merge, `data/manifest.jsonl` gets one line per (date, table):
`{date, table, rows, file_sha256, prev_chain, chain = sha256(file_sha256 + prev_chain)}`.
`ppm verify` recomputes the chain and **fails loudly** on a mismatch. Analysis calls it first.
Test: modify, delete or reorder a file and confirm verification fails.

## 7. Failure tests (each one gives a measured number for the WorkLog)

| Test | Pass condition | Measured |
|---|---|---|
| Kill the writer mid-batch, restart | 0 lost, 0 duplicated rows | Recovery time (s), messages redelivered |
| Kill each recorder, restart | Only the ticks while it was down are missing; logged as a gap | Gap length vs. downtime |
| Stop Redis for 5 min | Recorders spool to disk and replay; 0 lost | Spool size, replay time |
| Cut the network for 5 min | Gaps logged with start, end and cause; recording resumes on its own | Resume latency |
| Slow writer | Lag alert fires; 0 lost after catch-up | Peak lag |

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
| `polymarket` (PR ②) | Gamma `/events?slug=…` for `{bitcoin,ethereum}-{above,price}-on-{date}` over the listed days, refreshed **every 10 min** and cached ⚖️; CLOB `POST /books` every minute | ⚠️ CLOB returns bids ascending and asks descending: best price is the **last** element. The parser sorts. |
| `settlements` (PR ③) | Hourly tick ⚖️: Kalshi `status=finalized` markets (gives the real BRTI `expiration_value`), resolved Polymarket events | Refetches the last 48 h each run; the `(venue, contract_id)` dedup key makes that harmless. |

⚖️ **Polymarket tables split** into a dimension (`polymarket_contracts`) and a fact table (`polymarket_books`), joined on `yes_token_id` in analysis. Alternatives rejected: a stateful writer remembering discovery (breaks order-independent replay of the raw archive), and recorders merging metadata before publish (breaks decision A1).

**Kalshi `exchange_ts` is null** (changed during implementation): `updated_time` is when the market's metadata last changed (often hours old), not a quote time, so using it would give a meaningless clock-skew figure. Kalshi REST gives no quote timestamp.

**Staleness after sleep:** if the machine sleeps *during* a fetch, the response can arrive long after `snapshot_ts`. `recv_ts` stays truthful, so the Stage 1.5 data-quality check flags rows with `recv_ts − snapshot_ts` above a threshold.
