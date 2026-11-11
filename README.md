# implied-odds

Do prediction markets price crypto events the same way the options market does?

A Kalshi or Polymarket contract such as "BTC above $95,000 at 5pm ET" pays $1 or $0, which makes it a
digital option. This project fits an arbitrage-free volatility surface to Deribit BTC options, computes
the options-implied price of each such contract, and compares the two.

**Status:** Phase 0 (data recording) in progress. Built so far: a fault-tolerant recording pipeline (venue recorder processes → Redis Streams → Parquet writer) with Deribit BTC/ETH options as the first venue. Kalshi, Polymarket and the pricing library are not built yet.

## Stack

- C++20 core (`cpp/`) exposed to Python as `ppm_core` via pybind11
- Python research layer (`python/ppm/`): venue recorders, contract matching, analysis
- CMake + GoogleTest + Google Benchmark; scikit-build-core + uv; GitHub Actions CI

## Recording pipeline

```text
recorder (one process per venue) ──► Redis Streams ──► writer ──► Parquet
   raw API responses every 60 s        durable queue      validate, quarantine, atomic + idempotent writes
```

At-least-once delivery plus idempotent writes means a crash of any component loses no rows and creates no duplicates. Design: [`docs/design/recorder-pipeline.md`](docs/design/recorder-pipeline.md).

```bash
docker compose up -d redis
uv run ppm writer            # one terminal
uv run ppm record deribit    # another terminal

scripts/recording.sh start   # or: run everything detached (status / stop)
```

## Build and test

```bash
# C++
cmake --preset dev && cmake --build --preset dev && ctest --preset dev

# Python (builds the C++ extension too)
uv sync && uv run pytest
```

Paper trading only: this project never places real orders.
