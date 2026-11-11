# Pricing Prediction Markets with the Options Market — Project Plan

Nov 10, 2026 · @kale

## Overview

**Core question:** do prediction markets price crypto events the same way the options market does? If not, where do they disagree, why, and is the gap tradeable after fees and hedging?

A Kalshi or Polymarket contract such as "BTC above $95,000 at 5pm ET" pays $1 or $0. That is a cash-or-nothing digital option. Deribit's BTC options market implies its own price for the same digital through its volatility surface. This project builds the machinery to compute that price precisely and compares it against the prediction markets.

**What it demonstrates:**

- **QD (quant developer):** a C++20 pricing library with Python bindings, an implied-vol solver, an arbitrage-free surface fitter, tests and benchmarks; optionally a live low-latency engine.
- **QT (quant trader):** probability, risk-neutral vs. real-world measures, calibration, expected value after costs, hedging, and Kelly sizing.
- **Uniqueness:** the dataset is self-recorded and the question is rarely studied, so the findings are your own.

**Ground rule:** paper trading only. The research does not require placing real trades.

## Data sources and settlement

All three venues have free public data. The settlement rules differ, and handling those differences is part of the project, not a nuisance.

| Venue | What to pull | Settles on | Expiry timing | Docs |
| --- | --- | --- | --- | --- |
| Deribit | Full BTC/ETH options chain (bid, ask, mark, IV, underlying), futures and perpetual | Deribit index (30-min TWAP before expiry) | Fixed 08:00 UTC expiries: daily, weekly, monthly, quarterly | [API docs](https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency) |
| Kalshi | BTC/ETH "above/below" (series KXBTCD) and "range" (KXBTC) ladders: order books and trades | Simple average of the CF Benchmarks index (BRTI) over the 60 seconds before the named time | Hourly, daily, 15-minute and others, ET times | [Crypto market rules](https://help.kalshi.com/markets/popular-markets/crypto-markets) |
| Polymarket | "Bitcoin above \_\_\_ on \[date\]?" strike ladders: CLOB prices and books | Binance BTC/USDT 1-minute candle close at the named ET time | Daily and intraday, ET times | Market rules on each event page |

**Mismatches to model:**

1. **Time:** Deribit expiries never match the prediction-market settlement time, so you must interpolate the surface in time. This is why the surface must be free of calendar arbitrage.
2. **Index:** CF BRTI vs. Binance BTC/USDT vs. Deribit index. Measure the basis between them historically; USDT vs. USD adds a small extra basis.
3. **Averaging:** Kalshi's 60-second average slightly lowers effective volatility near expiry. It's negligible for daily contracts but matters for hourly and 15-minute ones.
4. **Quoting:** Deribit options are inverse (priced in BTC). Convert to USD with the underlying price before fitting.

**First action:** start a recorder on day 1 that snapshots all three venues together (for example every 60 seconds) into Parquet. Historical order books are not freely available, so your dataset only exists if you record it. Check what history each API offers (for example candlesticks or price history) as a backfill.

Open question: confirm the exact Deribit settlement method and current Kalshi fee formula from the official docs before the backtest.

## Core math

The fair price of a digital comes from the slope of call prices across strikes, so the surface's shape (not just one IV) sets the answer.

**1. Digital from call prices.** Under the risk-neutral measure Q, with discount factor D:

```latex
D \cdot Q(S_T > K) = -\frac{\partial C(K)}{\partial K}
```

**2. With a volatility smile.** Write C(K) = C\_BS(K, σ(K)) and apply the chain rule. The second term is the skew correction; ignoring it is a classic mistake:

```latex
Q(S_T > K) = N(d_2) - \frac{1}{D}\,\mathcal{V}_{BS}(K)\,\frac{\partial \sigma}{\partial K}
```

**3. Range contracts and the density.** A range contract \[K1, K2) equals Q(S > K1) − Q(S > K2). In the limit, Breeden–Litzenberger gives the full risk-neutral density:

```latex
q(K) = \frac{1}{D}\,\frac{\partial^2 C}{\partial K^2}
```

**4. SVI per expiry.** In log-moneyness k = ln(K/F), total implied variance w = σ²T is fitted as:

```latex
w(k) = a + b\left(\rho\,(k - m) + \sqrt{(k - m)^2 + \sigma^2}\right)
```

**5. SSVI across expiries.** Parameterize w(k, θ\_t) with ATM total variance θ\_t, so one smooth surface covers all expiries and can be evaluated at any settlement time.

**6. No-arbitrage checks.**

- **Butterfly (across strikes):** the implied density must be non-negative, i.e. Gatheral's g(k) ≥ 0.
- **Calendar (across time):** total variance must not decrease with maturity at fixed k.

**7. Risk-neutral vs. real-world.** Options give Q-probabilities, which embed a risk premium (fat tails, variance risk premium). Real outcomes happen under P. Some disagreement between venues is expected premium, not mispricing. Separating the two is the most "quant" part of the write-up.

## System architecture

A C++20 core does the numerical work and is exposed to Python through pybind11. Python handles data collection, research and plots.

&#91;embedded content: system architecture · data flow from venues to research\]

The matcher asks the C++ surface for σ(K, T) at each contract's exact settlement time; everything downstream reads the matched table.

**C++ core library (`ppm_core`):**

1. **Black–76 pricer** for calls, puts and digitals, with analytic Greeks.
2. **Implied-vol solver:** Newton with a bracketed bisection fallback first; Jäckel's "Let's Be Rational" as a stretch.
3. **SVI/SSVI fitter:** Levenberg–Marquardt (hand-written or Ceres), with parameter constraints and a weighted loss (by vega or bid-ask spread).
4. **Arbitrage checker:** butterfly g(k) ≥ 0 on a dense grid, calendar monotonicity of total variance.
5. **Surface object:** evaluate σ(K, T) and ∂σ/∂K at any strike and time, including the exact settlement time of a prediction contract.
6. **Digital and range pricer** using the skew-corrected formula, plus density extraction.

**Python research layer (`ppm`):**

1. **Recorders** for Deribit, Kalshi and Polymarket, writing timestamped Parquet snapshots.
2. **Contract matcher:** maps each prediction contract to (strike, settlement time, index) and the Deribit expiries around it.
3. **Analysis notebooks:** divergence, calibration, backtest.

**Tooling:** CMake (with presets), GoogleTest, Google Benchmark, pybind11 via scikit-build-core, `uv` or `pip` for Python, GitHub Actions running C++ and Python tests, clang-format and clang-tidy.

**Repo layout:**

```text
ppm/
├── CLAUDE.md                 # project context for Claude Code
├── CMakeLists.txt
├── pyproject.toml            # scikit-build-core builds the pybind11 module
├── cpp/
│   ├── include/ppm/          # black76.hpp, iv_solver.hpp, svi.hpp, ssvi.hpp, surface.hpp, digital.hpp, arb_check.hpp
│   ├── src/
│   ├── bindings/             # pybind11 module
│   ├── tests/                # GoogleTest
│   └── bench/                # Google Benchmark
├── python/ppm/
│   ├── recorders/            # deribit.py, kalshi.py, polymarket.py
│   ├── match.py              # contract ↔ surface mapping
│   ├── analysis/             # divergence.py, calibration.py, backtest.py
│   └── tests/
├── data/                     # Parquet snapshots (git-ignored)
├── notebooks/
└── docs/                     # write-up, figures
```

## Research questions and methodology

Three questions, each with a measurable output. A clear negative result is a valid finding.

**Q1. Divergence: where do the venues disagree?**

- Metric: gap = prediction-market mid − options-implied digital, per contract and snapshot.
- Slice by moneyness (ATM vs. tails), time to settlement, venue, liquidity (spread, depth), and around scheduled news (CPI, FOMC).
- Hypotheses to test: favorite–longshot bias in tails; gaps widen near expiry or during fast moves; Polymarket and Kalshi disagree with each other as well as with options.

**Q2. Calibration: who is closer to reality?**

- For settled contracts, compare prediction-market price, options-implied Q-probability and the outcome.
- Brier score and log loss per source; reliability diagrams (predicted probability bins vs. realized frequency).
- Expect options to over-price tails relative to outcomes (risk premium). Quantify by how much.

**Q3. Tradeability: does any gap survive costs?**

- Costs: bid-ask spreads, Kalshi trading fees, Polymarket fees if any, capital lockup until settlement.
- **Hedged trade:** buy the cheap digital on the prediction market and sell a tight Deribit call spread (K − Δ, K + Δ) that replicates the digital. Track P&L and residual risk from index basis, settlement-time gap and averaging.
- **Unhedged trade:** bet the gap directly, sized by fractional Kelly using the options-implied (or calibrated) probability.
- Report: number of trades, hit rate, mean edge per contract after costs, P&L distribution, max drawdown, and sensitivity to fee and slippage assumptions.

**Kelly sizing for a binary contract** bought at price c with estimated probability p:

```latex
f^* = \frac{p - c}{1 - c}
```

Use a fraction (¼ to ½ Kelly) and explain why: p is estimated with error, and full Kelly is very sensitive to that error.

**Pitfalls to guard against:** look-ahead bias (only use data available at the snapshot time), stale quotes on illiquid strikes, timestamp alignment across venues, and survivorship in which contracts you analyse.

## Milestones

About 10 weeks part-time. Phase 0 starts immediately because the dataset grows while everything else is built. Each phase ends with a done-check.

### Phase 0 — Recorder (week 1)

- [x] Repo skeleton, `pyproject.toml`, linting, CI stub
- [x] Deribit recorder: full BTC + ETH option chains + index + futures/perpetual, every 60 s
- [x] Kalshi recorder: open KXBTCD / KXBTC / KXETHD / KXETH markets, top-of-book and last trade (depth needs the authenticated websocket)
- [x] Polymarket recorder: BTC/ETH "above" and "price on" ladders, CLOB top of book and depth (top 5 levels)
- [x] Reference prices: Binance BTC/USDT 1m candle (Polymarket's settlement source), Coinbase BTC-USD (BRTI proxy)
- [x] Settlement collector: Kalshi results and settlement values, Polymarket outcomes, Binance close at each settle time
- [ ] Common schema, one Parquet file per venue per day, UTC timestamps
- [ ] Runs unattended (cron, systemd or a small cloud VM) with logging and restart

Done when: 48 hours of clean, aligned snapshots on disk.

**Systems work (agreed 2026-11-10; design in `docs/design/recorder-pipeline.md`).** Each item is proven by a failure test and one measured number in the WorkLog.

*Stage 1: pipeline (REST polling)*
- [x] One process per venue, so one venue failing never stops the others
- [x] Recorders publish to Redis Streams; a separate writer consumes into Parquet
- [ ] Idempotent writes: a replayed message never creates a duplicate row
- [ ] Exchange timestamp and local receive timestamp on every row; per-venue clock skew and latency logged
- [x] Schema validation on write; bad rows go to a quarantine file
- [ ] Backpressure: bounded buffers when Redis or the writer falls behind, a writer-lag alert, and a slow-writer test
- [ ] Daily hash chain: manifest of SHA-256(file + previous hash), verified before analysis; tampering test
- [ ] Failure test: kill each recorder and the writer mid-run and cut the network; zero lost or duplicated rows; recovery time recorded

*Stage 1.5: operations*
- [x] Dockerfile + docker-compose for every service (one image, one service per process), restart policies; writer SIGKILL test: back in 0.9 s, 0 lost / 0 duplicated
- [ ] Health checks per service + monitoring containers (with Prometheus)
- [ ] Prometheus metrics (messages/s, last-message age, gaps, writer lag, reconnects) + Grafana dashboard
- [ ] Alert when a venue has been silent longer than a threshold
- [ ] Data-quality checks: stale quotes, crossed books, out-of-order timestamps, outliers; share caught per venue and day

*Stage 2: streaming and cloud*
- [ ] Websockets for Deribit and Polymarket: reconnect with exponential backoff + jitter, resubscribe, sequence-gap detection, REST backfill
- [ ] Kalshi websocket + live CF Benchmarks index (only if an API key is created)
- [ ] Terraform for one small cloud VM; move recording off the laptop; record monthly cost
- [ ] Measure uptime and gap rate over the first multi-week run

### Phase 1 — C++ foundations (weeks 1–3)

- [x] CMake + GoogleTest + Google Benchmark + pybind11 module building from `pip install -e .` (via `uv sync`)
- [ ] Black–76 call, put, digital; analytic Greeks
- [ ] Implied-vol solver with bisection fallback; tests for deep ITM/OTM and near-expiry
- [ ] Put–call parity and price→IV→price round-trip tests
- [ ] Benchmark: IV solves per second

Done when: Python can call `ppm_core.implied_vol(...)` and all tests pass in CI.

### Phase 2 — Surface (weeks 3–5)

- [ ] SVI fit per expiry, with constraints and a weighted loss
- [ ] Butterfly and calendar arbitrage checks
- [ ] SSVI across expiries; evaluate σ(K, T) and ∂σ/∂K at any time
- [ ] Report of arbitrage violations found in raw Deribit quotes
- [ ] Plots: smiles per expiry, 3D surface, fit residuals
- [ ] Benchmark: full surface fit time

Done when: surfaces fit for a week of snapshots with no arbitrage flags.

### Phase 3 — Digital pricing and matching (weeks 5–6)

- [ ] Skew-corrected digital and range pricer in C++
- [ ] Contract matcher: prediction contract → strike, settlement time, index
- [ ] Options-implied probability next to Kalshi and Polymarket prices for every snapshot
- [ ] Sanity check: digitals from the surface agree with tight call spreads on Deribit

Done when: one table holds every snapshot's implied vs. market probability.

### Phase 4 — Research (weeks 6–8)

- [ ] Divergence study (Q1) with charts
- [ ] Calibration study (Q2): Brier, log loss, reliability diagrams
- [ ] Index basis study: BRTI vs. Binance vs. Deribit index

Done when: three findings, each stated in one sentence with a number.

### Phase 5 — Backtest and write-up (weeks 8–10)

- [ ] Cost model: spreads, fees, lockup
- [ ] Hedged and unhedged strategies with fractional Kelly
- [ ] Sensitivity analysis on costs and slippage
- [ ] Write-up in `docs/`: question, method, findings, limitations, next steps
- [ ] README with headline result, benchmark table and one hero chart

Done when: a stranger can read the README in 2 minutes and know what you found.

## Stretch goals

Pick these after Phase 5. Each one adds a distinct signal for interviews.

| Stretch | What it adds | Signal |
| --- | --- | --- |
| Live C++ engine | Subscribes to all three websockets, refits the surface incrementally, flags divergences; measured tick-to-signal latency with histograms | QD: concurrency, lock-free queues, latency engineering |
| Heston via COS method | Calibrate Heston to the chain, price digitals from the model, compare with SSVI | Math: characteristic functions, Fourier pricing |
| AAD Greeks | Sensitivity of every digital to every surface parameter in one backward pass | Industry technique, rare among students |
| ML divergence model | Predict whether a gap closes before settlement from gap size, time left, liquidity, recent volatility | Optional AI angle; must beat a simple baseline |

## Reading list and risks

**Read in this order:**

1. Black–Scholes, implied vol, digitals as call-spread limits: Hull (*Options, Futures, and Other Derivatives*) or Natenberg (*Option Volatility and Pricing*).
2. Gatheral, *The Volatility Surface* (chapters on SVI and local vol).
3. Gatheral and Jacquier, "Arbitrage-free SVI volatility surfaces" (2014).
4. Breeden and Litzenberger (1978) on risk-neutral densities from option prices.
5. Fang and Oosterlee (2008), the COS method (for the Heston stretch).
6. Jäckel, "Let's Be Rational" (for the IV solver stretch).
7. Kelly criterion and proper scoring rules (Brier, log loss).

**Risks and open questions:**

| Risk | Mitigation |
| --- | --- |
| API changes or rate limits break the recorder | Log failures, alert on gaps, back off and retry; keep raw responses |
| Too little settled data for calibration | Record from day 1; include hourly contracts for many more outcomes |
| Short-dated contracts poorly served by daily Deribit expiries | Treat hourly/15-min separately; compare with a realized-vol model from the perp |
| Thin prediction-market books distort mids | Filter by spread and depth; report results by liquidity bucket |
| Scope creep | Phases 0–5 are the project; everything else is a stretch |

Open question: which ETH contracts to include alongside BTC, if any.

## Using this plan with Claude Code

Export this doc as Markdown into `docs/PLAN.md`, add the `CLAUDE.md` below at the repo root, then work one phase checklist at a time.

**Seed `CLAUDE.md`:**

```markdown
# ppm — Pricing prediction markets with the options market

Goal: compare Kalshi/Polymarket BTC contract prices with digital-option probabilities
implied by an arbitrage-free SSVI surface fitted to Deribit options. Full plan: docs/PLAN.md.

## Stack
- C++20 core in cpp/ (CMake, GoogleTest, Google Benchmark), exposed via pybind11 as ppm_core
- Python package in python/ppm (recorders, matching, analysis); build with scikit-build-core
- Data: Parquet in data/ (git-ignored), UTC timestamps everywhere

## Conventions
- I am learning C++: explain non-obvious C++ choices briefly in PR descriptions or comments
- Every numerical function gets a unit test with a known answer or invariant (parity, round-trip)
- No look-ahead: analysis only uses data timestamped at or before the decision time
- Keep the hot path in C++, orchestration in Python
- Paper trading only; never add code that places real orders

## Commands
- Build + test C++: cmake --preset dev && cmake --build --preset dev && ctest --preset dev
- Python: pip install -e . && pytest
```

**First prompts, in order:**

1. "Read docs/PLAN.md and CLAUDE.md. Scaffold the repo layout from the architecture section, with CMake presets, pybind11 via scikit-build-core, and CI running both test suites."
2. "Implement the Phase 0 recorders for Deribit, Kalshi and Polymarket with a shared schema. Check each venue's current API docs first."
3. "Implement Phase 1: Black–76 pricer and the IV solver with tests and a benchmark. Explain the C++ design choices as you go."

**Tip:** ask Claude Code to tick the doc's checklist items in `docs/PLAN.md` as each one lands, so the plan stays the source of truth.
