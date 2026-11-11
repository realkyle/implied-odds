"""Reference-price recorder: the spot prices the prediction markets settle on.

- Binance BTC/USDT and ETH/USDT 1-minute candles. Polymarket settles on the close of the
  Binance 1m candle "closing at 12:00 ET". api.binance.com returns HTTP 451 from the
  owner's location; data-api.binance.vision serves the same public market data.
- Coinbase BTC-USD and ETH-USD ticker, a proxy for CF Benchmarks' BRTI (Kalshi's
  settlement index, which has no free real-time feed). Kalshi's settled markets report
  the actual BRTI average; the settlement collector records it.
"""

from __future__ import annotations

from ppm.recorders.base import FetchSpec, Recorder

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"
COINBASE_URL = "https://api.exchange.coinbase.com/products/{product}/ticker"


class ReferenceRecorder(Recorder):
    venue = "reference"
    binance_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    coinbase_products: tuple[str, ...] = ("BTC-USD", "ETH-USD")

    async def fetch_specs(self, snapshot_ts: int) -> list[FetchSpec]:
        specs = [
            # limit=2: the candle that just closed plus the one now forming. The parser
            # keeps only the closed one.
            FetchSpec(
                endpoint="binance_klines",
                url=BINANCE_URL,
                params={"symbol": s},
                query={"symbol": s, "interval": "1m", "limit": "2"},
            )
            for s in self.binance_symbols
        ]
        specs += [
            FetchSpec(
                endpoint="coinbase_ticker",
                url=COINBASE_URL.format(product=p),
                params={"product": p},
            )
            for p in self.coinbase_products
        ]
        return specs
