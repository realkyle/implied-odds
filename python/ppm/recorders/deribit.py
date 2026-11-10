"""Deribit recorder: option chains, futures/perpetuals and index prices for BTC and ETH.

Six public REST calls per tick (no API key). `get_book_summary_by_currency` returns the
whole chain in one call, which keeps each option chain an atomic snapshot.
"""

from __future__ import annotations

from ppm.recorders.base import FetchSpec, Recorder

BASE_URL = "https://www.deribit.com/api/v2/public"


class DeribitRecorder(Recorder):
    venue = "deribit"
    currencies: tuple[str, ...] = ("BTC", "ETH")

    async def fetch_specs(self) -> list[FetchSpec]:
        specs: list[FetchSpec] = []
        for currency in self.currencies:
            for kind in ("option", "future"):
                specs.append(
                    FetchSpec(
                        endpoint="book_summary",
                        url=f"{BASE_URL}/get_book_summary_by_currency",
                        params={"currency": currency, "kind": kind},
                        query={"currency": currency, "kind": kind},
                    )
                )
            index_name = f"{currency.lower()}_usd"
            specs.append(
                FetchSpec(
                    endpoint="index_price",
                    url=f"{BASE_URL}/get_index_price",
                    params={"index_name": index_name},
                    query={"index_name": index_name},
                )
            )
        return specs
