"""The writer process: Redis stream → parse → validate → Parquet, then acknowledge.

Order of operations is what makes this safe:
  1. write raw archive, tables, quarantine and fetch log (all atomic, all idempotent)
  2. only then XACK
A crash before step 2 means Redis redelivers the message; redoing step 1 overwrites the
same files with the same content. Result: no loss, no duplicates.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from redis import exceptions as redis_errors

from ppm.pipeline.bus import Delivery, StreamConsumer
from ppm.pipeline.envelope import Envelope
from ppm.writer.parsers import parse
from ppm.writer.schemas import Rejected, validate_rows
from ppm.writer.storage import DataStore

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Outcome:
    rows: int
    rejected: int
    error: str | None
    server_ts: int | None = None  # venue server clock (ms) if the response carries one


class Writer:
    def __init__(
        self,
        consumer: StreamConsumer,
        store: DataStore,
        retry_delay_s: float = 5.0,
        max_reconnect_delay_s: float = 60.0,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._retry_delay_s = retry_delay_s
        self._max_reconnect_delay_s = max_reconnect_delay_s

    async def run_forever(self, stop: asyncio.Event) -> None:
        log.info("writer started")
        failures = 0
        groups_ready = False
        while not stop.is_set():
            try:
                if not groups_ready:
                    await self._consumer.ensure_groups()
                    groups_ready = True
                await self.run_once()
                if failures:
                    log.info("redis connection recovered after %d failed attempts", failures)
                failures = 0
            except (redis_errors.ConnectionError, redis_errors.TimeoutError, OSError) as exc:
                # Redis restarting or unreachable: nothing is lost (unacked messages stay
                # pending in Redis), so wait and retry with capped exponential backoff.
                failures += 1
                delay = min(self._retry_delay_s * 2 ** (failures - 1), self._max_reconnect_delay_s)
                log.warning("redis unavailable (%r); retrying in %.1f s", exc, delay)
                self._consumer.rewind()
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
        log.info("writer stopped")

    async def run_once(self, block_ms: int = 5_000) -> int:
        """Read and process one batch. Returns the number of messages acknowledged."""
        acked = 0
        for delivery in await self._consumer.read(block_ms=block_ms):
            try:
                self.process(delivery)
            except OSError:
                # Disk full, permission error, ...: transient. Don't ack; retry later.
                log.exception("write failed for %s; will retry", delivery.message_id)
                self._consumer.rewind()
                await asyncio.sleep(self._retry_delay_s)
                break
            await self._consumer.ack(delivery)
            acked += 1
        return acked

    def process(self, delivery: Delivery) -> Outcome:
        try:
            env = Envelope.from_fields(delivery.fields)
        except Exception as exc:  # malformed message: keep it for inspection, then move on
            path = self._store.write_undecodable(
                delivery.stream, delivery.message_id, delivery.fields, repr(exc)
            )
            log.error("undecodable message %s quarantined at %s", delivery.message_id, path)
            return Outcome(0, 0, f"undecodable: {exc}")

        self._store.write_raw(env)
        outcome = self._write_tables(env) if env.ok else Outcome(0, 0, _error_text(env))
        self._write_fetch_log(env, outcome)
        return outcome

    def _write_tables(self, env: Envelope) -> Outcome:
        try:
            result = parse(env)
        except Exception as exc:  # parser bug or unexpected payload; raw is archived for replay
            log.error("parse failed for %s/%s at %d: %r", env.venue, env.key, env.snapshot_ts, exc)
            self._store.write_quarantine(
                "_unparsed", env, [Rejected({"venue": env.venue, "key": env.key}, repr(exc))]
            )
            return Outcome(0, 0, f"parse failed: {exc!r}")

        rows = rejected = 0
        tables = set(result.tables) | set(result.rejected)
        for table in sorted(tables):
            arrow, bad = validate_rows(table, result.tables.get(table, []))
            bad = result.rejected.get(table, []) + bad
            if arrow is not None:
                self._store.write_table(table, env, arrow)
                rows += arrow.num_rows
            if bad:
                self._store.write_quarantine(table, env, bad)
                rejected += len(bad)
                log.warning(
                    "%d rows quarantined for %s from %s/%s", len(bad), table, env.venue, env.key
                )
        return Outcome(rows, rejected, None, result.server_ts)

    def _write_fetch_log(self, env: Envelope, outcome: Outcome) -> None:
        row = {
            "snapshot_ts": env.snapshot_ts,
            "venue": env.venue,
            "endpoint_key": env.key,
            "request_ts": env.request_ts,
            "recv_ts": env.recv_ts,
            "latency_ms": env.recv_ts - env.request_ts,
            "status": env.status,
            "error": outcome.error,
            "server_ts": outcome.server_ts,
            "rows": outcome.rows if outcome.error is None else None,
            "rejected": outcome.rejected,
        }
        arrow, bad = validate_rows("fetch_log", [row])
        if arrow is not None:
            self._store.write_table("fetch_log", env, arrow)
        if bad:
            self._store.write_quarantine("fetch_log", env, bad)


def _error_text(env: Envelope) -> str:
    return env.body.decode(errors="replace")[:1000]
