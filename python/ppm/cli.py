"""Command-line entry points.

ppm record deribit    run one venue recorder (one OS process per venue)
ppm writer            run the Parquet writer
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time

import httpx
from redis.asyncio import Redis

from ppm.config import Settings
from ppm.pipeline.bus import DiskSpool, RedisBus, StreamConsumer
from ppm.recorders.base import Recorder
from ppm.recorders.deribit import DeribitRecorder
from ppm.writer.storage import DataStore
from ppm.writer.writer import Writer

RECORDERS: dict[str, type[Recorder]] = {"deribit": DeribitRecorder}


def _configure_logging() -> None:
    logging.Formatter.converter = time.gmtime  # log timestamps in UTC, like the data
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s: %(message)s"
    )
    # httpx logs every request at INFO (thousands of lines a day); failures are
    # already logged by the recorder with their cause.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _stop_event() -> asyncio.Event:
    """An event set on Ctrl-C or SIGTERM, so processes finish the current step cleanly."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    return stop


async def _record(venue: str, settings: Settings) -> None:
    stop = _stop_event()
    redis = Redis.from_url(settings.redis_url, socket_timeout=5, socket_connect_timeout=5)
    spool = DiskSpool(settings.spool_dir / venue, settings.spool_max_bytes)
    bus = RedisBus(redis, spool, settings.stream_maxlen)
    headers = {"User-Agent": "ppm-recorder/0.1 (research, read-only)"}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        await RECORDERS[venue](client, bus).run_forever(stop)
    await redis.aclose()


async def _write(settings: Settings) -> None:
    stop = _stop_event()
    # The socket timeout must exceed the consumer's blocking-read time (5 s). Without an
    # explicit value redis-py 8.1 aborted 5 s blocking reads with TimeoutError (2026-11-10).
    redis = Redis.from_url(settings.redis_url, socket_timeout=30, socket_connect_timeout=5)
    consumer = StreamConsumer(redis, venues=sorted(RECORDERS))
    await Writer(consumer, DataStore(settings.data_dir)).run_forever(stop)
    await redis.aclose()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="ppm")
    sub = ap.add_subparsers(dest="command", required=True)
    rec = sub.add_parser("record", help="run a venue recorder")
    rec.add_argument("venue", choices=sorted(RECORDERS))
    sub.add_parser("writer", help="run the Parquet writer")
    args = ap.parse_args(argv)

    _configure_logging()
    settings = Settings.from_env()
    if args.command == "record":
        asyncio.run(_record(args.venue, settings))
    elif args.command == "writer":
        asyncio.run(_write(settings))


if __name__ == "__main__":
    main()
