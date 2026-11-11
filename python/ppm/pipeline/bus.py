"""Redis Streams transport between recorders and the writer.

Delivery guarantee: at-least-once. A message stays in Redis until the writer
acknowledges it (XACK) after the data is safely on disk, so a writer crash means
redelivery, never loss. Duplicates are made harmless downstream by idempotent writes.

Recorder side: `RedisBus.publish` falls back to a `DiskSpool` while Redis is
unreachable, then replays the spool in order once Redis is back.
"""

from __future__ import annotations

import enum
import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from redis import exceptions as redis_errors
from redis.asyncio import Redis

from ppm.pipeline.envelope import Envelope

log = logging.getLogger(__name__)

STREAM_PREFIX = "ppm:"
WRITER_GROUP = "writer"

# Errors meaning "Redis is unreachable or unhealthy right now", which is worth spooling for.
_REDIS_DOWN = (redis_errors.ConnectionError, redis_errors.TimeoutError, OSError)


def stream_name(venue: str) -> str:
    return f"{STREAM_PREFIX}{venue}"


class PublishResult(enum.Enum):
    PUBLISHED = "published"  # on the Redis stream
    SPOOLED = "spooled"  # Redis unreachable; saved to local disk for replay
    DROPPED = "dropped"  # Redis unreachable AND spool full: data lost, logged loudly


class DiskSpool:
    """Durable FIFO of envelopes on local disk.

    One file per envelope. File names start with a nanosecond timestamp, so sorting by
    name gives arrival order. Each file is written to a temp name and then renamed, so a
    crash never leaves a half-written entry. Bounded by `max_bytes`: when full, `append`
    refuses (backpressure) rather than filling the disk.
    """

    def __init__(self, directory: Path, max_bytes: int) -> None:
        self._dir = directory
        self._max_bytes = max_bytes
        self._dir.mkdir(parents=True, exist_ok=True)

    def append(self, env: Envelope) -> bool:
        blob = env.encode()
        if self.size_bytes() + len(blob) > self._max_bytes:
            return False
        final = self._dir / f"{time.time_ns():020d}-{env.venue}-{env.key}.env"
        tmp = final.with_suffix(".tmp")
        with open(tmp, "wb") as fh:
            fh.write(blob)
            fh.flush()
            os.fsync(fh.fileno())  # make it durable before it becomes visible
        os.replace(tmp, final)
        return True

    def entries(self) -> Iterator[tuple[Path, Envelope]]:
        """Oldest first."""
        for path in sorted(self._dir.glob("*.env")):
            yield path, Envelope.decode(path.read_bytes())

    def remove(self, path: Path) -> None:
        path.unlink(missing_ok=True)

    def __len__(self) -> int:
        return sum(1 for _ in self._dir.glob("*.env"))

    def size_bytes(self) -> int:
        return sum(p.stat().st_size for p in self._dir.glob("*.env"))


class RedisBus:
    """Recorder-side publisher with a disk-spool fallback."""

    def __init__(self, redis: Redis, spool: DiskSpool, stream_maxlen: int) -> None:
        self._redis = redis
        self._spool = spool
        self._maxlen = stream_maxlen

    async def publish(self, env: Envelope) -> PublishResult:
        try:
            # Drain older spooled envelopes first so the stream stays in arrival order.
            await self._drain_spool()
            await self._xadd(env)
            return PublishResult.PUBLISHED
        except _REDIS_DOWN as exc:
            if self._spool.append(env):
                log.warning("redis unavailable (%s); spooled %s/%s", exc, env.venue, env.key)
                return PublishResult.SPOOLED
            log.error(
                "DATA LOSS: redis unavailable and spool full; dropped %s/%s at %d",
                env.venue,
                env.key,
                env.snapshot_ts,
            )
            return PublishResult.DROPPED

    async def _drain_spool(self) -> None:
        for path, spooled in self._spool.entries():
            await self._xadd(spooled)
            # If the process dies between XADD and this remove, the entry is published
            # again on restart. That duplicate is harmless: the writer is idempotent.
            self._spool.remove(path)

    async def _xadd(self, env: Envelope) -> None:
        # approximate=True ("MAXLEN ~") lets Redis trim in efficient whole blocks.
        await self._redis.xadd(
            stream_name(env.venue),
            env.to_fields(),  # type: ignore[arg-type]
            maxlen=self._maxlen,
            approximate=True,
        )


@dataclass(frozen=True, slots=True)
class Delivery:
    """One message handed to the writer. It must be acked once safely processed."""

    stream: str
    message_id: str
    fields: dict[bytes, bytes]


class StreamConsumer:
    """Writer-side reader using a Redis consumer group.

    A consumer group tracks, per message, whether it has been acknowledged. Messages that
    were delivered but never acked (because the writer crashed) stay in the group's
    pending list. On startup we read our own pending list first (id "0") and only then
    ask for new messages (id ">"), so nothing delivered before a crash is skipped.
    """

    def __init__(
        self, redis: Redis, venues: list[str], consumer: str = "writer-1", group: str = WRITER_GROUP
    ) -> None:
        self._redis = redis
        self._streams = [stream_name(v) for v in venues]
        self._consumer = consumer
        self._group = group
        self._pending_done = False

    async def ensure_groups(self) -> None:
        for stream in self._streams:
            try:
                # id "0": a brand-new group starts from the beginning of the stream, so
                # anything recorders published before the writer first ran is kept.
                await self._redis.xgroup_create(stream, self._group, id="0", mkstream=True)
            except redis_errors.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):  # group already exists: fine
                    raise

    async def read(self, count: int = 20, block_ms: int = 5_000) -> list[Delivery]:
        if not self._pending_done:
            pending = await self._read(start_id="0", count=count, block_ms=None)
            if pending:
                return pending
            self._pending_done = True
        return await self._read(start_id=">", count=count, block_ms=block_ms)

    async def ack(self, delivery: Delivery) -> None:
        await self._redis.xack(delivery.stream, self._group, delivery.message_id)

    def rewind(self) -> None:
        """Re-read our pending (unacked) messages next time, e.g. after a transient
        processing failure such as a full disk, so they are retried rather than skipped."""
        self._pending_done = False

    async def _read(self, start_id: str, count: int, block_ms: int | None) -> list[Delivery]:
        response = await self._redis.xreadgroup(
            self._group,
            self._consumer,
            {s: start_id for s in self._streams},
            count=count,
            block=block_ms,
        )
        deliveries: list[Delivery] = []
        for stream, messages in response or []:
            for message_id, fields in messages:
                if not fields:  # pending entry whose data was trimmed by MAXLEN
                    log.error("DATA LOSS: pending message %s on %s was trimmed", message_id, stream)
                    await self._redis.xack(stream, self._group, message_id)
                    continue
                deliveries.append(
                    Delivery(stream=_text(stream), message_id=_text(message_id), fields=fields)
                )
        return deliveries


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


async def queue_drained(redis: Redis, venues: list[str], group: str = WRITER_GROUP) -> bool:
    """True when the writer has read and acknowledged everything on every stream.

    `pending` counts messages delivered but not yet acked; `lag` counts messages not yet
    delivered. The daily merge waits for both to be zero, so a day isn't finalized while
    some of its data is still in the queue.
    """
    for venue in venues:
        try:
            groups = await redis.xinfo_groups(stream_name(venue))
        except redis_errors.ResponseError:  # stream doesn't exist yet: nothing queued
            continue
        for g in groups:
            if _text(g["name"]) != group:
                continue
            if g.get("pending", 0) or g.get("lag", 0):
                return False
    return True
