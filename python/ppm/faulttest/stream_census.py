"""Count every message on the Redis streams by envelope key. Run inside the test stack:

    docker compose -p ppmtest exec -T writer python -m ppm.faulttest.stream_census

Prints one `venue<TAB>endpoint_key<TAB>snapshot_ts<TAB>count` line per key. This is the
failure test's third witness: what actually went through the queue, independent of the
recorders' logs and of the writer's file layout. A count above 1 is a duplicate delivery
(e.g. a spool replay after a crash) that idempotent writes must absorb. It runs in the
container because Redis isn't published to the host in the test stack.
"""

from __future__ import annotations

import os
from collections import Counter

from redis import Redis

from ppm.pipeline.bus import STREAM_PREFIX
from ppm.pipeline.envelope import Envelope

UNDECODABLE = "<undecodable>"  # venue slot for messages that aren't valid envelopes


def census(redis: Redis, page: int = 200) -> Counter[tuple[str, str, int]]:
    counts: Counter[tuple[str, str, int]] = Counter()
    for stream in redis.scan_iter(match=f"{STREAM_PREFIX}*", _type="stream"):
        last = "-"
        while True:
            # Paging keeps memory flat (Deribit bodies are ~100 KB each).
            batch = redis.xrange(stream, min=last, max="+", count=page)
            for _message_id, fields in batch:
                try:
                    env = Envelope.from_fields(fields)
                except (KeyError, ValueError):  # the writer quarantines these too
                    counts[(UNDECODABLE, stream.decode(), 0)] += 1
                    continue
                counts[(env.venue, env.key, env.snapshot_ts)] += 1
            if len(batch) < page:
                break
            last = "(" + batch[-1][0].decode()  # exclusive: continue after the last id
    return counts


def main() -> None:
    redis = Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    for (venue, key, ts), n in sorted(census(redis).items()):
        print(f"{venue}\t{key}\t{ts}\t{n}")


if __name__ == "__main__":
    main()
