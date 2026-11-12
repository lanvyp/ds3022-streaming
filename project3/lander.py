"""lander.py: Kafka topic bluesky_posts -> raw Parquet files in data/raw/.

  python lander.py          # land forever (Ctrl+C to stop)
  python lander.py crash    # card D1: write one file, then exit BEFORE storing offsets

Rules (checkpoint C, do not change):
  - a file closes at 1,000 records or after 60 seconds, whichever comes first
  - write to .tmp first, then rename (no half-written files)
  - store offsets only AFTER the file exists (at-least-once)
  - bad messages are landed as action = 'malformed', never skipped, never a crash
"""
import json
import math
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
from quixstreams import Application

BROKER = "localhost:19092"
TOPIC = "bluesky_posts"
GROUP = "bluesky_posts_lander"
OUT_DIR = "data/raw"
BATCH_SIZE = 1000
BATCH_SECONDS = 60

SCHEMA = """kafka_partition INTEGER, kafka_offset BIGINT, kafka_key VARCHAR,
    action VARCHAR, kind VARCHAR, operation VARCHAR, did VARCHAR, rkey VARCHAR,
    time_us BIGINT, lang VARCHAR, text VARCHAR,
    landed_at TIMESTAMP, raw VARCHAR"""


def number(value):
    """A number, or None if it is not a number."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def to_row(msg):
    """One Kafka message -> one row, with the columns of SCHEMA in the same order."""
    raw = msg.value().decode("utf8", errors="replace") if msg.value() else ""
    try:
        m = json.loads(raw)
        d = m["data"]                      # the Jetstream event, inside the envelope
        action = m.get("action", "unknown")
        if not isinstance(d, dict):
            raise TypeError("data is not an object")
        c = d.get("commit") or {}          # identity/account events: no commit
        r = c.get("record") or {}          # deleted posts: no record
    except (ValueError, KeyError, TypeError, AttributeError):
        d, c, r, action = {}, {}, {}, "malformed"
    langs = r.get("langs") or [None]
    key = msg.key().decode("utf8", errors="replace") if msg.key() else None
    return (msg.partition(), msg.offset(), key, action, d.get("kind"),
            c.get("operation"), d.get("did"), c.get("rkey"), d.get("time_us"),
            langs[0], r.get("text"),
            datetime.now(timezone.utc).replace(tzinfo=None), raw)


def write_batch(rows):
    """Write rows to a .tmp Parquet file, then rename it. Returns the file name."""
    name = (f"{OUT_DIR}/bluesky_"
            f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}.parquet")
    tmp = name + ".tmp"
    con = duckdb.connect()                 # in memory
    con.execute(f"CREATE TABLE batch ({SCHEMA})")
    con.executemany(f"INSERT INTO batch VALUES ({', '.join(['?'] * len(rows[0]))})",
                    rows)
    con.execute(f"COPY batch TO '{tmp}' (FORMAT parquet)")
    con.close()
    os.replace(tmp, name)                  # the file appears in one step
    return name


def main():
    crash = len(sys.argv) > 1 and sys.argv[1] == "crash"
    os.makedirs(OUT_DIR, exist_ok=True)
    app = Application(broker_address=BROKER, consumer_group=GROUP,
                      auto_offset_reset="earliest", loglevel="WARNING")
    print(f"Landing records from {TOPIC} into {OUT_DIR} (Ctrl+C to stop)")

    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC])
        rows = []
        last_msg = {}                      # partition -> last message in this batch
        started = None

        while True:
            msg = consumer.poll(1)
            if msg is not None:
                if msg.error() is not None:
                    print(f"Kafka error: {msg.error()}")
                else:
                    rows.append(to_row(msg))
                    last_msg[msg.partition()] = msg
                    if started is None:
                        started = time.monotonic()

            full = len(rows) >= BATCH_SIZE
            old = started is not None and time.monotonic() - started >= BATCH_SECONDS
            if rows and (full or old):
                name = write_batch(rows)
                print(f"Landed {len(rows):>5} records -> {name}")

                if crash:
                    print("CRASH (on purpose): the file exists, the offsets were NOT stored")
                    os._exit(1)            # exit at once: nothing is committed

                for partition_msg in last_msg.values():
                    consumer.store_offsets(message=partition_msg)   # only now
                rows = []
                last_msg = {}
                started = None


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")