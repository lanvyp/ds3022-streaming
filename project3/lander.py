"""Kafka topic "quakes" -> raw Parquet files in data/raw/.

A batch is written when it has 1000 records or is 60 seconds old.
Offsets are stored only AFTER the file exists: at-least-once.

  python quakes_lander.py
  python quakes_lander.py crash   # experiment: crash after the first file, before storing offsets
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
from quixstreams import Application

BROKER = "127.0.0.1:19092"
TOPIC = "quakes"
OUT_DIR = "data/raw"
BATCH_SIZE = 1000
BATCH_SECONDS = 60
CRASH_AFTER_WRITE = "crash" in sys.argv[1:]

SCHEMA = """kafka_partition INTEGER, kafka_offset BIGINT, kafka_key VARCHAR,
    action VARCHAR, unid VARCHAR, event_time VARCHAR, mag DOUBLE, magtype VARCHAR,
    region VARCHAR, lat DOUBLE, lon DOUBLE, depth DOUBLE, lastupdate VARCHAR,
    landed_at TIMESTAMP, raw VARCHAR"""


def number(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def to_row(msg):
    """Keep the whole message (raw) plus a few columns. Never crash on bad input."""
    raw = msg.value().decode("utf8", errors="replace") if msg.value() else ""
    try:
        m = json.loads(raw)
        p = m["data"]["properties"]
        action = m.get("action", "unknown")
    except (ValueError, KeyError, TypeError, AttributeError):
        m, p, action = {}, {}, "malformed"
    key = msg.key().decode("utf8", errors="replace") if msg.key() else None
    return (msg.partition(), msg.offset(), key, action, p.get("unid"), p.get("time"),
            number(p.get("mag")), p.get("magtype"), p.get("flynn_region"),
            number(p.get("lat")), number(p.get("lon")), number(p.get("depth")),
            p.get("lastupdate"), datetime.now(timezone.utc).replace(tzinfo=None), raw)


def write_batch(rows):
    """Write one Parquet file. Write to .tmp first, then rename,
    so the flow never reads a half-written file."""
    os.makedirs(OUT_DIR, exist_ok=True)
    name = f"{OUT_DIR}/quakes_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}.parquet"
    con = duckdb.connect()  # in memory: no database file, no lock
    con.execute(f"CREATE TABLE batch ({SCHEMA})")
    con.executemany(f"INSERT INTO batch VALUES ({', '.join(['?'] * 15)})", rows)
    con.execute(f"COPY batch TO '{name}.tmp' (FORMAT parquet)")
    con.close()
    os.replace(f"{name}.tmp", name)
    return name


def main():
    app = Application(broker_address=BROKER, consumer_group="quakes_lander",
                      auto_offset_reset="earliest", loglevel="WARNING")
    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC])
        rows, last, started = [], {}, time.monotonic()
        print("Landing records from", TOPIC, "into", OUT_DIR, "(Ctrl+C to stop)")
        while True:
            msg = consumer.poll(1)
            if msg is not None:
                if msg.error() is not None:
                    raise Exception(msg.error())
                if not rows:
                    started = time.monotonic()
                rows.append(to_row(msg))
                last[msg.partition()] = msg  # newest message per partition
            full = len(rows) >= BATCH_SIZE
            old = rows and time.monotonic() - started >= BATCH_SECONDS
            if full or old:
                path = write_batch(rows)
                print(f"Landed {len(rows):4d} records -> {path}")
                if CRASH_AFTER_WRITE:
                    print("CRASH (on purpose): the file exists, the offsets were NOT stored")
                    os._exit(1)
                for m in last.values():
                    consumer.store_offsets(message=m)  # only now: "these are done"
                rows, last = [], {}


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass  # records not landed yet were not stored, so they are read again next time
