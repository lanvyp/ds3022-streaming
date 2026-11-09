"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

DB = "quakes.duckdb"
RAW = "data/raw/*.parquet"
CHART = "charts/quakes_per_day.png"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_quakes AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_quakes").fetchone()[0]


@task
def clean():
    with duckdb.connect(DB) as con:
        # 1. The same Kafka record landed twice (at-least-once): keep one copy.
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_quakes
            WHERE action <> 'malformed' AND unid IS NOT NULL
        """)
        # 2. The same earthquake sent several times (create, updates): keep the latest.
        #    Same key -> same partition -> a higher offset is a later version.
        con.execute("""
            CREATE OR REPLACE TABLE quakes AS
            SELECT unid, CAST(event_time AS TIMESTAMP) AS event_time, mag, magtype,
                   region, lat, lon, depth
            FROM (
                SELECT *, row_number() OVER (PARTITION BY unid ORDER BY kafka_offset DESC) AS rn
                FROM stg_records
            )
            WHERE rn = 1
        """)
        return {
            "malformed": con.execute("SELECT count(*) FROM raw_quakes WHERE action = 'malformed'").fetchone()[0],
            "records": con.execute("SELECT count(*) FROM stg_records").fetchone()[0],
            "events": con.execute("SELECT count(*) FROM quakes").fetchone()[0],
        }


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT time_bucket(INTERVAL '1 day', event_time) AS day,  -- 1-day tumbling windows
                   count(*) AS quakes
            FROM quakes
            GROUP BY day
            ORDER BY day
        """).fetchall()
    os.makedirs("charts", exist_ok=True)
    days = [r[0].strftime("%d %b") for r in rows]
    plt.figure(figsize=(8, 4))
    plt.bar(days, [r[1] for r in rows], color="#2a7fb8")
    plt.title("Earthquakes per day (EMSC), duplicates removed")
    plt.ylabel("events")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    return CHART


@flow(name="quakes-pipeline")
def quakes_pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = clean()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique earthquakes:        %d", n["events"])
    log.info("Chart: %s", path)


if __name__ == "__main__":
    quakes_pipeline()
