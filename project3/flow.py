"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

DB = "bluesky.duckdb"
RAW = (sys.argv[1] if len(sys.argv) > 1 else "data/raw") + "/*.parquet"
CHART = "charts/reply_share_by_language.png"
TOP_LANGUAGES = 5
WINDOW = "10 minutes"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    """Copy every raw row (duplicates and malformed included) into DuckDB."""
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer 
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        raw = con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]
        malformed = con.execute("SELECT count(*) FROM raw_posts WHERE action = 'malformed'").fetchone()[0]
    return raw, malformed


@task
def stage():
    """Duplicate kind 1: the same Kafka record landed twice (card D1). Keep one copy.
    Also drop malformed rows and parse the fields the analysis needs from raw."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT
                kafka_partition,
                kafka_offset,
                kind,
                operation,
                did,
                rkey,
                time_us,
                make_timestamp(time_us) AS event_time,                       -- microseconds -> timestamp (UTC)
                lower(trim(lang)) AS lang,
                json_extract(raw, '$.data.commit.record.reply') IS NOT NULL AS is_reply
            FROM raw_posts
            WHERE action <> 'malformed'
        """)
        records = con.execute("SELECT count(*) FROM stg_records").fetchone()[0]
    return records


@task
def entities():
    """Duplicate kind 2: the same post sent again with a new offset (card D5).
    Keep one row per post (did + rkey): its 'create' event, the first one in Kafka.
    Same key -> same partition, so a lower offset is the earlier copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, event_time, lang, is_reply
            FROM (
                SELECT *,
                       row_number() OVER (PARTITION BY did, rkey ORDER BY kafka_offset) AS rn
                FROM stg_records
                WHERE kind = 'commit'
                  AND operation = 'create'
                  AND did IS NOT NULL
                  AND rkey IS NOT NULL
            )
            WHERE rn = 1
        """)
        posts = con.execute("SELECT count(*) FROM posts").fetchone()[0]
    return posts


@task
def marts():
    """Reply share per language: per 10-minute window, and overall for the top languages."""
    with duckdb.connect(DB) as con:
        # the most common languages
        con.execute(f"""
            CREATE OR REPLACE TABLE top_languages AS
            SELECT lang, count(*) AS posts
            FROM posts
            WHERE lang IS NOT NULL
            GROUP BY lang
            ORDER BY posts DESC
            LIMIT {TOP_LANGUAGES}
        """)
        # 10-minute tumbling windows on event time per language
        con.execute(f"""
            CREATE OR REPLACE TABLE mart_reply_share_10min AS
            SELECT time_bucket(INTERVAL '{WINDOW}', p.event_time) AS window_start,
                   p.lang,
                   count(*) AS posts,
                   count(*) FILTER (WHERE p.is_reply) AS replies,
                   round(100.0 * count(*) FILTER (WHERE p.is_reply) / count(*), 1) AS reply_share_pct
            FROM posts AS p
            JOIN top_languages AS t ON p.lang = t.lang
            GROUP BY window_start, p.lang
            ORDER BY window_start, p.lang
        """)
        # one line per language which overall share and how much it moves between windows
        con.execute("""
            CREATE OR REPLACE TABLE mart_reply_share AS
            SELECT w.lang,
                   sum(w.posts) AS posts,
                   sum(w.replies) AS replies,
                   round(100.0 * sum(w.replies) / sum(w.posts), 1) AS reply_share_pct,
                   min(w.reply_share_pct) AS lowest_window_pct,
                   max(w.reply_share_pct) AS highest_window_pct,
                   count(*) AS windows
            FROM mart_reply_share_10min AS w
            GROUP BY w.lang
            ORDER BY reply_share_pct DESC
        """)
        rows = con.execute("SELECT * FROM mart_reply_share").fetchall()
    return rows


@task
def charts(rows):
    """Bar chart: reply share for the top languages."""
    os.makedirs("charts", exist_ok=True)
    languages = []
    shares = []
    labels = []
    for row in rows:
        lang = row[0]
        posts = row[1]
        share = row[3]
        languages.append(lang)
        shares.append(share)
        labels.append(f"{share}%\n({posts:,} posts)")

    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(languages, shares, color="#2a7fb8")
    plt.bar_label(bars, labels=labels, padding=3, fontsize=9)
    plt.title("Share of Bluesky posts that are replies, top 5 languages\n(Jetstream, duplicates removed)")
    plt.xlabel("Post language (first entry of langs)")
    plt.ylabel("Replies (% of the language's posts)")
    if shares:
        plt.ylim(0, max(shares) * 1.25)
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART


@flow(name="bluesky-pipeline")
def bluesky_pipeline():
    log = get_run_logger()
    log.info("Reading %s", RAW)
    raw, malformed = load_raw()
    records = stage()
    posts = entities()
    rows = marts()
    path = charts(rows)
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", malformed)
    log.info("Unique Kafka records:      %d", records)
    log.info("Unique posts (did + rkey): %d", posts)
    for row in rows:
        log.info("  %s: %s%% replies (%d posts, %d windows)", row[0], row[3], row[1], row[6])
    log.info("Chart: %s", path)


if __name__ == "__main__":
    bluesky_pipeline()