import sys
import duckdb


USAGE = "Give your id column(s), e.g.   python four_numbers.py did,rkey"
HELP = "Check: run it from project3, at least one file landed, id names as in your SCHEMA."
sys.excepthook = lambda kind, err, tb: print(f"{err}\n{HELP}", file=sys.stderr)
IDS = sys.argv[1].split(",") if len(sys.argv) > 1 else sys.exit(USAGE)
RAW, OK = "read_parquet('data/raw/*.parquet')", "action <> 'malformed'"
HAS_ID = " AND ".join(f"{c} IS NOT NULL" for c in IDS)
BAD = "count(*) FILTER (WHERE action = 'malformed')"
KEYS = f"SELECT DISTINCT kafka_partition, kafka_offset FROM {RAW} WHERE {OK}"
THINGS = f"SELECT DISTINCT {', '.join(IDS)} FROM {RAW} WHERE {OK} AND {HAS_ID}"
q = duckdb.connect().execute                         # in memory: reads the files, writes nothing
raw, bad = q(f"SELECT count(*), {BAD} FROM {RAW}").fetchone()
records = q(f"SELECT count(*) FROM ({KEYS})").fetchone()[0]
things = q(f"SELECT count(*) FROM ({THINGS})").fetchone()[0]
print(f"raw {raw:,} | malformed {bad:,} | unique Kafka records {records:,}", end="")
print(f" | unique {'+'.join(IDS)} {things:,}")