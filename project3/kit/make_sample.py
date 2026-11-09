"""Make data/sample/: a small copy of your raw landing files, committed to git,
so anyone can run your flow without Kafka:  python flow.py data/sample

  python kit/make_sample.py          # 5,000 random raw rows (default)
  python kit/make_sample.py 3000

Run it from the project3 folder. It keeps every column, including raw, so the
sample still has duplicates and malformed rows for the flow to clean.
"""
import glob
import os
import sys

import duckdb

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
RAW = "data/raw/*.parquet"
OUT = "data/sample/sample.parquet"

if not glob.glob(RAW):
    sys.exit("No files in data/raw/. Run your lander first (and run this from the project3 folder).")
os.makedirs("data/sample", exist_ok=True)
con = duckdb.connect()
con.execute(f"COPY (SELECT * FROM read_parquet('{RAW}') USING SAMPLE {ROWS} ROWS) TO '{OUT}' (FORMAT parquet)")
n = con.execute(f"SELECT count(*) FROM '{OUT}'").fetchone()[0]
mb = os.path.getsize(OUT) / 1e6
print(f"Wrote {n} rows to {OUT} ({mb:.2f} MB)")
if mb > 5:
    print("Over 5 MB: run again with fewer rows, e.g. python kit/make_sample.py 2000")
