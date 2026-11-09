# From the class pipeline to yours

The three class scripts from Mon 02 Nov already do the hard parts (batching, offsets after the file, the two kinds of duplicates, windows). You change **only the parts that depend on your source**. Work in this order; each step is one checkpoint.

## 0. Copy the class scripts (Session 1, Thu 05 Nov)

From the `project3` folder:

```bash
# macOS / Linux / Git Bash
cp ../quakes_producer.py producer.py
cp ../quakes_lander.py   lander.py
cp ../quakes_flow.py     flow.py
```
```powershell
# Windows PowerShell
Copy-Item ..\quakes_producer.py producer.py
Copy-Item ..\quakes_lander.py   lander.py
Copy-Item ..\quakes_flow.py     flow.py
```

Commit straight away (`git add producer.py lander.py flow.py`, `git commit -m "DP3: start from the class scripts"`): your history then shows exactly what you changed.

## 1. producer.py (checkpoint B)

| Part | In `quakes_producer.py` | What you change |
|---|---|---|
| `TOPIC` | `"quakes"` | Your topic name. Create it first: `docker exec -it redpanda-0 rpk topic create <topic> -p 3 -r 3` |
| `send()` | key = `data.properties.unid` | Your key from the Source Design Card. Missing key → log it, use `None` or a fallback, never crash |
| Envelope | `{"action": "history" or "create"/"update", "data": feature}` | Keep the same shape: `{"action": ..., "data": <the original message>}` |
| Connector | `history()` (REST, one day per request) and `live()` (WebSocket) | Your connection. `probe.py` has working code for WebSocket and REST polling: you may reuse it (say so in Acknowledgements) |
| Politeness | `time.sleep(1)` between requests, `ping_interval=15` | Respect the source's limits; on a dropped connection wait a few seconds and reconnect (a loop with a pause) |
| Logging | one line per event | Fast sources: one line every 1,000 messages, not every message |

**Done when** the Console (`http://127.0.0.1:8080`) shows at least 1,000 messages spread over your 3 partitions. Screenshot to `docs/`.

## 2. lander.py (checkpoint C)

| Part | In `quakes_lander.py` | What you change |
|---|---|---|
| `TOPIC`, consumer group | `"quakes"`, `"quakes_lander"` | Your topic; a new group name (e.g. `<topic>_lander`) |
| `SCHEMA` and `to_row()` | partition, offset, key, `action`, `unid`, 8 earthquake columns, `landed_at`, `raw` | **Keep** partition, offset, key, `action`, `landed_at`, `raw`. Replace the earthquake columns with 5-10 columns of yours (the id you dedupe on, the event time, what your question needs). Update the number of `?` in the `INSERT` |
| Malformed | invalid JSON → `action = "malformed"` | Keep it. Also catch a valid JSON message without your id |
| File names | `quakes_<time>_<id>.parquet` | Your prefix |
| Batching, `.tmp` + rename, `store_offsets` after the file, `crash` argument | | **Do not change**: this is what checkpoint D tests |

**Done when** `python -c "import duckdb; print(duckdb.sql(\"SELECT count(*) FROM 'data/raw/*.parquet'\"))"` matches the number of messages in your topic.

## 3. flow.py (checkpoint E)

| Part | In `quakes_flow.py` | What you change |
|---|---|---|
| `DB`, `CHART` | `quakes.duckdb`, `charts/quakes_per_day.png` | Your names |
| `RAW` | `"data/raw/*.parquet"` | Read a folder from the command line so the grader can use your sample: add `import sys` at the top, then `RAW = (sys.argv[1] if len(sys.argv) > 1 else "data/raw") + "/*.parquet"` |
| Stage | `SELECT DISTINCT * EXCLUDE (landed_at)`, malformed out | Same, plus: parse the nested fields you need from `raw` (`raw::JSON ->> '$.data.field'` or `json_extract_string`), cast types, trim text (e.g. OpenSky `callsign` has trailing spaces), drop messages that are not events (e.g. Coinbase `subscriptions` and `last_match`) |
| Entities | latest version per `unid` (`row_number() ... ORDER BY kafka_offset DESC`) | Latest version per **your** identity. Sources with one version per event (most of them): the DISTINCT on the id is enough, but say so in Design decisions |
| Marts and chart | 1-day `time_bucket` count | Your research question: windows, comparisons, anomalies or joins. One task per chart |
| The four numbers | raw, malformed, unique Kafka records, unique earthquakes | Same four, renamed for your entity |

**Done when** two runs in a row log the same four numbers, and `python flow.py data/sample` also runs (after `python kit/make_sample.py`).

## 4. Keep the class scripts working

Do not edit the `quakes_*.py` files in the folder above: they are your reference, and your Mon 02 Nov results depend on them.
