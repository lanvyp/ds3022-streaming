# Chaos cards (checkpoint D, Session 2 · Mon 09 Nov)

Break your own pipeline on purpose and show, with numbers, that it does not lose data. **D1 and D2 are compulsory; do at least one more.** Each card becomes one row of the **Chaos round** table in your README.

## How to run a card

1. **Numbers before.** Run the flow and write down the four numbers:
   ```
   python flow.py
   ```
   raw records landed · malformed · unique Kafka records · unique entities
2. **Predict.** Write in one sentence what you expect to change, *before* you do anything.
3. **Do** the steps on the card.
4. **Numbers after.** Run the flow again and write the four numbers down.
5. **Explain** the difference in one or two sentences, using the words on the card.
6. **Copy the row** into the README table: card · prediction · what I did · before → after · explanation.

Terminals as on Mon 02 Nov: **T1** producer, **T2** lander, **T3** flow, **T4** `rpk`, `docker`, `git`. Run project commands from the `project3` folder.

---

## D1 · Crash between the file and the offsets (compulsory)

*Same idea as card A on Mon 02 Nov, now on your own lander.*

1. T2: stop the lander (Ctrl+C).
2. T1: send some records again: re-run your backfill for a short range, or let the live producer run for a minute.
3. T2: `python lander.py crash` → it writes one file, prints `CRASH (on purpose)` and exits **before** storing offsets.
4. T2: `python lander.py` → it reads the same records again and writes them to a second file.
5. T3: `python flow.py`

| Expect | Raw records | Unique Kafka records | Unique entities |
|---|---|---|---|
| Change | up by about **2×** the records in the crashed batch | up by **1×** | up by the new entities only |

**Explain with:** at-least-once, offsets stored after the file, `DISTINCT` makes the re-landed copy harmless (idempotent flow).

## D2 · A poison message (compulsory)

*Same idea as card B on Mon 02 Nov.*

1. T4: `docker exec -it redpanda-0 rpk topic produce <your-topic> -k bad`
2. Type `this is not JSON`, press Enter, then Ctrl+C.
3. T2: wait for the next `Landed ... records` line (at most 60 s). The lander must **keep running**.
4. T3: `python flow.py`

| Expect | Raw records | Malformed | Unique Kafka records | Unique entities |
|---|---|---|---|---|
| Change | +1 | **+1** | no change | no change |

Find the row: `python -c "import duckdb; duckdb.sql(\"SELECT kafka_partition, kafka_offset, raw FROM 'data/raw/*.parquet' WHERE action='malformed'\").show()"`

**Explain with:** land everything, decide later; a consumer that crashes on bad input reads it again after every restart and blocks its partition (poison message).

## D3 · A broker goes down

*Same idea as the broker card on Thu 22 Oct.*

1. T1: producer running.
2. T4: `docker stop redpanda-1` (never stop `redpanda-0`: it is the address your scripts use).
3. Watch the producer for 60 s, and the Console (`http://127.0.0.1:8080`): partitions show as under-replicated.
4. T4: `docker start redpanda-1`. Wait until the Console is healthy again.
5. T3: `python flow.py`

**Expect:** the producer keeps going (maybe a short pause); raw records keep growing; nothing is lost.
**Explain with:** replication factor 3, leaders move to the brokers that are still up, a majority (2 of 3) is enough to keep writing.

## D4 · The source drops

1. T1: producer running live.
2. Cut the source: turn Wi-Fi off for 30 seconds, or stop the producer (Ctrl+C) and start it again after 30 seconds.
3. Watch the producer log: does it reconnect by itself? (If it crashes, that is a finding: add a reconnect loop with a pause.)
4. T3: `python flow.py`

**Expect:** push sources (WebSocket): a gap in the data while you were offline. Pull sources (polling): the producer just misses the snapshots while you were offline; a source with history could fill the gap.
**Explain with:** push vs. pull, what your producer can and cannot recover, why a source with history could recover.

## D5 · Send it all again

1. T1: send again records you **already** landed. OpenSky: run two polls less than 10 seconds apart, so the same snapshot is sent twice. Bluesky: restart the producer with a Jetstream `?cursor=` a few minutes before your last landed `time_us`. Own source with history: re-run your backfill for a range you already landed. Coinbase has no replay: skip this card.
2. T2: let the lander land it.
3. T3: `python flow.py`

**Expect:** raw records and unique Kafka records go **up** (these are new Kafka records with new offsets); unique entities do **not** change.
**Explain with:** the second kind of duplicate: the same thing sent twice is two different Kafka records, so `DISTINCT` cannot remove it; the entity step (dedupe by your id) does.

---

## Example README row

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D2 Poison message | The lander keeps running; malformed goes from 0 to 1 | `rpk topic produce coinbase_trades -k bad`, typed text, waited for the next file, ran the flow | raw 61,204 → 61,205; malformed 0 → 1; unique records 58,930 → 58,930; unique trades 58,911 → 58,911 | `to_row()` caught the JSON error and landed the text with `action = "malformed"`, so the lander did not crash and the partition was not blocked. The flow keeps it in raw and leaves it out of the clean tables. |
