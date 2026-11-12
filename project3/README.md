# Bluesky in Real Time: Which Languages Reply the Most?

**Author:** Lanvy Phan
**DS 3022 Data Project 3, Fall 2026**

## Data source

I stream new and deleted Bluesky posts from **Jetstream**, a public service that turns the Bluesky firehose into plain JSON over a **WebSocket** (push):
`wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`

The `wantedCollections` filter asks only for posts (no likes, follows or reposts). No account or API key is needed; my producer sends a User-Agent anyway. I stream live, and Jetstream can also replay a limited window with `?cursor=<time_us>`, which I used for card D5.

One message is one event from one account: `did` (the account id), `time_us` (when Jetstream received it, in microseconds), `kind` (`commit` for posts, `identity`/`account` for account changes) and `commit` with `operation` (`create` or `delete`), `rkey` (the post id within the account) and, for new posts, `record` with `text`, `langs`, `createdAt` and `reply` (present only if the post is a reply). In my probe (Mon 09 Nov) the rate was **45.1 messages/s**; 1.4% of messages had no `commit` and 6.4% had no `record` (deletes).

## How to run

From a fresh clone, in a terminal (macOS / Linux):

```bash
# 1. Virtual environment and packages (in the ds3022-streaming folder)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install prefect matplotlib

# 2. Start the 3-broker cluster (never use -v: it deletes the topics)
docker compose up -d
docker exec -it redpanda-0 rpk cluster info

# 3. Create the topic BEFORE the producer runs
docker exec -it redpanda-0 rpk topic create bluesky_posts -p 3 -r 3

# 4. Terminal 1: producer (Ctrl+C to stop)
cd project3
python producer.py

# 5. Terminal 2: lander (Ctrl+C to stop); files appear in data/raw/
cd project3
python lander.py

# 6. Terminal 3: the four numbers from the raw files
cd project3
python four_numbers.py did,rkey

# 7. The flow, on the full data and on the committed sample
python flow.py
python flow.py data/sample      # runs without Kafka, on the committed sample
```

Stop everything with Ctrl+C in each terminal, then `docker compose down` in the `ds3022-streaming` folder.

## Architecture

```
Jetstream WebSocket (wss://jetstream2.us-east.bsky.network, posts only)
        │  push, ~45 messages/s
        ▼
producer.py ── envelope {"action": "live", "data": <event>}, key = did
        │
        ▼
Kafka topic bluesky_posts  (3 partitions, replication factor 3, Redpanda brokers 0-2)
        │  consumer group bluesky_posts_lander
        ▼
lander.py ── 1,000 records or 60 s per file, .tmp then rename, offsets stored after the file
        │
        ▼
data/raw/bluesky_<time>_<id>.parquet   (13 columns, incl. raw JSON)
        │
        ▼
flow.py (Prefect + DuckDB)
   load_raw → stage → entities → marts → charts
   TODO: table and chart names once flow.py is written
```

## Design decisions

1. **Key and partitions.** My key is `did`, the account id. All events of one account go to the same partition in order, so a post's `create` always comes before its `delete`. With thousands of accounts the load spreads over the 3 partitions, but not evenly: after my first run the partitions held 1,758 / 1,973 / 4,529 messages, because a few very active (bot) accounts all hash to the same partition. A key like `lang` would be much worse: most posts are English, so almost everything would land on one partition, and order per account would be lost.

2. **Batch size and time.** A file closes at 1,000 records or after 60 seconds. At about 45 messages/s, 1,000 records arrive in about 22 seconds, so the **size rule closes almost every batch**; the 60-second rule only closes the last, smaller batch when the producer stops (for example my first run: 8 files of 1,000, then one of 260 after 60 s). That makes about 45 × 3,600 / 1,000 ≈ **162 files per hour** while streaming live.

3. **Offsets after the file.** The lander stores offsets only after the Parquet file exists. If it crashes between the two (card D1), the file is on disk but Kafka still thinks those records were not read, so the restarted lander reads them again and writes a second copy: raw went from 8,260 to 11,943 for 2,683 new records, because the crashed batch of 1,000 was landed twice. I accept this (at-least-once) because a duplicate can be removed later, while the opposite order (offsets first, then file) could lose 1,000 records forever if the crash came in between.

4. **Duplicates.** Two rows are **the same Kafka record** when they have the same `kafka_partition` + `kafka_offset` (the D1 crash makes these). Two rows are **the same post** when they have the same `did` + `rkey`, even with different offsets (a replay makes these: in D5, 26,956 posts arrived twice). TODO: name the SQL in flow.py that removes each kind (DISTINCT on partition + offset in stage; one row per did + rkey in entities).

5. **Windows and time.** TODO: which time column (I plan to use `time_us`, the event time, not `landed_at`) and window size (10 minutes), and why. Note for this answer: landing time is wrong for replays, because my D5 replay landed 10 minutes of posts in about 3 seconds.

6. **Ten times the volume.** At 10× (about 450 messages/s) Kafka itself would be fine, but the lander would close a 1,000-record file every 2 seconds, about 1,600 small files per hour, and one Python process turning every message into a row could fall behind. The flow would also slow down reading thousands of small files. I would raise the batch size (for example 10,000 records), run 3 landers in the same consumer group (one per partition), and compact old raw files into larger ones.

## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | raw goes up by the new records plus 1,000 extra, unique Kafka records only by the new records | Stopped the lander, ran `python producer.py` for 2,683 messages, ran `python lander.py crash` (one file, then exit before storing offsets), then `python lander.py` | raw 8,260 → 11,943; malformed 0 → 0; unique records 8,260 → 10,943; unique did+rkey 8,142 → 10,744 | The crash happened after the file was written but before offsets were stored, so the restarted lander read the same 1,000 records again (at-least-once). The copy has the same partition + offset, so DISTINCT in the flow removes it and the flow stays idempotent. |
| D2 Poison message | raw +1 and malformed +1; the lander keeps running; unique records and posts unchanged | `docker exec -it redpanda-0 rpk topic produce bluesky_posts -k bad`, typed `this is not JSON`, waited for the next Landed line, counted | raw 11,943 → 11,944; malformed 0 → 1; unique records 10,943 → 10,943; unique did+rkey 10,744 → 10,744 | `to_row()` caught the JSON error and landed the text as `malformed` (land everything, decide later). The lander did not crash, so the partition was not blocked by a poison message that it would read again after every restart. |
| D5 Send it again | raw and unique Kafka records go up by everything I resend, but unique did+rkey goes up much less, because resent posts are the same posts | Found max(`time_us`) minus 2 minutes (cursor `1794489977064640`), then ran `python producer.py 1794489977064640`. By accident I ran it twice, in two terminals (30,622 + 32,276 messages), so the same 10 minutes of posts were sent twice. Waited 60 s, counted | raw 11,944 → 74,842; malformed 1 → 1; unique records 10,943 → 73,841; unique did+rkey 10,744 → 39,517 | Resent posts become new Kafka records with new offsets, so DISTINCT on partition + offset cannot remove them; they are the same post (same did + rkey). 26,956 posts arrived exactly twice. The flow removes them by keeping one row per did + rkey: the second kind of duplicate. Raw grew by 62,898 = 30,622 + 32,276, which is how I found the double run. |

## The four numbers

From the flow log on the full data (screenshot in `docs/`):

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | TODO | TODO |
| Malformed (kept in raw) | TODO | TODO |
| Unique Kafka records | TODO | TODO |
| Unique posts (did + rkey) | TODO | TODO |

## Challenges / Obstacles

- **Fast source.** Jetstream sends about 45 posts/s even with the posts-only filter, so the topic grows quickly. I log one line every 1,000 messages instead of every message.
- **Missing fields.** Deleted posts have no `record`, account events have no `commit`, and 17% of posts have no `langs`. `to_row()` reads everything with `.get()`, so missing fields become NULL instead of crashing the lander.
- **Uneven partitions.** Keying by `did` put 4,529 of my first 8,260 messages on one partition, because of a few very active accounts. I kept the key because order per account matters more than perfect balance.
- **Redpanda Console showed wrong partition counts** (1,758 on every partition). I saved the real counts from `rpk topic describe` in `docs/partitions.txt`.
- **Replay bursts and an accidental double run.** A cursor replay sends minutes of posts in seconds (31,000 in about 3 s). I also ran the replay twice by mistake; I found it by comparing raw growth (62,898) with the producer logs (30,622 + 32,276) and with the topic high watermarks (73,842 in total).
- TODO: anything hard in the flow and analysis.

## Analysis

TODO: after flow.py. Research question: which languages reply the most? Share of replies vs. original posts for the top 5 languages, in 10-minute windows. Add what the data shows, one surprising finding and one limitation.

## Plot / Visualization

TODO: after flow.py.

![TODO chart title](charts/TODO.png)

## Reflection

TODO: about 150 words, in your own words, first person. What did you learn about streaming systems that you did not know before Thu 22 Oct? What would you do differently if you started again?

## Acknowledgements

- **Reused code:** the WebSocket connection code in `producer.py` is based on `probe.py` from the kit. `lander.py` follows the class `quakes_lander.py` and the Session 2 worksheet (Appendix A1 and A3). `four_numbers.py` is copied from the Session 2 worksheet (Appendix A5).
- **AI assistance:** I used Claude (Anthropic) to help me catch up after missing the setup sessions: it walked me through the setup, wrote first versions of `producer.py` and `lander.py`, helped me fill `SOURCE_CARD.md` with my probe numbers, helped me investigate the D5 duplicates, and drafted parts of this README. I checked the code by running it: the lander's raw count matched the topic's high watermarks (8,260 and later 73,842), and every chaos card gave the numbers I predicted. I edited the README so it describes what I actually did.
- TODO: classmates or the instructor, if anyone helped.