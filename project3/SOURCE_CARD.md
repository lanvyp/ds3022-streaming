# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:** Lanvy Phan
**Date:** Mon 09 Nov 2026 (probed late, see PROGRESS.md)

## 1. The source
- Source and URL: Bluesky posts through Jetstream, `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: No account or key. I send a User-Agent anyway.
- Limits I know about (rate limits, paging, reconnects): No rate limit, but it is fast (about 45 messages/s), so the topic grows quickly. The connection can drop, so the producer reconnects after 5 s and uses `?cursor=<time_us>` to continue where it stopped. Jetstream only keeps a limited replay window.

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "did": "did:plc:aq6ucuguuc67yqwu4x4eipro",
  "time_us": 1794231997361613,
  "cursor": 26991429372,
  "kind": "commit",
  "commit": {
    "rev": "3mzv2shdvfa2v",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3mzv2sgtz7s26",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-09T13:46:36.580Z",
      "langs": ["en"],
      "reply": {
        "parent": {"cid": "bafyrei...", "uri": "at://did:plc:jiadtdemeydpyr3utichqac7/app.bsky.feed.post/3mzuml4mid226"},
        "root":   {"cid": "bafyrei...", "uri": "at://did:plc:jiadtdemeydpyr3utichqac7/app.bsky.feed.post/3mzuml4mid226"}
      },
      "text": "Did no one offer them a cigarette and a blindfold"
    }
  }
}
```

## 3. Volume maths
- Measured rate (from the probe): 45.12 messages per second (2,708 messages in 60 s, 0 not JSON)
- Live capture needed for 50,000: about 1,108 s (18 min)
- History / backfill available? How many days? A limited replay with `?cursor=<time_us>` (a short window, hours, not days). I do not need it for volume.
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): run producer and lander together for about 30 to 45 minutes on Mon 09 Nov, until raw ≥ 60,000 (margin over 50,000).

## 4. Topic design
- Topic name: `bluesky_posts`                          (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: `did` (the account id)
- Why this key (ordering, spread across partitions): All events of one account go to the same partition, in order, so a post's `create` always comes before its `delete`. There are many thousands of accounts, so the posts spread evenly over the 3 partitions. A key like `lang` would put most posts (English) on one partition.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) The same `kafka_partition` + `kafka_offset`. This happens when the lander crashes after writing a file but before storing offsets (card D1).
- What makes two records **the same thing** (the same trade, post, aircraft position)? The same `did` + `commit.rkey` (one post of one account). This happens when Jetstream sends the same post again, for example after a reconnect with `?cursor=` (card D5).
- If the same thing can arrive in several versions, which one do I keep? A post can be `create`d and later `delete`d (same did + rkey). For my analysis I keep the `create` events (they have the text and language). Deletes (about 5% in the probe) have no record.

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: Which languages reply the most? Compare the share of replies vs. original posts for the top 5 languages, in 10-minute windows (in the probe, 40% of posts were replies and 83% had a language).

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join