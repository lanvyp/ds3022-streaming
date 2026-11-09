# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:**
**Date:**

## 1. The source
- Source and URL:
- Method (WebSocket push, REST polling or REST history pull):
- Account, key or User-Agent needed:
- Limits I know about (rate limits, paging, reconnects):

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json

```

## 3. Volume maths
- Measured rate (from the probe):            per
- Live capture needed for 50,000:
- History / backfill available? How many days?
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov):

## 4. Topic design
- Topic name:                          (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key:
- Why this key (ordering, spread across partitions):

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander)
- What makes two records **the same thing** (the same trade, post, aircraft position)?
- If the same thing can arrive in several versions, which one do I keep?

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join:

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [ ] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [ ] The rate in section 3 is measured, not guessed
- [ ] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [ ] It needs no API key or account that takes days to get
- [ ] My key has a reason that fits my question
- [ ] I named both kinds of duplicates for my source
- [ ] My draft question can be answered with a time window, a comparison, an anomaly or a join
