# Project 3 checkpoints

Tick them off in your own copy. Add an entry to **PROGRESS.md** (done / next / blocked) at the end of every session and every time you stop working.

## Session 1 · Thu 05 Nov

### Checkpoint A: source and topic design
- [x] `git remote -v` shows **your** `ds3022-streaming` repository; the kit is unzipped into `project3/` and pushed
- [x] Probes run for at least two sources; one source chosen
- [x] `SOURCE_CARD.md` complete with **measured** numbers (rate, volume plan, key, duplicate identity, draft question)
- [x] Self-check in section 7 of the card: every box ticked
- [x] Repository private with the instructor added as collaborator (or kept public, your choice)

### Checkpoint B: producer
- [x] Class scripts copied into `project3/` and committed before you change them
- [x] Topic created with `rpk topic create <topic> -p 3 -r 3` **before** the producer runs
- [x] Every message wrapped in the same envelope (`{"action": ..., "data": ...}`), key as on the card
- [x] At least 1,000 messages in the topic, spread over 3 partitions; Console screenshot in `docs/`
- [x] The producer survives a dropped connection (pause, reconnect) and logs what it skips

## Session 2 · Mon 09 Nov

### Checkpoint C: lander writes raw Parquet
- [x] `to_row()` keeps partition, offset, key, `action`, `landed_at`, `raw` + your own columns
- [x] Batches of 1000 records or 60 s, `.tmp` then rename, offsets stored only after the file exists
- [x] Row count in `data/raw/` matches the messages in the topic

### Checkpoint D: chaos round (steps in [CHAOS_CARDS.md](CHAOS_CARDS.md))
- [x] Card D1 (crash between file and offsets) with numbers before and after
- [x] Card D2 (poison message) with numbers before and after
- [x] At least one of D3, D4, D5
- [x] "Chaos round" table in README.md

## Session 3 · Thu 12 Nov

### Checkpoint E: Prefect flow
- [x] **50,000 raw records landed** (hard target at the start of the session) (reached during Session 3, not at the start: 74,842)
- [x] Tasks: load raw → stage → entities → marts → charts
- [x] The four numbers logged; two runs in a row give the same numbers
- [x] `python kit/make_sample.py`, then `python flow.py data/sample` runs
- [x] Screenshots in `docs/`: flow log with the four numbers, Prefect dashboard

### Checkpoint F (part 1): insight
- [x] Research question, query, first chart (insight sprint)
- [x] Chart read correctly by a classmate without your help (chart swap), or fixed until it is

## Session 4 · Mon 16 Nov

### Checkpoint F (part 2): README and submission
- [x] README complete: every heading, no `TODO` or `(replace` left
- [x] README peer test done; the one place your reader got stuck is fixed
- [x] `python kit/check_project.py` prints `READY`
- [x] `RUBRIC.md` Self column filled; everything committed and pushed
- [x] `git tag dp3-submit` and `git push origin dp3-submit`, **last**
- [x] Repository link + rubric on Canvas by **23:59**