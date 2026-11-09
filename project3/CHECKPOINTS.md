# Project 3 checkpoints

Tick them off in your own copy. Add an entry to **PROGRESS.md** (done / next / blocked) at the end of every session and every time you stop working.

## Session 1 · Thu 05 Nov

### Checkpoint A: source and topic design
- [ ] `git remote -v` shows **your** `ds3022-streaming` repository; the kit is unzipped into `project3/` and pushed
- [ ] Probes run for at least two sources; one source chosen
- [ ] `SOURCE_CARD.md` complete with **measured** numbers (rate, volume plan, key, duplicate identity, draft question)
- [ ] Self-check in section 7 of the card: every box ticked
- [ ] Repository private with the instructor added as collaborator (or kept public, your choice)

### Checkpoint B: producer
- [ ] Class scripts copied into `project3/` and committed before you change them
- [ ] Topic created with `rpk topic create <topic> -p 3 -r 3` **before** the producer runs
- [ ] Every message wrapped in the same envelope (`{"action": ..., "data": ...}`), key as on the card
- [ ] At least 1,000 messages in the topic, spread over 3 partitions; Console screenshot in `docs/`
- [ ] The producer survives a dropped connection (pause, reconnect) and logs what it skips

## Session 2 · Mon 09 Nov

### Checkpoint C: lander writes raw Parquet
- [ ] `to_row()` keeps partition, offset, key, `action`, `landed_at`, `raw` + your own columns
- [ ] Batches of 1000 records or 60 s, `.tmp` then rename, offsets stored only after the file exists
- [ ] Row count in `data/raw/` matches the messages in the topic

### Checkpoint D: chaos round (steps in [CHAOS_CARDS.md](CHAOS_CARDS.md))
- [ ] Card D1 (crash between file and offsets) with numbers before and after
- [ ] Card D2 (poison message) with numbers before and after
- [ ] At least one of D3, D4, D5
- [ ] "Chaos round" table in README.md

## Session 3 · Thu 12 Nov

### Checkpoint E: Prefect flow
- [ ] **50,000 raw records landed** (hard target at the start of the session)
- [ ] Tasks: load raw → stage → entities → marts → charts
- [ ] The four numbers logged; two runs in a row give the same numbers
- [ ] `python kit/make_sample.py`, then `python flow.py data/sample` runs
- [ ] Screenshots in `docs/`: flow log with the four numbers, Prefect dashboard

### Checkpoint F (part 1): insight
- [ ] Research question, query, first chart (insight sprint)
- [ ] Chart read correctly by a classmate without your help (chart swap), or fixed until it is

## Session 4 · Mon 16 Nov

### Checkpoint F (part 2): README and submission
- [ ] README complete: every heading, no `TODO` or `(replace` left
- [ ] README peer test done; the one place your reader got stuck is fixed
- [ ] `python kit/check_project.py` prints `READY`
- [ ] `RUBRIC.md` Self column filled; everything committed and pushed
- [ ] `git tag dp3-submit` and `git push origin dp3-submit`, **last**
- [ ] Repository link + rubric on Canvas by **23:59**
