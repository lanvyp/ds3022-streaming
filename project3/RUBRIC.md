---
course: DS 3022, Systems II: Advanced Data Science Systems
term: Fall 2026, UVA in Valencia
artifact: Data Project 3, Grading Rubric
project: Data Project 3, Stream Ingestion (individual)
assigned: Project 3 Session 1, Thursday 05 November 2026
due: Monday 16 November 2026, 23:59 (after Project 3 Session 4)
total: 100 points
weight: 20% of the course grade
---

# Data Project 3: Rubric

**Stream Ingestion** · brief: [BRIEF.md](BRIEF.md) · checkpoints: [CHECKPOINTS.md](CHECKPOINTS.md)

## Point structure at a glance

| Section | Points | Mostly built in |
|---|---:|---|
| §1 Ingestion: source, producer, topic | 20 | Session 1 · Thu 05 Nov (A, B) |
| §2 Landing and robustness | 20 | Session 2 · Mon 09 Nov (C, D) |
| §3 Processing flow (Prefect + DuckDB) | 20 | Session 3 · Thu 12 Nov (E) |
| §4 Analysis and visualisation | 15 | Session 3 · Thu 12 Nov (F) |
| §5 README, design justification and hygiene | **25** | Throughout; finished in Session 4 (Mon 16 Nov) |
| **Total** | **100** | |

> There is no interview for this project. You explain and defend your design **in writing** in your README, so §5 is a quarter of the grade.

Three things worth noticing before you plan your time:

1. **Kafka is required.** A pipeline where the records do not pass through a topic on your cluster is capped at **60**.
2. **Volume is evidence, not a promise.** Row 1.4 is graded from your flow log and README numbers; below 50,000 raw records it scores 0-2.
3. **The README is a quarter of the grade.** A pipeline that works but whose Design decisions and Chaos round sections are missing cannot score above 88.

## How to use this

**Before you submit,** mark the Self column `Y` (done and verified), `P` (partial) or `N` (not attempted) for every row, commit this file, and upload it to Canvas with your repository link. Partial credit is real; an honest `N` costs you that row and nothing more.

**How it is marked.** The grader checks out your `dp3-submit` tag, follows your README, runs `python flow.py data/sample`, reads the code, and checks the evidence in `docs/`. Where the Evidence column says "code", the grader reads the relevant function.

---

## §1 Ingestion: source, producer, topic (20 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 1.1 | **Producer runs** from the README commands and fills the topic | 4 | README steps; `producer.py` | | |
| 1.2 | **Topic design**: created with `-p 3 -r 3` before producing; key chosen and justified | 4 | Console screenshot (3 partitions); Design decision 1 | | |
| 1.3 | **Real-world handling**: reconnect with a pause, rate limits or paging respected, missing fields do not crash the producer | 5 | Code; Challenges section | | |
| 1.4 | **At least 50,000 raw records landed**, shown by the flow log | 5 | Flow log screenshot; four-numbers table | | |
| 1.5 | **One envelope** (`{"action", "data"}`) for every message, history and live alike | 2 | Code; sample message | | |

**Subtotal: ___ / 20**

---

## §2 Landing and robustness (20 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 2.1 | **Raw Parquet batches**: size and time rule, `.tmp` then rename, `raw` column kept | 5 | `lander.py` | | |
| 2.2 | **Offsets stored after the file exists** (at-least-once) | 4 | `lander.py` | | |
| 2.3 | **Malformed messages landed**, not skipped and not crashing the lander | 3 | Code; malformed count in the flow log | | |
| 2.4 | **Chaos round**: D1 and D2 plus one more card, each with a prediction and numbers before and after | 6 | README Chaos round table | | |
| 2.5 | **Useful logging** (progress, skips, reconnects), no log files committed | 2 | Code; repository file list | | |

**Subtotal: ___ / 20**

---

## §3 Processing flow, Prefect + DuckDB (20 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 3.1 | **Modular Prefect flow** (load raw → stage → entities → marts → charts) that runs with one command, on full data or `data/sample` | 5 | `flow.py`; grader runs it on the sample | | |
| 3.2 | **Both kinds of duplicates removed**: re-landed Kafka records and repeated entities | 5 | Stage and entity SQL; Design decision 4 | | |
| 3.3 | **Cleaning**: nested fields parsed, types cast, malformed and test records excluded | 4 | Stage SQL | | |
| 3.4 | **Idempotent**: two runs give the same four numbers | 3 | Grader runs it twice on the sample | | |
| 3.5 | **The four numbers** logged and reported (full data and sample) | 3 | Flow log; README table | | |

**Subtotal: ___ / 20**

---

## §4 Analysis and visualisation (15 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 4.1 | **Research question** answered with a non-trivial query: time windows, a comparison, an anomaly or a join | 5 | Marts SQL; Analysis section | | |
| 4.2 | **At least one clear chart**: title, labelled axes with units, readable without explanation | 5 | `charts/`; README | | |
| 4.3 | **Analysis paragraph**: a surprising or non-obvious finding and one limitation | 5 | README Analysis section | | |

**Subtotal: ___ / 15**

---

## §5 README, design justification and hygiene (25 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 5.1 | **How to run** works from a fresh clone (peer-tested in Session 4, Mon 16 Nov) | 5 | Grader follows it | | |
| 5.2 | **Architecture** diagram with your file, topic, table and chart names | 3 | README / `docs/` | | |
| 5.3 | **Design decisions**: all six answered in your own words with your own numbers | 6 | README section | | |
| 5.4 | **Sample and numbers**: `data/sample/` committed (≤ 5 MB) and the four-numbers table filled for full data and sample | 3 | Repository; README table | | |
| 5.5 | **Reflection** (about 150 words) and **Acknowledgements** (help, reused code, AI use and how it was checked) | 3 | README sections | | |
| 5.6 | **Repository hygiene**: work in `project3/` of `ds3022-streaming`; no `data/raw/`, `*.duckdb`, `.venv/` or `.env` committed; `PROGRESS.md` kept each session; `dp3-submit` tag | 5 | `git ls-files`; `kit/check_project.py` | | |

**Subtotal: ___ / 25**

---

## Total

| Section | Score |
|---|---:|
| §1 Ingestion | ___ / 20 |
| §2 Landing and robustness | ___ / 20 |
| §3 Processing flow | ___ / 20 |
| §4 Analysis and visualisation | ___ / 15 |
| §5 README, design justification and hygiene | ___ / 25 |
| **Total** | **___ / 100** |

**Academic integrity.** This is an individual project with open collaboration (see BRIEF.md). Shared code, queries, charts or README text, or undeclared help, are handled under the UVA honor policy. Late submissions (after 23:59 on Monday 16 November 2026) are penalised under syllabus §3.4.
