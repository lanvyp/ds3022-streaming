# Data Project 3: Stream Ingestion (brief)

**DS 3022, Fall 2026, UVA in Valencia** · 20% of the course grade · individual project
**Project 3 sessions:** Session 1 (Thu 05 Nov), Session 2 (Mon 09 Nov), Session 3 (Thu 12 Nov), Session 4 (Mon 16 Nov)
**Due:** Monday 16 November 2026, 23:59 · **no interview**: you defend your design in writing, in your README

## The task

On Mon 02 Nov we built one streaming pipeline together, on earthquakes. Now you build the same pipeline **on your own source**:

```
source ──► producer.py ──► Kafka topic ──► lander.py ──► data/raw/*.parquet ──► flow.py (Prefect + DuckDB) ──► charts/
```

You start from the class scripts (`quakes_producer.py`, `quakes_lander.py`, `quakes_flow.py`): copy them into this folder and adapt them. [ADAPTING.md](ADAPTING.md) lists exactly what changes and what stays.

## Where your work lives

Everything goes into the `ds3022-streaming` repository you created on Thu 22 Oct, in a new `project3/` folder (this kit). Same folder, same cluster, same virtual environment as on Mon 02 Nov.

```
ds3022-streaming/
  producer_cities.py  consumer_cities.py            (Thu 22 Oct)
  quakes_producer.py  quakes_lander.py  quakes_flow.py   (Mon 02 Nov: the worked example)
  project3/                                         (Data Project 3: this kit)
    BRIEF.md  ADAPTING.md  SOURCES.md  CHECKPOINTS.md  CHAOS_CARDS.md  RUBRIC.md
    SOURCE_CARD.md  PROGRESS.md  README.md           <- you fill these
    probe.py  kit/                                   <- tools, do not edit kit/
    producer.py  lander.py  flow.py                  <- you write these
    data/raw/      (not committed)   data/sample/  (committed, at most 5 MB)
    charts/  docs/                                   (committed)
```

Run every project command **from the `project3` folder** (`cd project3`). Keep the cluster running from the `ds3022-streaming` folder as on Mon 02 Nov.

## Minimum requirements

1. **Kafka in the path.** Every record passes through a topic on your 3-broker cluster, created with `-p 3 -r 3`. A pipeline without Kafka is capped at 60/100.
2. **At least 50,000 raw records landed** in `data/raw/*.parquet`. History backfill is allowed (and needed for slow sources). Target: reached by the start of Session 3 (Thu 12 Nov).
3. **The four numbers**, logged by your flow and reported in your README: raw records landed, malformed, unique Kafka records, unique entities (trades, posts, aircraft positions, ...).
4. **Robustness**: the lander survives a restart and a malformed message (checkpoint D, the chaos round: [CHAOS_CARDS.md](CHAOS_CARDS.md)).
5. **Prefect** runs the processing: load raw → stage → entities → marts → charts. The flow is idempotent (two runs, same numbers) and runs on your sample with `python flow.py data/sample`.
6. **Analysis**: one research question answered with a non-trivial query (time windows, a comparison, an anomaly, or a join) and at least one clear chart.
7. **README.md** (the template in this folder) complete, including Design decisions, Chaos round, Reflection and Acknowledgements.

## Choosing a source

Pick from the menu in [SOURCES.md](SOURCES.md) or propose your own. Taste them first with `python probe.py <source>` (60 seconds, no Kafka needed). Your own idea is welcome **if its probe works in class** in Session 1 (Thu 05 Nov); otherwise choose from the menu. You check your choice yourself with the self-check at the end of `SOURCE_CARD.md`.

## Working alone, together

This is an **individual** project: your own repository, code, research question, chart and README. Collaboration is encouraged where it helps learning:

- You **may** choose the same source as a classmate, debug together, share tips and links in the class tracker, and explain things to each other.
- You **may not** share code files, queries, charts or README text, or submit work you cannot explain in writing.
- Name everyone who helped you, and any AI assistance (what for, and how you checked it), in **Acknowledgements**. Naming help never costs points; hiding it does.

If your laptop cannot run Docker, tell the instructor in Session 1 (Thu 05 Nov): you will pair with a classmate for the cluster, and still write your own code and README.

## How to submit (by 23:59, Monday 16 November)

1. `python kit/check_project.py` prints `READY`.
2. Fill the **Self** column of `RUBRIC.md`, commit and push.
3. Tag the version you submit, last of all:
   ```
   git tag dp3-submit
   git push origin dp3-submit
   ```
4. On Canvas: the link to your `ds3022-streaming` repository + your completed rubric.

If your repository is private, add the instructor as a collaborator (Settings → Collaborators). The grader checks out the `dp3-submit` tag, follows your README, runs `python flow.py data/sample`, and reads your evidence in `docs/`. Late work is penalised under syllabus §3.4.

## Last year's projects (for ideas, not as a model of the rules)

Prof. Magee's repository [uvasds-systems/ds3022-data-project-3](https://github.com/uvasds-systems/ds3022-data-project-3), folder `project-3-examples/`, has 35 READMEs from Fall 2025. They are good for source ideas and for seeing what went wrong (read `earthquake`, `coinbase-streaming`, `team-td`). The rules were different: many did not use Kafka at all. This year Kafka is required.
