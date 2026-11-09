# TODO: your project title

**Author:** TODO
**DS 3022 Data Project 3, Fall 2026**

> There is no interview for this project: this README is where you explain and defend your pipeline. Fill in the title, your name and every italic placeholder, then delete this note. Keep the headings: `kit/check_project.py` looks for them.

## Data source

_(replace: what source, which URL or stream, pull or push, what one message contains. Mention any account, key or User-Agent needed. If you replay an archive rather than stream live, say so.)_

## How to run

_(replace: the exact commands, in order, from a fresh clone. A classmate will follow them in Session 4 (Mon 16 Nov) without asking you. Include: virtual environment and `pip install`, starting the cluster, creating the topic, running the producer and lander, running the flow on the full data and on the sample:)_

```bash
cd project3
python flow.py data/sample      # runs without Kafka, on the committed sample
```

## Architecture

_(replace: a diagram (an image in docs/ or a text diagram) of producer → topic → lander → raw Parquet → flow tasks → tables → charts, with the names of your files, topic, tables and charts.)_

## Design decisions

Answer each in 2-4 sentences, in your own words, with your own numbers.

1. **Key and partitions.** _(replace: which key, why, and what it guarantees about order. What would go wrong with another key?)_
2. **Batch size and time.** _(replace: your batch size and time limit. With your source's rate, which rule closes most batches? How many files per hour does that make?)_
3. **Offsets after the file.** _(replace: what happens if the lander crashes after writing a file but before storing offsets, and why you accept that.)_
4. **Duplicates.** _(replace: what makes two records the same Kafka record, and what makes them the same thing. Which SQL removes each kind?)_
5. **Windows and time.** _(replace: which time column your analysis uses (event time or landing time), which window size, and why.)_
6. **Ten times the volume.** _(replace: what in your pipeline would break or slow down first at 10x your rate, and what you would change.)_

## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | TODO | TODO | TODO | TODO |
| D2 Poison message | TODO | TODO | TODO | TODO |
| TODO (D3, D4 or D5) | TODO | TODO | TODO | TODO |

## The four numbers

From the flow log on the full data (screenshot in `docs/`):

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | TODO | TODO |
| Malformed (kept in raw) | TODO | TODO |
| Unique Kafka records | TODO | TODO |
| Unique TODO (your entity) | TODO | TODO |

## Challenges / Obstacles

_(replace: what this source made hard in gathering, processing and analysis, and how you worked through it. What methods and tools did you use?)_

## Analysis

_(replace: one clear paragraph. Your research question, what the data shows, one finding that surprised you or is not obvious, and one limitation of your data or method.)_

## Plot / Visualization

_(replace: at least one chart, saved in charts/ and shown here. Give it a title and labelled axes with units.)_

![TODO chart title](charts/TODO.png)

## Reflection

_(replace: about 150 words, first person. What did you learn about streaming systems that you did not know before Thu 22 Oct? What would you do differently if you started again?)_

## Acknowledgements

_(replace: everyone who helped you and how (classmates, instructor, online sources), code you reused (for example connection code from probe.py), and any AI assistance: what for, and how you checked the result. Write "None" if there is nothing to declare.)_
