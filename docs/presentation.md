# Presentation and demo plan (15 minutes, recorded)

The handout asks for a 15-minute recorded group presentation in which every member speaks, plus the
slides. Peer reviewers grade slide design and coherence, clarity, and style (pace, time limit), and
the report rubric grades the demonstration. This plan keeps one story: *a market-data pipeline, and
whether its claims of speed, durability and exactly-once results hold up when we measure them.*

Speakers below follow the proposal's work division (Kafka: Arya; Spark: Rawad; output and
measurement: Mark). Swap freely; just keep each person's part in one block.

## The deck

`presentation/group6-final-presentation.pptx` (17 slides, speaker notes on every slide) is built by
`presentation/build_deck.py` from `results/figures/` and the live dashboard screenshot in
`presentation/assets/`. `presentation/speaker-script.txt` has the same notes for printing. Rebuild after
changing a figure: `uv run --no-project --with python-pptx --with pillow python presentation/build_deck.py`.

## Running order (about 12.7 minutes of speaking and demo, 2 minutes of slack)

| Slides | Speaker | Content |
|---|---|---|
| 1-4 | Arya | Title, problem and the three research questions, the pipeline, Kafka (partitions, replication, acks). |
| 5-7 | Rawad | Spark (micro-batch, windows, watermark, checkpoint), integration (exactly-once = replay + idempotent sink), correctness first (oracle and verifier). |
| 8 | Mark | **Live demo** (about 2.5 minutes, script in the slide's notes): dashboard, an alert, Spark crash, broker crash, PASS. |
| 9-10 | Rawad | The two reproductions: E1a (Kreps et al. Fig. 4) and E1b (Armbrust et al. Fig. 7). |
| 11-12, 14 | Mark | RQ1 (E1c), RQ2 broker crash (E2), RQ3 (E4). |
| 13 | Rawad | RQ2 Spark crash (E3). |
| 15 | Arya | Challenges and the limits of one machine. |
| 16 | Mark | The live Coinbase run (230 of 230 bars). |
| 17 | all | Conclusions and reproducibility. |

Swap speakers freely, but keep each person's slides in blocks and make sure everyone speaks.

Rehearse with a timer: the rubric penalizes going over.

## Demo script (about 3 minutes)

Before recording (on the demo machine):

```bash
make reset && make up        # wait about 60 s; open http://localhost:3000
```

Then, live:

1. **Dashboard** (30 s). Bars arriving for 100 symbols at 30x speed; pick a symbol; point at the
   candlestick and the VWAP line; the health row (lag 0, seconds since last bar).
2. **A shock and its alert** (30 s). After about 80 s the first alerts appear: open the alerts table,
   pick one, select that symbol, show the jump in its candlestick.
3. **Spark crash** (45 s).
   ```bash
   docker kill spark-bars
   docker compose up -d spark-bars
   ```
   The dashboard's "seconds since last bar" climbs, then bars resume (E3 measured how long).
4. **Broker crash** (45 s).
   ```bash
   docker kill kafka-2
   ```
   Bars keep coming: RF 3 and min ISR 2 tolerate one broker. Bring it back:
   `docker compose up -d kafka-2`.
5. **Proof of correctness** (30 s). After the generator finishes (or on a shorter run):
   ```bash
   uv run python -m tradegen --mode virtual --speed max --minutes 480 --seed 42 --shock-rate-per-min 0.2 --dump-csv runs/demo.csv
   uv run python -m verify.verify --trades runs/demo.csv --wait 120
   ```
   (`PYTHONPATH=.:generator` before the first command.) It prints `PASS: bars N/N matched ...`:
   every bar and alert identical to the oracle despite both crashes. For a shorter recording, use
   `scripts/live_check.sh` with `KILL_SPARK_AT=15 KILL_BROKER_AT=35` and show its final PASS line.

Fallbacks: if Docker is slow on the recording machine, record the demo separately and splice it in;
if the Coinbase feed is part of the demo, `make demo-live` (or `make demo-offline` without network).

## Slide checklist

- One idea per slide; a figure from `results/figures/` on every results slide, with the claim as the
  slide title ("acks=1 lost acknowledged trades in k of 6 kills"), not "E2 results".
- Same colors as the figures; no more than three series per chart.
- Show the architecture diagram once and come back to it (highlight the part being discussed).
- Cite the papers on the slides where their results are reproduced (Kreps et al. Fig. 4, Armbrust
  et al. Fig. 7).
