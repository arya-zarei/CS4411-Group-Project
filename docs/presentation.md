# Presentation and demo plan (15 minutes, recorded)

The handout asks for a 15-minute recorded group presentation in which every member speaks, plus the
slides. Peer reviewers grade slide design and coherence, clarity, and style (pace, time limit), and
the report rubric grades the demonstration. This plan keeps one story: *a market-data pipeline, and
whether its claims of speed, durability and exactly-once results hold up when we measure them.*

Speakers below follow the proposal's work division (Kafka: Arya; Spark: Rawad; output and
measurement: Mark). Swap freely; just keep each person's part in one block.

## Running order

| Time | Slide(s) | Speaker | Content |
|---|---|---|---|
| 0:00-1:30 | 1-2 | Arya | Title, problem: trades in, 1-minute bars and alerts out within seconds. Three research questions (RQ1 latency vs throughput, RQ2 crashes, RQ3 cost of exactly-once). |
| 1:30-4:00 | 3-5 | Arya | Kafka: partitions and ordering by key, replication and ISR, acks, KRaft, consumer groups vs Spark's own offsets. One diagram: `docs/architecture.md` section 1. |
| 4:00-6:30 | 6-8 | Rawad | Spark: micro-batch vs continuous, windows and watermark, state store, checkpoint. Integration: partitions become tasks, offsets in the checkpoint, exactly-once = replay + idempotent sink (sequence diagram). |
| 6:30-9:30 | live | Mark | **Demo** (script below): dashboard, a shock and its alert, Spark crash, broker crash, `make verify` PASS. |
| 9:30-12:30 | 9-12 | Rawad + Mark | Results, one figure per question: E1a and E1b (the two reproductions, shape vs the papers), E1c (RQ1), E2 and E3 (RQ2), E4 (RQ3). `results/figures/`. |
| 12:30-14:00 | 13 | Arya | Challenges and limitations (one machine, watermark trade-off, continuous mode's core-per-partition limit, live feed). |
| 14:00-15:00 | 14 | all | Conclusions and reproducibility: one `docker compose up`, `make golden`, `make bench-all`, public repository. |

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
