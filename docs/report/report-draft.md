# Real-Time Data Pipeline Architecture with Apache Kafka and Spark Structured Streaming: A Market-Data Case Study

Group 6: Arya Zarei, Rawad Alharastani, Mark Samwaiel. COMPSCI 4411/9538.

> **Draft content for the final report.** The course requires the report as a PDF made with the
> provided Overleaf (LaTeX) template: port this text there. Numbers in section 6 come from
> `results/RESULTS.md` and `results/summary.csv`; figures from `results/figures/`. Where a sentence
> depends on a measurement, it cites the experiment, so the claim can be checked against the data.

**How this draft maps to the evaluation rubric (System Analysis, plus the reproduction criteria):**

| Rubric criterion | Section |
|---|---|
| Understanding of background and significance | 1 |
| Goals, objectives, purpose of the extension | 2 |
| Analysis of functionalities and features | 3, 4 (detail: `docs/architecture.md`) |
| Demonstration | 5, and the recorded demo (`docs/presentation.md`) |
| Experimental reproduction and extension | 6, 7 |
| Challenges faced | 8 |
| Reproducibility of case study and demonstration | 9 |
| Clarity, writing and format | whole report; consistent section titles, citations [1]-[5], page numbers in LaTeX |

## Abstract

Financial firms turn streams of trades into per-minute summaries and alerts within seconds. We built
such a pipeline from Apache Kafka 4.4 (three brokers, KRaft) and Spark 4.2 Structured Streaming
(PySpark), writing one-minute OHLC/VWAP bars and three-sigma alerts to PostgreSQL and showing them in
Grafana, all started with one `docker compose up`. A seeded generator makes the correct output known
exactly, so every run is verified bar by bar. We reproduce two published results (Kafka producer
batching, Kreps et al. Figure 4; continuous versus micro-batch latency, Armbrust et al. Figure 7) and
extend them with what neither paper measured: whether the output stays correct when a broker or the
Spark job crashes, and what exactly-once output costs. *(Add one sentence of headline results after
M3.)* Suggested: "No acknowledged trade was lost in 12 broker crashes, Spark recovered from its own
crash in about 14 s with exact output, and exactly-once output cost nothing measurable at 2,000
trades per second, while plain inserts left 300 duplicate rows after every crash."

## 1. Background and significance

Market data arrives as a continuous stream of trades, and trading desks need summaries and alerts
within seconds. The common architecture pairs a durable, partitioned log that stores the events with
a stream processor that reads the log and computes results over time windows.

**Apache Kafka** [1, 2] is the log. A topic is split into partitions; each partition is an
append-only file on a broker, written and read sequentially in batches. Kreps et al. showed that
this design gives far higher throughput than traditional message queues [1]. Kafka orders events only
within a partition, so keying trades by symbol keeps each symbol's trades in order. Each partition is
replicated to several brokers: a leader serves reads and writes, and followers that are caught up
form the in-sync replica set. Whether an acknowledged write survives a broker crash depends on the
replication factor, on how many copies the producer waits for (`acks`), and on the minimum number of
in-sync replicas [2]. Kafka 4 manages cluster metadata with its own Raft-based protocol (KRaft)
instead of ZooKeeper.

**Spark Structured Streaming** [3] is the processor. It treats a stream as an unbounded table and
runs ordinary SQL-style queries over it incrementally, usually in micro-batches, an approach that
descends from discretized streams [4]. It aggregates over event-time windows and keeps intermediate
state between batches; a watermark bounds how long it waits for late events before finalizing a
window, an idea from the Dataflow model [5]. Spark records each batch's Kafka offsets and its state in
a checkpoint; because Kafka can re-deliver from any offset, Spark re-runs exactly the unfinished
batches after a crash, and an idempotent sink then makes the result as if each event were processed
once. An experimental continuous mode lowers latency but supports only per-record operations and
delivers at least once.

**Significance.** These systems promise high throughput from batched sequential I/O, durability from
replication, and exactly-once results from replay plus checkpoints. Each promise depends on
configuration and on how the two systems are connected. We test them together, on a financial
workload, with a known correct answer.

## 2. Goals and objectives

Three research questions:

1. **RQ1.** How do the number of partitions, the trigger interval and the processing mode trade
   latency against throughput?
2. **RQ2.** Which Kafka settings prevent lost or duplicated data when a broker crashes, and how long
   does Spark take to recover after its own crash?
3. **RQ3.** What does exactly-once output cost compared with at-least-once output?

Objectives: (1) an architectural analysis of Kafka, Spark and their integration; (2) a case-study
pipeline started with one command; (3) reproduction of Kreps et al. Fig. 4 and Armbrust et al.
Fig. 7, comparing the shape of each curve, not absolute numbers; (4) an **extension** neither paper
evaluated: correctness under crashes and the cost of exactly-once output; (5) a reproducible public
repository with pinned versions and scripted experiments.

## 3. Architecture analysis

*(Port `docs/architecture.md` sections 1 to 4: the pipeline diagram, the per-batch sequence diagram,
and the Kafka, Spark and integration tables. Keep the two diagrams; condense the tables to prose
where the template's page budget requires.)*

Key points to keep:
- Kafka: partitions and ordering by key; batching, sequential I/O and the page cache [1]; retention;
  replication, ISR, `acks`, `min.insync.replicas`, unclean leader election off [2]; the idempotent
  producer; KRaft; consumer groups and the three delivery guarantees; security features (TLS, SASL,
  ACLs) described but not measured.
- Spark: the unbounded-table model and incremental planning [3]; micro-batch triggers versus
  continuous epochs; event-time windows and watermarks [5]; the state store; the checkpoint's
  write-ahead offsets and commit log; output modes; `foreachBatch`.
- Integration: one task per partition; offsets in Spark's checkpoint rather than a consumer group,
  and why; exactly-once as replay plus an idempotent, transactional sink.

## 4. Functionalities and features exercised

| Feature | How the pipeline uses it | Demonstrated by |
|---|---|---|
| Partitioned, keyed log | 6 partitions, key = symbol, per-symbol order | M1 ack-log check; E1c partitions |
| Replication and acks | RF 3, min ISR 2, acks=all, idempotent producer | E2 |
| Event-time windows + watermark | 1-minute OHLC/VWAP bars, 10 s watermark, append mode | `make golden` (4,000 bars exact) |
| Stateful lookback | Alert: log return more than 3 standard deviations from the previous 30 bars | 23 alerts exact; 20/20 shocks detected |
| Checkpoint recovery | Restart from offsets and state after SIGKILL | E3 |
| Exactly-once output | One transaction per batch, upsert on (symbol, minute) | E4 |
| Continuous processing | Kafka-to-Kafka pass-through | E1b |
| Monitoring | StreamingQueryListener to `query_progress`, Grafana | Dashboard |

## 5. Case study: the market-data pipeline

Describe: the generator (seeded random-walk prices for 100 symbols with a busiest-to-quietest rate
ratio of about 10, log-normal sizes, permanent price shocks of 1.5% to 3% at known times after a
30-bar warm-up; virtual, realtime and replay modes; per-trade acknowledgement log); the Spark job
(parsing with an explicit schema, `min_by`/`max_by` ordered by (time, trade id) for deterministic
open and close, VWAP via `try_divide`); the alert rule (decision D1, confirmed by the group: bar-to-bar log return of the
close; mean and sample standard deviation of the previous 30 changes; alert when |change - mean| >
3 sd); the sink (one PostgreSQL transaction per batch; alert history read back from the table);
Grafana; and the live Coinbase producer with capture and replay. Screenshot: the dashboard
(`docs/presentation.md` demo, step 1).

**Verification as a first-class component.** The oracle (`tools/gen_sample_dataset.py`, copied in
`verify/reference.py`) computes the exact expected bars and alerts; the verifier compares every bar
(open, high, low, close exact; VWAP within 1e-6) and alert, and a Kafka-layer verifier compares the
producer's acknowledgement log with the topic. This turns "the output looks right" into a pass or
fail for every run, including every crash experiment.

## 6. Experiments and results

**Setup.** One machine (`results/MACHINE.md`): 4 CPUs, 16 GB, Docker; every experiment alone on it;
three runs per setting; latency reported as the median over runs of each run's p50, p95 and p99.
Sustained throughput: the highest input rate at which Spark keeps up over a five-minute run
(decision D5). A warm-up period is discarded from latency measurements (measured: JVM warm-up
inflates the first seconds about twentyfold).

Medians over three runs (six kills for E2 acks=1), with the range in brackets. Every number:
`results/RESULTS.md`, `results/summary.csv`.

### 6.1 E1a, producer batching (reproduction a, Kreps et al. Fig. 4)

| batch size (messages) | 1 | 10 | 50 | 100 | 200 |
|---|---|---|---|---|---|
| records per second | 19,798 | 154,909 | 525,376 | 739,153 | 848,033 |

Throughput rises with batching, by 26 times from 1 to 50 messages per batch (the paper: about 8
times, 50,000 to 400,000 messages per second). Figure `e1a_producer_batching.png`. With acks=0, as in
the paper, the tool's latency column is not meaningful and is not reported.

### 6.2 E1b, continuous versus micro-batch (reproduction b, Armbrust et al. Fig. 7)

Continuous mode (4 partitions): p50 latency 43 to 83 ms from 5,000 to 40,000 records/s (p99 0.3 to
0.9 s), then 1.8 s p50 and 6.9 s p99 at 80,000 records/s, where its four tasks saturate. Micro-batch
(100 ms trigger) sustained the highest rate we could generate, **at least 100,000 records/s**, at p50
0.27 to 0.62 s. Figure `e1b_continuous_vs_microbatch.png`.

### 6.3 E1c, partitions and trigger interval (RQ1)

Sustained throughput (five-minute runs): 1 partition 75,000 records/s (one of three runs passed
90,000); 3 and 6 partitions, and every trigger from 500 ms to 5 s, at least 90,000 records/s, the most
our generator could supply beside Spark on 4 cores; the 100 ms trigger reached its limit in one run of
three (75,000).

Bar latency p50 (window end + watermark to row written):

| | 30,000 records/s | 60,000 records/s |
|---|---|---|
| 1, 3, 6 partitions (trigger 1 s; machine B) | 2.5, 2.4, 2.5 s | 2.4, 2.1, 3.5 s |
| trigger 100 ms, 500 ms, 1 s, 5 s (6 partitions; machine A) | 2.2, **1.7**, 2.9, 7.2 s | 6.4, 5.7, **4.9**, 8.7 s |

The best trigger depends on load. At 30,000 records/s a 500 ms trigger is fastest. At 60,000 a batch
takes about 2 s whatever the trigger (batch duration p50 2.4 s at 100 ms, 2.1 s at 500 ms, 2.0 s at
1 s), so a shorter trigger no longer starts batches sooner and only adds per-batch overhead: 1 s is
fastest. A 5 s trigger always waits longest. Partitions matter little below saturation; at 60,000,
6 partitions on 4 cores cost more than 3 (more tasks and state-store work per batch). The two sweeps
ran on different CPUs and are compared only within a sweep (`results/MACHINE.md`). Figures
`e1c_sustained_throughput.png`, `e1c_bar_latency_30k.png`, `e1c_bar_latency_60k.png`.

### 6.4 E2, broker crash (RQ2)

No acknowledged trade was lost in any of 12 leader kills: acks=1 (6 kills), acks=all (3) and acks=all
without idempotence (3). Writes stalled for 9.3 s [8.1 to 9.5] with acks=all, **on every partition**
(the longest gap with no acknowledgement anywhere was 9.1 s), because the dead broker sits in every
partition's in-sync replica set until the controller fences it (`broker.session.timeout.ms` = 9 s).
With acks=1 only the partitions the dead broker led stalled, for 10.3 s [9.3 to 12.4]; the others
kept acknowledging (longest gap anywhere 0.08 s). No duplicates appeared in these runs, also without
idempotence (an earlier fault check did produce one, section 8). In one of the three runs without
idempotence, 33 bars of one minute were wrong although Kafka held every trade: a 3.8 s difference in
when partitions resumed, multiplied by the experiment's 10x time compression, exceeded the 10 s
watermark, and Spark dropped those trades as late (`docs/decisions.md`, 2026-11-12). The dry run, at
a different rate, did lose 2 acknowledged trades with acks=1: loss is possible but rare.
Figure `e2_broker_crash.png`.

### 6.5 E3, Spark crash (RQ2)

After a SIGKILL the first new bar was written 12.8 s [12.6 to 12.9] later in deterministic replay and
14.6 s [14.2 to 14.9] later on a live stream; output was exact in all six runs. Recovery time is
almost entirely JVM and Spark start-up (restart to first completed batch: 12.8 to 13.4 s).
Figure `e3_spark_recovery.png`.

### 6.6 E4, exactly-once cost (RQ3)

Crashing Spark after the database commit but before Spark's own commit left **300 duplicate bar rows
in every run with plain inserts and none with upserts** (3 runs each). Crash-free at 2,000 records/s,
the cost of upserting was not measurable: batch duration p50 460 ms (insert) versus 451 ms (upsert);
bar latency p50 1.7 s [1.5 to 1.9] versus 2.1 s [1.7 to 2.3], overlapping ranges, p99 2.45 s versus
2.43 s; both kept up. Figure `e4_exactly_once.png`.

## 7. Reproduction and extension: discussion

**Reproduction (a)** holds in shape: batching raises producer throughput by more than an order of
magnitude, as Kreps et al. found. Our unbatched rate is lower than theirs (each one-record request is
a round trip through a single shared 4-core machine) and our batched rates are higher (a modern
producer and CPU); the paper used two dedicated 8-core machines, and batch sizes here are approximated
in bytes.

**Reproduction (b)** holds in shape: continuous processing gives far lower latency (tens of
milliseconds against hundreds for micro-batch) up to a saturation point, beyond which its latency
climbs steeply, while micro-batch sustains a higher input rate. Armbrust et al. reported about 1 ms
continuous latency; ours is tens of milliseconds because it includes the producer's batching
(`linger.ms` = 5), Kafka writes with acks=all on three replicas, and the sink's own producer. The
4-partition cap on 4 cores (spike S3) is the constraint the paper describes.

**Extension.** RQ2: with RF 3 and min ISR 2, no acknowledged trade was lost in 12 leader kills, and
the price of acks=all is a stall of about 9 s on every partition until the dead broker is fenced;
acks=1 limits the stall to the dead broker's partitions but can lose acknowledged writes (seen once,
in a dry run). Spark recovers from its own crash in about 13 to 15 s with exact output. RQ3: plain
inserts are wrong after a crash (300 duplicate rows every time); upserts are exact, at no measurable
cost at 2,000 records/s.

## 8. Challenges faced

Real problems met while building and measuring (details and dates: `docs/decisions.md`):

1. **Determinism versus late data.** An unpaced replay squeezes 40 virtual minutes into seconds, so
   millisecond skew between partitions looked like more than the 10 s watermark and trades would have
   been dropped as late. Solution: preload the topic before Spark starts (deterministic replay).
2. **Duplicates from producer retries.** With acks=all but idempotence off, killing a leader made the
   producer write one trade twice; the verifier caught the wrong bar. Solution: idempotence on by
   default; E2 measures the difference.
3. **Measuring a write stall.** A stall across all partitions hid the acks=1 behaviour (only the dead
   leader's partitions stall). Solution: per-partition stall from timestamped acknowledgements.
4. **JVM warm-up** made early latencies about twenty times worse; latency now excludes a warm-up
   period.
5. **Continuous mode needs a core per partition**: with 6 partitions on 4 cores, two partitions were
   silently never processed. Continuous runs use 4 partitions.
6. **Crashing exactly between two commits.** SIGKILL to PID 1 inside a container is ignored; the
   fault hook needed an init process and a kill of the driver JVM from inside foreachBatch.
7. **Floating-point equality.** Fractional (crypto) sizes summed in a different order differ in the
   last bits; the verifier uses a relative 1e-9 tolerance for volume only.
8. **Pinned versions and tooling.** Verifying that Kafka 4.4, Spark 4.2, PostgreSQL 18 and Grafana 13
   exist, building behind a TLS-inspecting proxy, and Docker Hub rate limits.
9. **Live data access.** The Coinbase feed could not be reached from the cloud development
   environment; the adapter was tested against a protocol-faithful fake server and from captures.

## 9. Reproducibility

Public repository; `docker compose up` starts everything; image tags and Python packages pinned;
golden dataset with SHA-256 manifest; `make golden` (verifies the whole pipeline against the oracle);
`make bench-all` runs every experiment and `uv run python -m bench.summarize` regenerates every table
and figure; each run stores its configuration, versions, git commit and machine. CI runs lint, unit
and PostgreSQL integration tests on every push.

## 10. Limitations

One machine (no network partitions, no multi-node Spark); synthetic data for every measurement
(live data only in the demo); security features described, not measured; virtual-mode time runs
ahead of the wall clock (the dashboard plots arrival time); results are trends for this machine, not
absolute numbers.

## 11. Conclusion

**RQ1.** Processing mode decides the latency floor: continuous processing answers in tens of
milliseconds but saturates between 40,000 and 80,000 records/s on 4 cores, while micro-batch trades
hundreds of milliseconds for at least 100,000 records/s. On the bars pipeline the trigger interval
should be matched to the load (500 ms at moderate load, 1 s at high load; 5 s is never better), and
more partitions than cores do not help.

**RQ2.** With replication factor 3 and min ISR 2, no acknowledged trade was lost in 12 leader kills;
acks=all stalls all writes for about 9 s until the dead broker is fenced, acks=1 stalls only its
partitions but can lose acknowledged writes. A failover can also make Spark drop trades as late when
the watermark is shorter than the skew between partitions. Spark recovers from its own crash in
about 13 to 15 s with exact output.

**RQ3.** Exactly-once output needs an idempotent sink: plain inserts duplicated 300 rows after every
crash between the database commit and Spark's commit, upserts none, and at 2,000 records/s the
upsert cost nothing measurable.

## References

1. J. Kreps, N. Narkhede, J. Rao. Kafka: a distributed messaging system for log processing. NetDB 2011.
2. G. Wang et al. Building a replicated logging system with Apache Kafka. PVLDB 8(12):1654-1655, 2015.
3. M. Armbrust et al. Structured Streaming: a declarative API for real-time applications in Apache Spark. SIGMOD 2018, 601-613.
4. M. Zaharia et al. Discretized streams: fault-tolerant streaming computation at scale. SOSP 2013, 423-438.
5. T. Akidau et al. The Dataflow model: a practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing. PVLDB 8(12):1792-1803, 2015.
