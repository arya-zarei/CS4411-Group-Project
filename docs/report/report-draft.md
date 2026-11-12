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
M3.)*

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
open and close, VWAP via `try_divide`); the alert rule (decision D1: bar-to-bar log return of the
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

*(Fill each subsection from `results/RESULTS.md` once M3 has run; one figure each.)*

- **6.1 E1a, producer batching (reproduction a).** Figure `e1a_producer_batching.png`.
- **6.2 E1b, continuous versus micro-batch (reproduction b).** Figure `e1b_continuous_vs_microbatch.png`.
- **6.3 E1c, partitions and trigger (RQ1).** Figures `e1c_sustained_throughput.png`, `e1c_bar_latency.png`.
- **6.4 E2, broker crash (RQ2).** Figure `e2_broker_crash.png`.
- **6.5 E3, Spark crash (RQ2).** Figure `e3_spark_recovery.png`.
- **6.6 E4, exactly-once cost (RQ3).** Figure `e4_exactly_once.png`.

## 7. Reproduction and extension: discussion

For each reproduction, state what matches in shape, what differs, and why (one shared 4-core
machine instead of the papers' dedicated hardware; Kafka 4.4's Java producer counts batches in bytes
rather than messages; modern Kafka writes through the page cache with replication available).
For the extension, state the answers to RQ2 and RQ3 directly, with the numbers.

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

*(Answer RQ1 to RQ3 in one paragraph each, with the headline numbers.)*

## References

1. J. Kreps, N. Narkhede, J. Rao. Kafka: a distributed messaging system for log processing. NetDB 2011.
2. G. Wang et al. Building a replicated logging system with Apache Kafka. PVLDB 8(12):1654-1655, 2015.
3. M. Armbrust et al. Structured Streaming: a declarative API for real-time applications in Apache Spark. SIGMOD 2018, 601-613.
4. M. Zaharia et al. Discretized streams: fault-tolerant streaming computation at scale. SOSP 2013, 423-438.
5. T. Akidau et al. The Dataflow model: a practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing. PVLDB 8(12):1792-1803, 2015.
