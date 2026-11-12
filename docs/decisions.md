# Decisions and deviations

Every decision and every deviation from the proposal or `PLAN.md`, newest first. Format: date, ID, decision, reason.

## 2026-11-12 (M4)

* **Fresh-clone check (T40)**: `scripts/fresh_clone_check.sh` cloned `main` from GitHub into an empty
  folder and followed the README: `uv sync`, ruff, oracle `MATCH`, 80 tests (including PostgreSQL
  integration), `make golden` with images built from the clone: PASS (4,000/4,000 bars, 23/23 alerts).
  Same VM as development, so the second-machine part of T40 stays open for a teammate.
* **Demo rehearsal (T42)**, from that clone: `KILL_SPARK_AT=15 KILL_BROKER_AT=35 scripts/live_check.sh`
  PASS (6,000/6,000 bars, 29/29 alerts after both crashes); `docker compose up` reached a populated
  dashboard with 12 alerts within about two minutes.
* **E1c latency figure**: compared at 30,000 and 60,000 records/s, below saturation. At the top rate
  (90,000) bar latency is backlog (20 to 40 s) and says nothing about the setting.
* **`v1.0`**: the development environment refuses tag pushes (HTTP 403), so a maintainer pushes the
  tag on `main`. Everything after the rehearsed commit (8a8122c) is documentation only.

## 2026-11-12 (M3 findings)

### E2: a broker failover can make Spark drop trades as late (watermark), at 10x time compression

`e2/acks-all-no-idempotence/rep2`: Kafka kept every acknowledged trade exactly once (VER-1 PASS),
but 33 bars of one minute were wrong against both the generator and the topic. Diagnosis: the
failover stalled partitions 0 and 3 for 14.8 s and the other four for 11.0 s. The 33 wrong bars are
exactly the 33 symbols on partitions 0 and 3 that traded in the minute those partitions resumed;
110 of their trades were acknowledged after the other partitions had already resumed. E2 runs
virtual time at 10x, so 3.8 wall seconds of partition skew is about 38 s of event time, more than the
10 s watermark: the other partitions had advanced the watermark, and Spark dropped the delayed
trades as late, as a watermark is designed to do. At real-time speed the same skew would stay inside
the watermark.

Decision: keep E2's procedure (its measurements are loss, duplicates and stall, which are unaffected)
and report bar correctness together with this explanation. Lesson for the report: **the watermark
must exceed the worst partition skew a failure can cause** (here, about 4 s in wall time, times the
time compression). This is the latency/completeness trade-off of decision D6 shown by a real failure.

### M3 ran on two physical CPUs (the VM moved between hosts on restart)

`results/MACHINE.md` (from each run's own `config.json`): machine A, Xeon at 2.10 GHz, and machine
B, Xeon at 2.30 GHz, both 4 vCPUs and 15.7 GB. Every comparison the results make is within one
machine: E2, E3 and the E4 crash runs on A; E1a, E1b, the E1c partition sweep and the E4 cost runs on
B. The E1c trigger sweep started after the VM moved back to A, so its 1 s baseline is re-measured
beside it as setting `p6-1s-b`, and the figures compare the trigger sweep only with that baseline.
Nothing compares numbers across the two machines. Teams rerunning on their own hardware should run
everything there (`make bench-all`).

### Worker restart during M3

The development VM restarted at about 23:30 UTC on 2026-11-11, stopping Docker and the runner for
about four hours. Nothing measured was lost (results are written per run); the runner resumed from
`results/raw/` and repeated only the interrupted run.

## 2026-11-11 (M3 setup)

| Topic | Decision | Reason |
|---|---|---|
| E1a matches the paper | `acks=0`, 10 million records of 200 bytes, batch sizes 1 and 50 messages plus 10, 100, 200 for the curve's shape | Kreps et al. section 5 (read 2026-11-11): one producer, 10 million 200-byte messages, batches of 1 and 50, "the Kafka producer currently doesn't wait for acknowledgements"; reported about 50,000 and 400,000 messages/s. **Deviations:** batch size in bytes (n x 200) approximates n messages; "batch size 1" is `batch.size=0`; producer and broker share one 4-core machine (the paper used two 8-core machines with RAID 10 disks). |
| Rate ladders from capacity probes | E1c: 10k, 30k, 60k per second then one bisection; E1b micro-batch: 40k, 80k, 100k; E1b continuous: 5k to 80k | One-minute probes (6 partitions, 1 s trigger for bars; 4 partitions for pass-through) all kept up at the old ladders' tops (bars 40k/s, micro-batch 80k/s), so the old ladders would only have reported "limited by ladder top". PLAN.md 5.9 allows adapting the ladder to the machine. |
| E1b continuous run length | 2 minutes per rate (20 s warm-up discarded), not 5 | It is a latency curve, not a sustained-throughput test; the proposal fixes five-minute runs for sustained throughput only. E1c and E1b micro-batch keep five-minute steps. |
| E2 acks=1 repeats | 6 kills instead of 3 | Loss with acks=1 happens only when the dead leader had unreplicated acknowledged records (first dry run: 2 lost; second: 0), so it is reported as a rate. |
| Note on D5 | The "end lag at most the largest lag in the first minute" test is lenient when the first minute contains Spark's warm-up backlog; the slope test (below 1% of the input rate) is what fails a rate that Spark cannot sustain | Seen in the probes: first-minute lag peaks of 100k to 530k records from warm-up. Both tests are reported per step in `result.json`. |
| Benchmark machine | The cloud development VM: 4 vCPUs, 15.7 GB, Docker 29.9.2 (`results/MACHINE.md`) | Matches the proposal's one 16 GB machine and Armbrust et al.'s 4 cores. Shared cloud hardware adds noise (risk R7): compare trends, not absolute numbers. The group can rerun everything on its own machine with `make bench-all`. |

## 2026-11-11 (M2)

### Spike S3: continuous mode needs a free core per partition

On `local[4]`, the pass-through job in continuous mode moved 14,997 of 14,997 records with 3 and with 4 partitions, but only 9,881 of 14,997 with 6: two partitions never got a task (risk R4). **E1b uses 4 partitions** for continuous mode, and micro-batch runs use the same count so the two modes are compared fairly.

### Spike S4: one Python generator is enough (decision D9 stands)

`tradegen --mode realtime` into `trades` (3 brokers, RF 3, acks=all, idempotence on), 10 s per step, nothing else running: 9,963/s, 24,901/s, 49,846/s and 99,679/s achieved for targets of 10k, 25k, 50k and 100k per second, every trade acked, no backpressure. The experiment ladders top out at 80k/s (E1b) and 40k/s (E1c). When Spark shares the 4 cores the generator may fall short; the harness marks such a step `generator_limited` and stops the ladder there instead of reporting Spark's limit.

### Measurement choices (T23, T24)

| Topic | Choice | Reason |
|---|---|---|
| Warm-up | Discard the first 30 s (pass-through) and 70 s (bars) of each measured run | Measured: micro-batch pass-through p50 fell from 434 ms in the first 10 s to about 190 ms after 40 s (JVM and connection warm-up). Without this, E1b looked 20 times slower. |
| D5 "keeps up" | Scaled to the run length: end lag (median of the last 5%) at most the largest lag in the first 20%, and slope over the last 80% below 1% of the rate | The D5 wording assumes exactly five minutes; dry runs are shorter. Identical to D5 for a five-minute run. |
| Lag source | Pass-through: input end offsets minus output end offsets. Bars: input end offsets minus the end offsets of Spark's latest batch (`query_progress`) | Both measured outside Spark's own metrics, sampled every second. |
| E2 write stall | Longest gap between acks **within one partition**; the gap across all partitions is kept too | Found in the E2 dry run: with acks=1 only the partitions the dead broker led stall (about 12 s); the other partitions keep acking, so a gap across all partitions showed only 67 ms. With acks=all every partition stalls about 11 s, because the dead broker is a follower in every partition's in-sync replica set until Kafka removes it. Ack times are recorded in the ack log (`ack_ms`, accurate to about 50 sends). |
| E3 recovery | Kill to the first progress event after the restart, and to the first new bar row | The progress event is written when a batch finishes, so the first new bar can come slightly earlier. |
| E1a | `kafka-producer-perf-test` inside the kafka-1 container, `--command-property` (the 4.4 name; `--producer-props` is deprecated), 200-byte records, acks=1, batch.size = messages x 200 bytes | Batches in bytes only approximate the paper's batches in messages (per-record overhead). **Still to do before the full run (T25):** re-read Kreps et al. Figure 4's setup and log any further deviation here. |
| **Deviation from T24's "about one minute":** dry-run length | 1 to 2 minutes per setting, about 20 minutes for `make bench-dry` | Every run starts from a known state (PLAN.md 5.9): a full stack reset (about 25 s) and a fresh Spark start (about 20 s) come before the short measurement. Skipping the reset would make dry runs faster but stop them testing the real procedure. |
| Results layout | `results/raw/<experiment>/<setting>/rep<k>/` with `config.json` first and `result.json` last; runs with a result are skipped | Resumable overnight runs (PLAN.md 5.9). Dry runs go to `results/raw-dry/` (git-ignored). |

### Fault injection (T26)

* SPK-10 (`FAULT_AFTER_BATCH=N`): after batch N's PostgreSQL transaction commits, the Python foreachBatch code SIGKILLs the Spark driver JVM (its parent process) before returning to Spark, so the batch is replayed after a restart. A marker file in the checkpoint volume makes it fire once. It waits 0.5 s before the kill, because Python's output is piped through the JVM and would otherwise be lost from the log.
* `spark-bars` and `spark-passthrough` run with `init: true`: otherwise `spark-submit` is PID 1 in the container, and the kernel ignores SIGKILL sent to PID 1 from inside the container.
* Deterministic replay (golden dataset preloaded, `maxOffsetsPerTrigger=5000`) gave exact output; the late-data risk at batch boundaries noted in M1 did not appear at this batch size. Re-check if E3/E4 change it.
* Dry-run results: E4 insert-mode crash left 300 duplicate bar rows (batch 5's bars), upsert 0; E3 recovered in about 15 s (deterministic) to 28 s (live) with exact output.

### Other M2 choices

| Topic | Choice | Reason |
|---|---|---|
| Dashboard time axis | Time-series panels plot by `written_at` / `query_progress.ts` (wall clock); tables show event-time minutes; candlestick panel fixed to the last 5 minutes | Virtual-mode bars carry January 2026 timestamps, so a normal time range would be empty. At 30x a 15-minute view packs about 450 candles. Checked by screenshot. |
| Dashboard source | Generated by `tools/make_dashboard.py`; a test fails if the committed JSON differs | Hand-edited dashboard JSON drifts. |
| Compose project name | `name: market-pipeline` | Volume names (the harness wipes checkpoints by name) must not depend on the clone's folder name. |
| Checkpoint volumes | `spark-checkpoint` (bars) and `passthrough-checkpoint` (pass-through) | Each can be wiped without touching the other (risk R8). |
| Coinbase feed (T27) | Advanced Trade public market-data endpoint `wss://advanced-trade-ws.coinbase.com`, channel `market_trades` (no key), plus `heartbeats` | Confirmed through search results of Coinbase's docs; the docs host was not reachable from the development sandbox, which also cannot open WebSockets (proxy 403). **Spike S5 must confirm the live feed on a teammate's machine.** The adapter, reconnect, capture and fallback are tested against a local fake server that speaks the documented protocol. |
| Capture replay | Re-stamped to start at the current time, looped | Old event times would be dropped as late by a pipeline that has already seen newer data. |
| Live price precision | Prices go to Kafka with 4 decimals only when that loses nothing (all synthetic prices), otherwise in full | Found while testing: DOGE at 0.119812 was sent as 0.1198. The golden format is unchanged (GEN-1 tests). |
| **Deviation from VER-2:** volume tolerance | Volume within a relative 1e-9 instead of exactly equal | With fractional (crypto) sizes Spark and the oracle add in a different order; 17 of 30 bars differed by about 1e-16 relative. Whole-number sizes are still effectively exact (any real error is at least one unit). OHLC remain exact. |
| Verifying live runs | `python -m verify.verify --from-topic` (VER-2 b) | No generator file exists for live data; the topic's actual contents are the reference. |

## 2026-11-06 (M1)

### D1 adopted: "price change" in the alert rule is the log return of the close

Taken as the plan recommends, because the oracle and the golden dataset (23 expected alerts) already define it that way. **Confirm in the group;** changing it means changing `tools/gen_sample_dataset.py`, `verify/reference.py` and `spark/alert_rule.py` together and regenerating `data/sample/`.

### M1 build: choices and deviations

| Topic | Choice | Reason |
|---|---|---|
| Virtual mode | `tradegen` calls the oracle's `make_trades` directly | GEN-1 holds by construction, and the generator can never drift from the bars and alerts the verifier expects. Tested: seed 42, 40 minutes hashes to the golden `trades.csv.gz`. |
| Producer idempotence | **On by default whenever `acks=all`**; off otherwise; `--idempotence false` to turn it off | Found in a fault check: with `acks=all` but idempotence off, killing the partition-0 leader once duplicated trade 36641 in the topic (a producer retry), and the verifier reported one wrong bar. With idempotence on, 3 out of 3 broker-kill runs were exact. Experiment 2 should measure this explicitly (idempotence on versus off). |
| Alert rule in Spark | Plain Python (`spark/alert_rule.py`) | The Spark image has no numpy or pandas. A test proves it gives the oracle's 23 alerts on the golden data. |
| Alert history | Read back from `bars` in the same transaction: the 31 bars before the batch's first window plus the batch's windows | Correct for any batch size, including the golden run's single 4,000-bar batch and replays after a crash. |
| Insert mode (SPK-11) | `bars_plain` / `alerts_plain`; alert history is read de-duplicated | Duplicates in `bars_plain` must not change the alert maths, only show up as duplicate rows. |
| `query_progress` | Added `query_name`, `batch_ts`, `end_offsets`, `latest_offsets` (jsonb) to Appendix A | SPK-8 asks for per-partition latest versus processed offsets. |
| Shuffle partitions | `spark.sql.shuffle.partitions=4` (equal to the cores) | The default of 200 would mean 200 state-store tasks per batch on 4 cores. Fixed per checkpoint, so it is part of the experiment configuration. |
| Driver memory | `--driver-memory 2g`, container limit 3 GB, 4 CPUs | Within the PLAN.md 7.2 budget. Measured under the 60x stream: Spark 1.1 GB, brokers 0.7 to 0.9 GB each, total about 3.8 GB. |
| Golden run | Topic preloaded completely, then Spark starts from `earliest` | An unpaced replay packs 40 virtual minutes into a few wall seconds; milliseconds of partition skew would exceed the 10 s watermark and drop trades as late. Preloading puts everything in one batch (PLAN.md deterministic replay mode). Still a caveat for T26: with `maxOffsetsPerTrigger`, batch boundaries can split partitions unevenly in event time. |
| Flush trade in the verifier | Assumed present unless `--no-flush` | `tradegen` adds it when sending; trade files do not contain it. |
| Generator image | `python:3.13.18-slim-bookworm`, requirements exported from `uv.lock` | Same versions as the tests (numpy decides the random streams). CI fails if the export is stale. |
| Spark code | Python 3.10 compatible (`per-file-target-version` in ruff) | The official Spark image ships Python 3.10. |

Observed while streaming 60 virtual minutes at 60x (about 1,500 trades/s): batch duration p50 0.9 s, p90 1.8 s, Kafka lag never above 0 at batch end.

Robustness checks run on 2026-11-06 (`scripts/live_check.sh`, 90,071 trades, 6,000 bars, 29 alerts each), all exact (PASS): no faults; Spark SIGKILL and restart from checkpoint; partition-0 leader broker SIGKILL; both faults in the same run (twice). Plus `make golden` from a clean reset, and a second full run over the same data that left `bars` and `alerts` byte-identical (including `written_at`).

## 2026-11-06

### S1: versions pinned (task T01)

All four versions named in the proposal exist as official Docker Hub tags, so **no deviation from the proposal**. Checked against the Docker Hub tag API on 2026-11-06 and a `docker pull` of each image.

| Component | Proposal | Pinned tag | Notes |
|---|---|---|---|
| Apache Kafka | 4.4 | `apache/kafka:4.4.0` | GA, pushed 2026-10-22. Earlier 4.4.0 release candidates exist; do not use them. |
| Apache Spark | 4.2 | `spark:4.2.1-scala2.13-java17-python3-ubuntu` | Official image with Python 3, Scala 2.13, Java 17. |
| Kafka connector for Spark | n/a | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1` | Must match Spark and Scala version. Present on Maven Central. Baked into the image at build time (SPK-12). Maven also lists a 4.3.0 release, which we do not use. |
| PostgreSQL | 18 | `postgres:18.6` | Latest 18.x patch at the time. |
| Grafana | 13 | `grafana/grafana:13.3.0` | Latest 13.x minor at the time. 13.0.12, 13.1.9 and 13.2.5 also exist if 13.3 misbehaves. |

Whether the stack works together is checked in later tasks (T02 to T04), not by this entry.

### M0 foundation: choices made while building it

| Topic | Choice | Reason |
|---|---|---|
| Kafka roles | 3 nodes in KRaft combined mode (`broker,controller`) | Three nodes are the three brokers the proposal fixes, and they also form the controller quorum. Saves two containers. A separate controller set is not needed for the experiments. |
| Compose image tags | Defaults written in `docker-compose.yml`, mirrored in `.env.example` | INF-1 asks for no manual step after a clean clone. `tests/test_config.py` fails if the two differ. |
| Topic creation | `kafka-init` runs `infra/kafka/create-topics.sh` (`--if-not-exists`), `auto.create.topics.enable=false` | Topic settings can never be silently defaulted. `perf_test` is RF 1 with `min.insync.replicas=1`, because the broker default of 2 would reject every write to an RF 1 topic. |
| PostgreSQL data path | Volume mounted at `/var/lib/postgresql`, not `.../data` | The PostgreSQL 18 image keeps data in a versioned subdirectory. |
| Grafana data source type | `grafana-postgresql-datasource`, uid `pg` | Verified: Grafana's data source health check returns "Database Connection OK" on 13.3.0. |
| Exposed ports | Grafana and PostgreSQL bound to `127.0.0.1`; Kafka on `29092` to `29094` for host tools | Credentials are for local development only. Grafana allows anonymous viewers so the demo opens without login. |
| Python | Host and CI: 3.13 (`.python-version`, `uv.lock`). Spark image: Python 3.10 | The official Spark image ships Python 3.10. Only the Spark job code runs there, so the shared code (oracle, tests) does not depend on it. |
| Kafka connector jars | Resolved once at build time with `spark-submit --packages`, then copied into `/opt/spark/jars` | SPK-12. The connector brings `kafka-clients` 3.9.2; it works against the 4.4.0 brokers (T04 proof). |
| Build behind a proxy | Optional build secret `extra_ca` (`EXTRA_CA_FILE`), trusted only during the download steps, deleted afterwards | The cloud sandbox used for development intercepts TLS. Without the secret the build is unchanged. |
| CI actions | `actions/checkout@v7.0.1`, `astral-sh/setup-uv@v10.3.3`, uv 0.11.32 | Latest tags found with `git ls-remote` on 2026-11-06. |
| Oracle formatting | `tools/gen_sample_dataset.py` is excluded from `ruff format` | It is the source of truth for the dataset; its bytes are kept as delivered. |

Idle memory with Kafka x3, PostgreSQL and Grafana up: about 1.3 GB. The budget in `PLAN.md` section 7.2 is a ceiling under load, to be measured in spike S4.

### Workflow: work directly on `main`

Decided by the user on 2026-11-06 for the initial build, while the plan is being finalized, before the repository is shared with the group. This overrides the "one branch and PR per task" rule in `CLAUDE.md` for now. Switch to branches and PRs once the repository is shared. `PLAN.md` section 8.1 also asks for branch protection on `main`, which is not set up yet.
