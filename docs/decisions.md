# Decisions and deviations

Every decision and every deviation from the proposal or `PLAN.md`, newest first. Format: date, ID, decision, reason.

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
