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

### Workflow: work directly on `main`

Decided by the user on 2026-11-06 for the initial build, while the plan is being finalized, before the repository is shared with the group. This overrides the "one branch and PR per task" rule in `CLAUDE.md` for now. Switch to branches and PRs once the repository is shared. `PLAN.md` section 8.1 also asks for branch protection on `main`, which is not set up yet.
