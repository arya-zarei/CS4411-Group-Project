"""Pass-through job (PLAN.md 5.6, PTJ-1 to PTJ-4): Kafka `trades` -> Kafka `passthrough_out`, unchanged.

No parsing and no aggregation, so it can run in continuous mode, which only supports simple
per-record operations. Used for reproduction (b), Armbrust et al. Figure 7 (Experiment E1b).
`passthrough_out` uses LogAppendTime, so latency = Kafka's write timestamp - the trade's creation time.

Configuration (environment variables):
  PT_MODE                  microbatch (default) or continuous
  TRIGGER_INTERVAL         micro-batch trigger interval, default "100 milliseconds"
  CONTINUOUS_CHECKPOINT    continuous-mode checkpoint interval, default "1 second"
  KAFKA_BOOTSTRAP_SERVERS  default kafka-1:9092,kafka-2:9092,kafka-3:9092
  INPUT_TOPIC / OUTPUT_TOPIC  default trades / passthrough_out
  STARTING_OFFSETS         default latest (benchmarks measure only new records)
  MAX_OFFSETS_PER_TRIGGER  micro-batch only, default unset
  CHECKPOINT_DIR           default /checkpoints/passthrough-<mode>
"""

import os
import sys

from pyspark.sql import SparkSession

MODE = os.environ.get("PT_MODE", "microbatch")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "100 milliseconds")
CONTINUOUS_CHECKPOINT = os.environ.get("CONTINUOUS_CHECKPOINT", "1 second")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "trades")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "passthrough_out")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", f"/checkpoints/passthrough-{MODE}")


def main():
    if MODE not in ("microbatch", "continuous"):
        sys.exit(f"PT_MODE must be microbatch or continuous, not {MODE!r}")
    spark = SparkSession.builder.appName(f"passthrough-{MODE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "true")
    )
    if MAX_OFFSETS_PER_TRIGGER and MODE == "microbatch":
        reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    writer = (
        reader.load()
        .selectExpr("key", "value")  # PTJ-1: same key and value, no parsing
        .writeStream.format("kafka")
        .queryName(f"passthrough-{MODE}")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_DIR)
    )
    if MODE == "continuous":
        writer = writer.trigger(continuous=CONTINUOUS_CHECKPOINT)  # PTJ-2
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)
    query = writer.start()
    print(
        f"[passthrough] started: mode={MODE} {INPUT_TOPIC} -> {OUTPUT_TOPIC} "
        f"trigger={CONTINUOUS_CHECKPOINT if MODE == 'continuous' else TRIGGER_INTERVAL}",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
