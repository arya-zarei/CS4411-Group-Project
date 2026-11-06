"""Spark Structured Streaming job: 1-minute bars and alerts per symbol (PLAN.md 5.4, 5.5).

Kafka ``trades`` -> parse JSON -> watermark -> 1-minute tumbling window per symbol (append mode)
-> foreachBatch: one PostgreSQL transaction per micro-batch writes the bars and their alerts.

Configuration (environment variables):
  KAFKA_BOOTSTRAP_SERVERS  default kafka-1:9092,kafka-2:9092,kafka-3:9092
  KAFKA_TOPIC              default trades
  STARTING_OFFSETS         default earliest
  MAX_OFFSETS_PER_TRIGGER  default unset (no limit)
  TRIGGER_INTERVAL         default "1 second"  (SPK-6)
  WATERMARK_DELAY          default "10 seconds" (decision D6)
  CHECKPOINT_DIR           default /checkpoints/bars
  SINK_MODE                upsert (default) or insert (SPK-11)
  PG_DSN                   PostgreSQL connection string
"""

import os
import sys

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from progress_listener import ProgressToPostgres
from sink import ms_to_ts, write_batch

FLUSH_SYMBOL = "__FLUSH__"

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "1 second")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "10 seconds")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints/bars")
SINK_MODE = os.environ.get("SINK_MODE", "upsert")
PG_DSN = os.environ.get("PG_DSN", "postgresql://pipeline:change-me-locally@postgres:5432/market")
QUERY_NAME = os.environ.get("QUERY_NAME", "bars")

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", LongType()),
        StructField("symbol", StringType()),
        StructField("event_time_ms", LongType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
    ]
)


def build_bars(spark):
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "true")  # SPK-1
    )
    if MAX_OFFSETS_PER_TRIGGER:
        reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)

    trades = (
        reader.load()
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))  # SPK-2: explicit schema
        .select("t.*")
        .where(F.col("event_time_ms").isNotNull() & F.col("symbol").isNotNull())
        .withColumn("event_time", F.timestamp_millis(F.col("event_time_ms")))
        .withWatermark("event_time", WATERMARK_DELAY)  # SPK-3
    )
    # The __FLUSH__ trade must reach the watermark operator; it is dropped later, in the sink.
    # Ties on event time are broken by trade_id so open and close are deterministic (SPK-4).
    order = F.struct(F.col("event_time_ms"), F.col("trade_id"))
    return (
        trades.groupBy(F.window("event_time", "1 minute").alias("w"), F.col("symbol"))
        .agg(
            F.min_by("price", order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", order).alias("close"),
            F.sum("size").alias("volume"),
            F.sum(F.col("price") * F.col("size")).alias("notional"),
            F.count(F.lit(1)).alias("trade_count"),
        )
        .select(
            "symbol",
            F.unix_millis(F.col("w.start")).alias("window_start_ms"),
            F.unix_millis(F.col("w.end")).alias("window_end_ms"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            F.try_divide(F.col("notional"), F.col("volume")).alias("vwap"),  # SPK-9: ANSI mode
            "trade_count",
        )
    )


class BatchWriter:
    """foreachBatch target. Keeps one database connection for the life of the driver."""

    def __init__(self, dsn, mode):
        self.dsn = dsn
        self.mode = mode
        self.conn = None

    def __call__(self, df, batch_id):
        rows = df.where(F.col("symbol") != FLUSH_SYMBOL).collect()  # at most ~100 bars per closed minute
        bars = [
            {
                "symbol": r["symbol"],
                "window_start": ms_to_ts(r["window_start_ms"]),
                "window_end": ms_to_ts(r["window_end_ms"]),
                "open": r["open"],
                "high": r["high"],
                "low": r["low"],
                "close": r["close"],
                "volume": r["volume"],
                "vwap": r["vwap"],
                "trade_count": r["trade_count"],
            }
            for r in rows
        ]
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(self.dsn)
        try:
            n_bars, n_alerts = write_batch(self.conn, bars, self.mode)
        except Exception:
            self.conn.close()
            self.conn = None
            raise
        if n_bars:
            print(f"[bars_job] batch {batch_id}: {n_bars} bars, {n_alerts} alerts", flush=True)


def main():
    if SINK_MODE not in ("upsert", "insert"):
        sys.exit(f"SINK_MODE must be upsert or insert, not {SINK_MODE!r}")
    spark = SparkSession.builder.appName("bars-job").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressToPostgres(PG_DSN))
    query = (
        build_bars(spark)
        .writeStream.queryName(QUERY_NAME)
        .outputMode("append")  # SPK-5: each bar emitted once, when final
        .foreachBatch(BatchWriter(PG_DSN, SINK_MODE))
        .option("checkpointLocation", CHECKPOINT_DIR)  # SPK-7
        .trigger(processingTime=TRIGGER_INTERVAL)  # SPK-6
        .start()
    )
    print(
        f"[bars_job] started: topic={TOPIC} trigger={TRIGGER_INTERVAL} watermark={WATERMARK_DELAY} "
        f"sink={SINK_MODE} checkpoint={CHECKPOINT_DIR}",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
