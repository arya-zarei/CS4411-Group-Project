"""T04 / spike S2: minimal PySpark job that reads the `trades` topic and prints each record.

Proves that the Kafka connector is baked into the image and that Spark can reach the 3-broker cluster.
Not part of the pipeline. Configuration comes from environment variables.
"""

import os

from pyspark.sql import SparkSession

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

spark = SparkSession.builder.appName("smoke-kafka-read").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "true")
    .load()
)

out = stream.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "partition",
    "offset",
)

query = (
    out.writeStream.format("console")
    .option("truncate", "false")
    .outputMode("append")
    .trigger(processingTime="1 second")
    .start()
)
query.awaitTermination()
