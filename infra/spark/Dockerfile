# Spark image with the Kafka connector baked in (PLAN.md SPK-12), so `docker compose up` never downloads packages at run time.
# Build context is the repository root.
ARG SPARK_IMAGE=spark:4.2.1-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG SPARK_KAFKA_CONNECTOR=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1

USER root

COPY infra/spark/with-extra-ca.sh /usr/local/bin/with-extra-ca.sh
COPY infra/spark/requirements.txt /tmp/requirements.txt
RUN --mount=type=secret,id=extra_ca,required=false \
    with-extra-ca.sh pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Resolve the connector and its transitive jars once, at build time, then copy them next to Spark's own jars.
# `cp -n` keeps any jar Spark already ships.
RUN --mount=type=secret,id=extra_ca,required=false \
    echo 'print("resolved")' > /tmp/noop.py \
 && with-extra-ca.sh /opt/spark/bin/spark-submit --master 'local[1]' --conf spark.jars.ivy=/tmp/ivy --packages "${SPARK_KAFKA_CONNECTOR}" /tmp/noop.py \
 && cp -n /tmp/ivy/jars/*.jar /opt/spark/jars/ \
 && ls /opt/spark/jars | grep -E 'kafka|commons-pool2' \
 && rm -rf /tmp/ivy /tmp/noop.py

RUN mkdir -p /checkpoints /app && chown spark:spark /checkpoints /app

COPY --chown=spark:spark spark/ /app/

USER spark
WORKDIR /app
