# Trade generator image (tradegen). Build context is the repository root.
ARG PYTHON_IMAGE=python:3.13.18-slim-bookworm
FROM ${PYTHON_IMAGE}

COPY infra/build/with-extra-ca.sh /usr/local/bin/with-extra-ca.sh
# Exported from uv.lock (`make requirements`), so the image uses the same pinned versions as tests.
COPY infra/generator/requirements.txt /tmp/requirements.txt
RUN --mount=type=secret,id=extra_ca,required=false \
    with-extra-ca.sh pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

WORKDIR /app
COPY tools/ /app/tools/
COPY generator/ /app/generator/
ENV PYTHONPATH=/app:/app/generator \
    PYTHONUNBUFFERED=1 \
    RUNS_DIR=/runs \
    KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092

ENTRYPOINT ["python", "-m", "tradegen"]
CMD ["--mode", "virtual", "--speed", "30", "--minutes", "40"]
