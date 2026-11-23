"""SPK-8: write every streaming progress event to PostgreSQL ``query_progress``.

Used by the dashboard (pipeline health) and the sustained-throughput test (decision D5): ``lag_total``
is the sum over Kafka partitions of (latest available offset - offset processed by this batch).
"""

import json

import psycopg

try:
    from pyspark.sql.streaming import StreamingQueryListener
except ImportError:  # host-side unit tests import the helpers below without PySpark
    StreamingQueryListener = object

INSERT_SQL = """
INSERT INTO query_progress (query_name, batch_ts, batch_id, num_input_rows, input_rows_per_s,
                            processed_rows_per_s, batch_duration_ms, lag_total, end_offsets, latest_offsets)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _offsets(value):
    """Kafka source offsets as {topic: {partition: offset}}; progress events give them as JSON text."""
    if not value:
        return {}
    return json.loads(value) if isinstance(value, str) else dict(value)


def lag_total(end, latest):
    """Records Spark has not read yet. For each partition it subtracts the offset the batch ended at from
    the latest offset Kafka has, never below zero, and adds them up. A lag that keeps growing means
    Spark is not keeping up.
    """
    total = 0
    for topic, parts in latest.items():
        for part, off in parts.items():
            total += max(0, int(off) - int(end.get(topic, {}).get(part, 0)))
    return total


class ProgressToPostgres(StreamingQueryListener):
    def __init__(self, dsn):
        """Keep the database address. The connection is created on first use."""
        super().__init__()
        self.dsn = dsn
        self.conn = None

    def _connection(self):
        """Return an open autocommit connection, reconnecting if it was closed or never opened."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(self.dsn, autocommit=True)
        return self.conn

    def onQueryStarted(self, event):
        """Nothing to do when the query starts. The listener interface requires the method."""
        pass

    def onQueryProgress(self, event):
        """Store one row per finished batch in query_progress: batch id, rows read, input and processing
        rates, trigger time, source lag and offsets. A database error is printed and ignored, because
        monitoring must never stop the query.
        """
        p = event.progress
        src = p.sources[0] if p.sources else None
        end = _offsets(src.endOffset) if src else {}
        latest = _offsets(src.latestOffset) if src else {}
        row = (
            p.name,
            p.timestamp,
            p.batchId,
            p.numInputRows,
            p.inputRowsPerSecond,
            p.processedRowsPerSecond,
            (p.durationMs or {}).get("triggerExecution"),
            lag_total(end, latest),
            json.dumps(end),
            json.dumps(latest),
        )
        try:
            self._connection().execute(INSERT_SQL, row)
        except Exception as exc:  # never let monitoring kill the query
            print(f"[progress_listener] could not write progress: {exc}", flush=True)
            self.conn = None

    def onQueryIdle(self, event):
        """Nothing to do when a trigger finds no new data. The listener interface requires the method."""
        pass

    def onQueryTerminated(self, event):
        """Nothing to do when the query ends. The listener interface requires the method."""
        pass
