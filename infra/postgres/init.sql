-- PLAN.md Appendix A. Runs once, when the postgres data volume is first created.

CREATE TABLE bars (
  symbol        text             NOT NULL,
  window_start  timestamptz      NOT NULL,
  window_end    timestamptz      NOT NULL,
  open          double precision NOT NULL,
  high          double precision NOT NULL,
  low           double precision NOT NULL,
  close         double precision NOT NULL,
  volume        double precision NOT NULL,
  vwap          double precision NOT NULL,
  trade_count   integer          NOT NULL,
  written_at    timestamptz      NOT NULL DEFAULT clock_timestamp(),  -- first insert only
  PRIMARY KEY (symbol, window_start)
);

CREATE TABLE alerts (
  symbol        text             NOT NULL,
  window_start  timestamptz      NOT NULL,
  window_end    timestamptz      NOT NULL,
  close         double precision NOT NULL,
  log_return    double precision NOT NULL,
  baseline_mean double precision NOT NULL,
  baseline_std  double precision NOT NULL,
  z_score       double precision NOT NULL,
  written_at    timestamptz      NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (symbol, window_start)
);

-- Experiment 4 baseline: same columns, no unique key, so replays create duplicates.
CREATE TABLE bars_plain   (LIKE bars   INCLUDING DEFAULTS, id bigserial PRIMARY KEY);
CREATE TABLE alerts_plain (LIKE alerts INCLUDING DEFAULTS, id bigserial PRIMARY KEY);

CREATE TABLE query_progress (
  id                   bigserial PRIMARY KEY,
  ts                   timestamptz NOT NULL DEFAULT clock_timestamp(),
  batch_id             bigint,
  num_input_rows       bigint,
  input_rows_per_s     double precision,
  processed_rows_per_s double precision,
  batch_duration_ms    bigint,
  lag_total            bigint
);

CREATE INDEX ON bars (window_start);
CREATE INDEX ON alerts (window_start DESC);
