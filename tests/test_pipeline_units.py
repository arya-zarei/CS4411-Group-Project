"""Unit tests for M1 code that need no Docker: generator formatting, alert rule, verifier, listener."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import alert_rule
from progress_listener import lag_total
from tradegen import cli
from tradegen.model import (
    CSV_HEADER,
    FLUSH_SYMBOL,
    Trade,
    csv_line,
    flush_trade,
    kafka_value,
    replay_trades,
    virtual_params,
    virtual_trades,
)
from verify import reference as ref
from verify import verify as ver

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "data" / "sample"
MANIFEST = json.loads((SAMPLE / "MANIFEST.json").read_text())
SAME_VERSIONS = (
    MANIFEST["versions"]["numpy"] == np.__version__ and MANIFEST["versions"]["pandas"] == pd.__version__
)


@pytest.fixture(scope="module")
def sample_bars() -> pd.DataFrame:
    return ref.compute_bars(ref.load_sample_trades())


# --- tradegen (T10) ------------------------------------------------------------------------------
@pytest.mark.skipif(
    not SAME_VERSIONS, reason="random streams are only stable for the manifest's numpy/pandas"
)
def test_gen1_virtual_seed42_equals_golden_trades():
    """GEN-1 through tradegen's own record and formatting path."""
    h = hashlib.sha256()
    h.update(CSV_HEADER.encode())
    for t in virtual_trades(virtual_params(seed=42, minutes=40)):
        h.update(csv_line(t).encode())
    assert h.hexdigest() == MANIFEST["sha256_uncompressed"]["trades.csv.gz"]


def test_kafka_value_round_trips_to_the_same_numbers():
    trades = list(replay_trades(SAMPLE / "trades.csv.gz"))
    assert len(trades) == 60163
    for t in trades[:2000]:
        d = json.loads(kafka_value(t))
        assert d == {
            "trade_id": t.trade_id,
            "symbol": t.symbol,
            "event_time_ms": t.event_time_ms,
            "price": t.price,
            "size": t.size,
        }
    # the value carries the same 4-decimal text as trades.csv.gz
    assert kafka_value(Trade(1, "SYM078", 1767623400023, 81.3873, 25)) == (
        b'{"trade_id":1,"symbol":"SYM078","event_time_ms":1767623400023,"price":81.3873,"size":25}'
    )


def test_fractional_sizes_are_kept_for_live_data():
    d = json.loads(kafka_value(Trade(7, "BTC-USD", 1, 65000.12, 0.00123)))
    assert d["size"] == 0.00123


def test_live_prices_keep_full_precision():
    assert json.loads(kafka_value(Trade(1, "DOGE-USD", 1, 0.119812, 10.5)))["price"] == 0.119812
    assert b'"price":81.3873,' in kafka_value(Trade(1, "SYM078", 1, 81.3873, 25))  # golden format unchanged


def test_flush_trade_closes_every_window():
    last = Trade(60163, "SYM001", 1_000_000, 10.0, 1)
    f = flush_trade(last)
    assert f.symbol == FLUSH_SYMBOL and f.trade_id == 60164 and f.event_time_ms == 1_000_000 + 120_000


def test_virtual_params_default_is_the_oracle_default():
    p = virtual_params()
    assert p["n_shocks"] == 20 and p["shock_end_s"] == 38 * 60 + 30


def test_shock_rate_spreads_shocks_after_warmup_and_caps_at_symbols():
    p = virtual_params(minutes=120, shock_rate_per_min=0.5)
    assert p["shock_start_s"] == 32 * 60 and p["shock_end_s"] == 120 * 60 - 30
    assert p["n_shocks"] == round(0.5 * (p["shock_end_s"] - p["shock_start_s"]) / 60)
    assert virtual_params(minutes=1000, shock_rate_per_min=5)["n_shocks"] == 100


def test_cli_validates_arguments():
    with pytest.raises(SystemExit):
        cli.parse_args(["--mode", "replay"])  # needs --file
    with pytest.raises(SystemExit):
        cli.parse_args(["--mode", "virtual", "--log-trades"])  # needs --run-id
    a = cli.parse_args(["--mode", "virtual", "--speed", "30"])
    assert a.speed == 30.0 and cli.producer_conf(a)["acks"] == "all"
    assert cli.parse_args(["--mode", "virtual"]).speed is None  # max

    # idempotence follows acks unless set explicitly; it cannot be combined with acks=1
    assert cli.producer_conf(cli.parse_args(["--mode", "virtual"]))["enable.idempotence"] is True
    assert (
        cli.producer_conf(cli.parse_args(["--mode", "virtual", "--acks", "1"]))["enable.idempotence"] is False
    )
    a = cli.parse_args(["--mode", "virtual", "--idempotence", "false"])
    assert cli.producer_conf(a)["enable.idempotence"] is False
    with pytest.raises(SystemExit):
        cli.parse_args(["--mode", "virtual", "--acks", "1", "--idempotence", "true"])


def test_dump_csv_matches_golden_file(tmp_path):
    if not SAME_VERSIONS:
        pytest.skip("numpy/pandas differ from the manifest")
    out = tmp_path / "t.csv"
    assert cli.main(["--mode", "virtual", "--minutes", "40", "--dump-csv", str(out)]) == 0
    assert hashlib.sha256(out.read_bytes()).hexdigest() == MANIFEST["sha256_uncompressed"]["trades.csv.gz"]


# --- alert rule used by the Spark sink (T12) ----------------------------------------------------
def _alerts_by_rule(bars: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for sym, b in bars.sort_values(KEY := ["symbol", "window_start_ms"]).groupby("symbol"):
        b = b.reset_index(drop=True)
        for i, r, m, s, z in alert_rule.evaluate(list(b["close"])):
            rows.append((sym, int(b.loc[i, "window_start_ms"]), r, m, s, z))
    return pd.DataFrame(rows, columns=[*KEY, "log_return", "baseline_mean", "baseline_std", "z_score"])


def test_alert_rule_equals_oracle_on_golden_data(sample_bars):
    mine = _alerts_by_rule(sample_bars).sort_values(["symbol", "window_start_ms"]).reset_index(drop=True)
    exp = ref.load_expected_alerts().sort_values(["symbol", "window_start_ms"]).reset_index(drop=True)
    assert len(mine) == 23
    assert (mine[["symbol", "window_start_ms"]] == exp[["symbol", "window_start_ms"]]).all().all()
    for c in ("log_return", "baseline_mean", "baseline_std", "z_score"):
        np.testing.assert_allclose(mine[c], exp[c], rtol=1e-7, atol=1e-12, err_msg=c)


def test_alert_rule_constants_match_oracle():
    assert (alert_rule.BASELINE_BARS, alert_rule.Z_THRESHOLD) == (ref.BASELINE_BARS, ref.Z_THRESHOLD)


def test_alert_rule_edge_cases():
    assert alert_rule.evaluate([100.0] * 30 + [200.0]) == []  # 31 bars: no full baseline
    assert alert_rule.evaluate([100.0] * 32 + [101.0]) == []  # zero variance never alerts
    rng = np.random.default_rng(1)
    closes = list(100 * np.exp(np.cumsum(rng.normal(0, 0.001, 31))))
    closes.append(closes[-1] * math.exp(0.05))
    hits = alert_rule.evaluate(closes)
    assert [h[0] for h in hits] == [31]  # the 32nd bar
    assert alert_rule.evaluate(closes, indices=[30]) == []  # only the requested positions


# --- verifier (T13) ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def expected():
    trades = ref.load_sample_trades()
    flush = pd.DataFrame(
        [
            {
                "trade_id": 60164,
                "symbol": FLUSH_SYMBOL,
                "event_time_ms": int(trades["event_time_ms"].max()) + 120_000,
                "price": 1.0,
                "size": 1,
            }
        ]
    )
    return ver.expected_output(pd.concat([trades, flush], ignore_index=True), 10_000)


def test_expected_output_with_flush_requires_every_bar(expected):
    bars, alerts, uncertain, _ = expected
    assert len(bars) == 4000 and len(alerts) == 23 and uncertain.empty


def test_with_flush_adds_the_closing_trade_once():
    trades = ref.load_sample_trades()
    flushed = ver.with_flush(trades)
    assert len(flushed) == len(trades) + 1
    assert flushed.iloc[-1]["event_time_ms"] == trades["event_time_ms"].max() + 120_000
    assert len(ver.with_flush(flushed)) == len(flushed)  # idempotent
    bars, alerts, uncertain, _ = ver.expected_output(flushed, 10_000)
    assert len(bars) == 4000 and len(alerts) == 23 and uncertain.empty


def test_unclosed_windows_are_not_required():
    trades = ref.load_sample_trades()  # no flush trade: the last minute cannot have closed
    bars, _, uncertain, cutoff = ver.expected_output(trades, 10_000)
    assert len(bars) < 4000
    assert (bars["window_end_ms"] <= cutoff).all()


def test_compare_bars_pass_and_each_failure_kind(expected):
    exp_bars = expected[0]
    assert ver.compare_bars(exp_bars, exp_bars.copy())["ok"]

    corrupted = exp_bars.copy()
    corrupted.loc[5, "close"] += 0.0001
    r = ver.compare_bars(exp_bars, corrupted)
    assert not r["ok"] and r["value_mismatches"] == 1 and r["mismatches_by_column"]["close"] == 1

    r = ver.compare_bars(exp_bars, exp_bars.drop(index=[0, 1]))
    assert not r["ok"] and r["missing"] == 2

    extra = pd.concat([exp_bars, exp_bars.iloc[[0]].assign(window_start_ms=0)], ignore_index=True)
    assert ver.compare_bars(exp_bars, extra)["extra"] == 1

    dup = pd.concat([exp_bars, exp_bars.iloc[[3, 4]]], ignore_index=True)
    r = ver.compare_bars(exp_bars, dup)
    assert not r["ok"] and r["duplicate_rows"] == 2

    vw = exp_bars.copy()
    vw.loc[7, "vwap"] += 5e-7  # within tolerance
    assert ver.compare_bars(exp_bars, vw)["ok"]

    vol = exp_bars.copy()
    vol["volume"] = vol["volume"].astype(float)  # fractional sizes, as from a live feed
    vol.loc[2, "volume"] *= 1 + 1e-15  # summation-order noise: accepted
    assert ver.compare_bars(exp_bars, vol)["ok"]
    vol.loc[2, "volume"] += 1  # one share missing: a real error
    r = ver.compare_bars(exp_bars, vol)
    assert not r["ok"] and r["mismatches_by_column"]["volume"] == 1


def test_compare_alerts(expected):
    exp_alerts = expected[1]
    assert ver.compare_alerts(exp_alerts, exp_alerts.copy())["ok"]
    r = ver.compare_alerts(exp_alerts, exp_alerts.iloc[1:])
    assert not r["ok"] and r["missing"] == 1
    z = exp_alerts.copy()
    z.loc[0, "z_score"] += 1e-3
    assert ver.compare_alerts(exp_alerts, z)["value_mismatches"] == 1


def test_borderline_alerts_are_ignored():
    base = dict(
        symbol="X", window_end_ms=60_000, close=1.0, log_return=0.0, baseline_mean=0.0, baseline_std=1.0
    )
    borderline = pd.DataFrame([dict(base, window_start_ms=0, z_score=3.0 + 1e-12)])
    empty = borderline.iloc[0:0]
    assert ver.compare_alerts(borderline, empty)["ok"]
    assert ver.compare_alerts(empty, borderline)["ok"]


# --- progress listener (T15) ---------------------------------------------------------------------
def test_lag_total_sums_partitions():
    end = {"trades": {"0": 10, "1": 5}}
    latest = {"trades": {"0": 15, "1": 5, "2": 3}}
    assert lag_total(end, latest) == 5 + 0 + 3
    assert lag_total({}, {}) == 0
