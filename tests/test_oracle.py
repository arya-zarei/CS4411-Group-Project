"""T05: the oracle reproduces the committed golden files, and the two copies of it agree."""

from __future__ import annotations

import ast
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tools import gen_sample_dataset as gen
from verify import reference as ref

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "data" / "sample"


@pytest.fixture(scope="module")
def trades() -> pd.DataFrame:
    return ref.load_sample_trades()


@pytest.fixture(scope="module")
def bars(trades) -> pd.DataFrame:
    return ref.compute_bars(trades)


def test_bars_equal_expected(bars):
    expected = ref.load_expected_bars()
    assert len(bars) == 4000
    assert list(bars.columns) == list(expected.columns)
    key = ["symbol", "window_start_ms"]
    a = bars.sort_values(key).reset_index(drop=True)
    b = expected.sort_values(key).reset_index(drop=True)
    for col in ("symbol", "window_start_ms", "window_end_ms", "trade_count"):
        assert (a[col] == b[col]).all(), col
    for col in ("open", "high", "low", "close", "volume"):
        np.testing.assert_allclose(a[col], b[col], rtol=0, atol=1e-9, err_msg=col)
    np.testing.assert_allclose(a["vwap"], b["vwap"], rtol=0, atol=1e-6)


def test_alerts_equal_expected(bars):
    alerts = ref.compute_alerts(bars)
    expected = ref.load_expected_alerts()
    assert len(alerts) == 23
    key = ["symbol", "window_start_ms"]
    a = alerts.sort_values(key).reset_index(drop=True)
    b = expected.sort_values(key).reset_index(drop=True)
    assert (a[key] == b[key]).all().all()
    for col in ("close", "log_return", "baseline_mean", "baseline_std", "z_score"):
        np.testing.assert_allclose(a[col], b[col], rtol=1e-6, atol=1e-9, err_msg=col)


def test_all_injected_shocks_are_detected(bars):
    alerts = ref.compute_alerts(bars)
    shocks = pd.DataFrame(json.loads((SAMPLE / "shocks.json").read_text()))
    det = gen.shock_detection(shocks, bars, alerts)
    assert det["detected"] == det["shocks"] == 20


def test_dataset_shape(trades):
    assert len(trades) == 60163
    assert trades["symbol"].nunique() == 100
    assert trades["trade_id"].is_unique


# --- copy-sync guard: verify/reference.py must equal the oracle script ---------------------------
def _fn_source(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.dump(node)
    raise AssertionError(f"{name} not found in {path}")


@pytest.mark.parametrize("name", ["compute_bars", "compute_alerts"])
def test_reference_is_copy_of_oracle(name):
    assert _fn_source(ROOT / "verify" / "reference.py", name) == _fn_source(
        ROOT / "tools" / "gen_sample_dataset.py", name
    )


def test_constants_match_oracle():
    assert (ref.WINDOW_MS, ref.BASELINE_BARS, ref.Z_THRESHOLD) == (
        gen.WINDOW_MS,
        gen.BASELINE_BARS,
        gen.Z_THRESHOLD,
    )


# --- golden generator: skipped on a different numpy/pandas than MANIFEST.json --------------------
def test_generator_matches_manifest():
    manifest = json.loads((SAMPLE / "MANIFEST.json").read_text())
    if manifest["versions"]["numpy"] != np.__version__ or manifest["versions"]["pandas"] != pd.__version__:
        pytest.skip("numpy/pandas differ from MANIFEST.json; random streams are only stable per version")
    files, *_ = gen.build(manifest["params"])
    for name, raw in files.items():
        assert gen.sha256_bytes(raw) == manifest["sha256_uncompressed"][name], name


# --- alert edge cases (PLAN.md section 10) -------------------------------------------------------
def _bars_from_closes(closes, symbol="X"):
    n = len(closes)
    start = np.arange(n) * ref.WINDOW_MS
    return pd.DataFrame(
        dict(
            symbol=symbol,
            window_start_ms=start,
            window_end_ms=start + ref.WINDOW_MS,
            open=closes,
            high=closes,
            low=closes,
            close=closes,
            volume=1.0,
            vwap=closes,
            trade_count=1,
        )
    )


def test_no_alert_with_fewer_than_32_bars():
    closes = [100.0] * 30 + [200.0]  # 31 bars: a huge jump but no full baseline yet
    assert ref.compute_alerts(_bars_from_closes(closes)).empty


def test_alert_on_32nd_bar_not_before():
    rng = np.random.default_rng(1)
    closes = list(100 * np.exp(np.cumsum(rng.normal(0, 0.001, 31))))
    closes.append(closes[-1] * math.exp(0.05))  # 32nd bar: large jump
    alerts = ref.compute_alerts(_bars_from_closes(closes))
    assert list(alerts["window_start_ms"]) == [31 * ref.WINDOW_MS]
    assert alerts["z_score"].iloc[0] > ref.Z_THRESHOLD


def test_zero_variance_baseline_never_alerts():
    closes = [100.0] * 32 + [101.0]  # std of the previous 30 changes is exactly 0
    assert ref.compute_alerts(_bars_from_closes(closes)).empty


def test_missing_minutes_use_previous_existing_bars():
    # Bars need not be contiguous: "previous 30 bars" means the previous 30 that exist.
    rng = np.random.default_rng(2)
    closes = list(100 * np.exp(np.cumsum(rng.normal(0, 0.001, 31))))
    closes.append(closes[-1] * math.exp(0.05))
    bars = _bars_from_closes(closes)
    bars["window_start_ms"] = bars["window_start_ms"] * 2  # a gap of one minute between every bar
    bars["window_end_ms"] = bars["window_start_ms"] + ref.WINDOW_MS
    assert len(ref.compute_alerts(bars)) == 1


def test_symbols_are_independent():
    rng = np.random.default_rng(3)
    base = list(100 * np.exp(np.cumsum(rng.normal(0, 0.001, 31))))
    shocked = [*base, base[-1] * math.exp(0.05)]
    calm = [*base, base[-1]]
    both = pd.concat([_bars_from_closes(shocked, "A"), _bars_from_closes(calm, "B")], ignore_index=True)
    assert list(ref.compute_alerts(both)["symbol"]) == ["A"]
