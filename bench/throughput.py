"""Sustained throughput (PLAN.md 5.9, decision D5).

Spark "keeps up" with an input rate over a run when the gap between the newest Kafka offset and the
offset Spark has processed does not grow. D5 is written for a five-minute run; it is applied here to
any duration T by scaling its windows:

* the lag at the end of the run is at most the largest lag seen in the first 20% (minute 1 of 5);
  "at the end" is the median of the samples in the last 5% of the run, so one sample taken during a
  batch does not decide the result;
* the least-squares slope of lag over the last 80% (minutes 2 to 5) is below 1% of the input rate
  (records per second).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import numpy as np


def keeps_up(samples: list[tuple[float, float]], rate: float) -> dict:
    """samples: (seconds since the generator started, lag in records)."""
    if len(samples) < 10:
        return {"keeps_up": False, "reason": "too few lag samples", "samples": len(samples)}
    a = np.asarray(samples, dtype=float)
    t, lag = a[:, 0], a[:, 1]
    span = t[-1] - t[0]
    first = lag[t <= t[0] + 0.2 * span]
    tail = lag[t >= t[-1] - max(0.05 * span, 3.0)]
    later = t >= t[0] + 0.2 * span
    end_lag = float(np.median(tail))
    max_first = float(first.max())
    slope = float(np.polyfit(t[later], lag[later], 1)[0]) if later.sum() >= 3 else float("inf")
    ok_end = end_lag <= max_first
    ok_slope = slope < 0.01 * rate
    return {
        "keeps_up": bool(ok_end and ok_slope),
        "end_lag": end_lag,
        "max_lag_first_20pct": max_first,
        "slope_records_per_s": slope,
        "slope_limit": 0.01 * rate,
        "max_lag": float(lag.max()),
        "samples": int(len(samples)),
    }


class LagSampler:
    """Calls ``lag_fn()`` about once a second in a background thread."""

    def __init__(self, lag_fn: Callable[[], float], interval_s: float = 1.0):
        self.lag_fn = lag_fn
        self.interval_s = interval_s
        self.samples: list[tuple[float, float]] = []
        self.errors = 0
        self._stop = threading.Event()
        self._t0 = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> LagSampler:
        self._t0 = time.monotonic()
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.samples.append((time.monotonic() - self._t0, float(self.lag_fn())))
            except Exception:  # noqa: BLE001  a missed sample is not fatal
                self.errors += 1
            self._stop.wait(self.interval_s)

    def stop(self) -> list[tuple[float, float]]:
        self._stop.set()
        self._thread.join(timeout=10)
        return self.samples


def search(rates: list[float], measure: Callable[[float], dict], bisect: bool = True) -> dict:
    """Step up a ladder of rates until one fails, then bisect once (PLAN.md 5.9).
    ``measure(rate)`` returns a dict with ``keeps_up`` (and anything else worth keeping).
    A step whose generator could not reach the target rate stops the search: the limit found is
    then the generator's, and the result says so."""
    steps, last_ok, first_fail = [], None, None
    for rate in rates:
        r = measure(rate)
        steps.append({"rate": rate, **r})
        if r.get("generator_limited"):
            return {"sustained_rate": last_ok, "limited_by": "generator", "steps": steps}
        if r["keeps_up"]:
            last_ok = rate
        else:
            first_fail = rate
            break
    if bisect and first_fail is not None and last_ok is not None:
        mid = (last_ok + first_fail) / 2
        r = measure(mid)
        steps.append({"rate": mid, **r})
        if r["keeps_up"] and not r.get("generator_limited"):
            last_ok = mid
    limited_by = "spark" if first_fail is not None else "ladder_top"
    return {"sustained_rate": last_ok, "limited_by": limited_by, "steps": steps}
