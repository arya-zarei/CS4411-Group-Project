"""The alert rule (PLAN.md 5.5) in plain Python, for the Spark driver (no numpy or pandas there).

Must give the same alerts as ``compute_alerts`` in ``verify/reference.py``; ``tests/test_alert_rule.py``
checks that on the golden dataset. Decision D1: "price change" is the log return of the close
between consecutive existing bars.

    change_t = ln(close_t / close_{t-1})
    mean_t, std_t = mean and sample std (ddof=1) of change_{t-30} .. change_{t-1}
    alert_t  = std_t > 0 and |change_t - mean_t| > 3 * std_t
"""

import math

BASELINE_BARS = 30
Z_THRESHOLD = 3.0


def evaluate(closes, indices=None):
    """Alerts for one symbol.

    ``closes``: closes of the symbol's consecutive existing bars, oldest first.
    ``indices``: which positions to evaluate (default: all). Positions without a full baseline
    (the first 31 bars) never alert.
    Returns a list of ``(index, log_return, baseline_mean, baseline_std, z_score)``.
    """
    n = len(closes)
    changes = [None] + [math.log(closes[i] / closes[i - 1]) for i in range(1, n)]
    out = []
    for t in range(n) if indices is None else sorted(indices):
        if t < BASELINE_BARS + 1:
            continue
        base = changes[t - BASELINE_BARS : t]
        mean = math.fsum(base) / BASELINE_BARS
        std = math.sqrt(math.fsum((c - mean) ** 2 for c in base) / (BASELINE_BARS - 1))
        chg = changes[t]
        if std > 0 and abs(chg - mean) > Z_THRESHOLD * std:
            out.append((t, chg, mean, std, (chg - mean) / std))
    return out
