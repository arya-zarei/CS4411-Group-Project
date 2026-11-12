"""T33: results/raw -> results/summary.csv, results/RESULTS.md and results/figures/*.png.

  uv run python -m bench.summarize                      # from results/raw (the M3 runs)
  uv run python -m bench.summarize --raw results/raw-dry/final --out /tmp/check

summary.csv is long-format, one row per (experiment, setting, rate, metric): the median, minimum and
maximum over the runs of that setting, plus every run's value. Latency percentiles (p50/p95/p99) are
computed within each run; the CSV reports their median across the three runs (PLAN.md 5.9).
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Reference palette (dataviz skill, light mode); validated: first three slots, all pairs.
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"


# --- loading ---------------------------------------------------------------------------------------
def load(raw: Path) -> dict[str, dict[str, list[dict]]]:
    out: dict = defaultdict(lambda: defaultdict(list))
    for f in sorted(raw.glob("*/*/rep*/result.json")):
        r = json.loads(f.read_text())
        out[r["experiment"]][r["setting"]].append(r)
    for exp in out.values():
        for runs in exp.values():
            runs.sort(key=lambda r: r["repeat"])
    return out


def num(v):
    return None if v is None else float(v)


class Rows:
    def __init__(self):
        self.rows: list[dict] = []

    def add(self, exp, setting, metric, unit, values, rate=None):
        vals = [num(v) for v in values if v is not None]
        if not vals:
            return
        self.rows.append(
            {
                "experiment": exp,
                "setting": setting,
                "rate": "" if rate is None else int(rate),
                "metric": metric,
                "unit": unit,
                "runs": len(vals),
                "median": round(statistics.median(vals), 3),
                "min": round(min(vals), 3),
                "max": round(max(vals), 3),
                "values": json.dumps([round(v, 3) for v in vals]),
            }
        )

    def get(self, exp, setting, metric, rate=None):
        for r in self.rows:
            if (r["experiment"], r["setting"], r["metric"]) == (exp, setting, metric) and (
                rate is None or r["rate"] == int(rate)
            ):
                return r
        return None


def steps_by_rate(runs: list[dict]) -> dict[float, list[dict]]:
    by = defaultdict(list)
    for r in runs:
        for s in r.get("steps", []):
            by[float(s["rate"])].append(s)
    return dict(sorted(by.items()))


def summarize(data) -> Rows:
    t = Rows()
    for s, runs in data.get("e1a", {}).items():
        t.add("e1a", s, "throughput", "records/s", [r["records_per_s"] for r in runs])
        t.add("e1a", s, "throughput_mb", "MB/s", [r["mb_per_s"] for r in runs])
        for p in ("p50", "p99"):
            t.add("e1a", s, f"latency_{p}", "ms", [r.get(f"{p}_latency_ms") for r in runs])
    for exp in ("e1b", "e1c"):
        for s, runs in data.get(exp, {}).items():
            if any("sustained_rate" in r for r in runs):
                t.add(exp, s, "sustained_throughput", "records/s", [r.get("sustained_rate") for r in runs])
                # 1 when the search found no failing rate (ladder top or generator limit): the sustained
                # rate is then a lower bound, not Spark's measured limit
                t.add(exp, s, "sustained_is_lower_bound", "fraction of runs",
                      [0.0 if r.get("limited_by") == "spark" else 1.0 for r in runs])  # fmt: skip
            for rate, steps in steps_by_rate(runs).items():
                key = "latency" if exp == "e1b" else "bar_latency"
                for p in ("p50", "p95", "p99"):
                    t.add(
                        exp, s, f"latency_{p}", "ms", [st.get(key, {}).get(f"{p}_ms") for st in steps], rate
                    )
                t.add(
                    exp,
                    s,
                    "keeps_up",
                    "fraction of runs",
                    [1.0 if st.get("keeps_up") else 0.0 for st in steps],
                    rate,
                )
                if exp == "e1c":
                    t.add(
                        exp,
                        s,
                        "batch_duration_p50",
                        "ms",
                        [st.get("batch_duration_ms", {}).get("p50_ms") for st in steps],
                        rate,
                    )
    for s, runs in data.get("e2", {}).items():
        t.add("e2", s, "lost_trades", "trades", [r["kafka"]["lost"] for r in runs])
        t.add(
            "e2", s, "runs_with_loss", "fraction of runs", [1.0 if r["kafka"]["lost"] else 0.0 for r in runs]
        )
        t.add("e2", s, "duplicated_trades", "trades", [r["kafka"]["duplicated_ids"] for r in runs])
        t.add(
            "e2",
            s,
            "write_stall",
            "s",
            [num(r.get("write_stall_ms")) / 1000 if r.get("write_stall_ms") else None for r in runs],
        )
        t.add("e2", s, "write_stall_all_partitions", "s",
              [num(r.get("write_stall_any_ack_ms")) / 1000 if r.get("write_stall_any_ack_ms") else None for r in runs])  # fmt: skip
        t.add("e2", s, "bars_correct_vs_generator", "fraction of runs",
              [1.0 if r["bars_vs_generator"]["status"] == "PASS" else 0.0 for r in runs])  # fmt: skip
        t.add("e2", s, "bars_correct_vs_topic", "fraction of runs",
              [1.0 if r["bars_vs_topic"]["status"] == "PASS" else 0.0 for r in runs])  # fmt: skip
    for s, runs in data.get("e3", {}).items():
        for m in ("kill_to_first_batch_s", "kill_to_first_bar_s", "restart_to_first_batch_s"):
            t.add("e3", s, m.removesuffix("_s"), "s", [r.get(m) for r in runs])
        t.add(
            "e3",
            s,
            "output_correct",
            "fraction of runs",
            [1.0 if r["bars"]["status"] == "PASS" else 0.0 for r in runs],
        )
    for s, runs in data.get("e4", {}).items():
        if runs[0].get("crash"):
            t.add("e4", s, "duplicate_bar_rows", "rows", [r["duplicate_bar_rows"] for r in runs])
            t.add("e4", s, "duplicate_alert_rows", "rows", [r["duplicate_alert_rows"] for r in runs])
            t.add(
                "e4",
                s,
                "output_correct",
                "fraction of runs",
                [1.0 if r["bars"]["status"] == "PASS" else 0.0 for r in runs],
            )
        else:
            for p in ("p50", "p95", "p99"):
                t.add(
                    "e4",
                    s,
                    f"bar_latency_{p}",
                    "ms",
                    [r["bar_latency"].get(f"{p}_ms") for r in runs],
                    runs[0]["rate"],
                )
            t.add(
                "e4",
                s,
                "batch_duration_p50",
                "ms",
                [r["batch_duration_ms"].get("p50_ms") for r in runs],
                runs[0]["rate"],
            )
            t.add(
                "e4",
                s,
                "keeps_up",
                "fraction of runs",
                [1.0 if r["keeps_up"] else 0.0 for r in runs],
                runs[0]["rate"],
            )
    return t


# --- figures ---------------------------------------------------------------------------------------
def style(ax, title, ylabel, xlabel=None):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10)
    ax.set_ylabel(ylabel, color=INK2, fontsize=9)
    if xlabel:
        ax.set_xlabel(xlabel, color=INK2, fontsize=9)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def new_fig(n=1, width=7.0, height=3.6):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, n, figsize=(width, height), facecolor=SURFACE, squeeze=False)
    return plt, fig, list(axes[0])


def lower_bound(t, exp, setting) -> bool:
    r = t.get(exp, setting, "sustained_is_lower_bound") if setting else None
    return bool(r) and r["min"] == 1.0


def bars(ax, labels, rows, color=BLUE, fmt="{:,.0f}", prefixes=None):
    """Median bar with min-max whisker and every run as a dot; value label above each bar."""
    xs = range(len(labels))
    med = [r["median"] if r else 0 for r in rows]
    ax.bar(xs, med, width=0.6, color=color, zorder=2)
    for x, r in zip(xs, rows, strict=True):
        if not r:
            continue
        vals = json.loads(r["values"])
        if len(vals) > 1:
            ax.vlines(x, r["min"], r["max"], color=INK2, linewidth=1, zorder=3)
            ax.scatter([x] * len(vals), vals, s=10, color=INK, zorder=4)
        ax.annotate((prefixes[x] if prefixes else "") + fmt.format(r["median"]), (x, r["max"]), textcoords="offset points", xytext=(0, 4),
                    ha="center", fontsize=8, color=INK)  # fmt: skip
    ax.set_xticks(list(xs), labels)
    top = max((r["max"] for r in rows if r), default=0)
    ax.set_ylim(0, top * 1.18 if top > 0 else 1)  # counts and durations start at zero, even when all are 0


def plain_log_axis(ax):
    """Log-scale y axis labelled with plain numbers (1,000 rather than 10^3)."""
    from matplotlib.ticker import FuncFormatter, LogLocator

    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))


def thousands_x_axis(ax):
    """x tick labels with thousands separators (20,000 rather than 20000)."""
    from matplotlib.ticker import FuncFormatter

    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))


def save(fig, path: Path, note: str):
    fig.text(0.01, 0.01, note, fontsize=7, color=INK2)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(path, dpi=200, facecolor=SURFACE)


def figures(t: Rows, data, out: Path) -> list[str]:
    made = []
    note = "Median of 3 runs; whisker = min to max; dots = individual runs. Full numbers: results/summary.csv"

    # E1a: Kreps et al. Figure 4 shape
    sets = [
        s
        for s in ("batch-1", "batch-10", "batch-50", "batch-100", "batch-200")
        if t.get("e1a", s, "throughput")
    ]
    if sets:
        plt, fig, (ax,) = new_fig()
        bars(ax, [s.replace("batch-", "") for s in sets], [t.get("e1a", s, "throughput") for s in sets])
        style(ax, "E1a  Producer throughput rises with batching (Kreps et al., Fig. 4)", "records per second",
              "batch size (messages of 200 bytes, approximated in bytes)")  # fmt: skip
        save(fig, out / "e1a_producer_batching.png", note)
        plt.close(fig)
        made.append("e1a_producer_batching.png")

    # E1b: continuous latency versus input rate, and micro-batch sustained throughput (Armbrust Fig. 7)
    cont = next((s for s in data.get("e1b", {}) if "continuous" in s), None)
    micro = next((s for s in data.get("e1b", {}) if "microbatch" in s), None)
    if cont:
        rates = sorted(
            {
                r["rate"]
                for r in t.rows
                if r["experiment"] == "e1b" and r["setting"] == cont and r["rate"] != ""
            }
        )
        plt, fig, (ax,) = new_fig()
        for p, color in (("p50", BLUE), ("p99", ORANGE)):
            ys = [t.get("e1b", cont, f"latency_{p}", r) for r in rates]
            pts = [(r, y["median"]) for r, y in zip(rates, ys, strict=True) if y]
            if pts:
                ax.plot(
                    [x for x, _ in pts],
                    [y for _, y in pts],
                    color=color,
                    linewidth=2,
                    marker="o",
                    markersize=5,
                )
                ax.annotate(f"continuous {p}", pts[-1], textcoords="offset points", xytext=(6, 0),
                            va="center", fontsize=8, color=INK)  # fmt: skip
        sus = t.get("e1b", micro, "sustained_throughput") if micro else None
        ax.set_yscale("log")
        plain_log_axis(ax)
        thousands_x_axis(ax)
        if sus:
            ax.axvline(sus["median"], color=AQUA, linewidth=2, linestyle="--")
            bound = "≥ " if lower_bound(t, "e1b", micro) else ""
            # label at the bottom of the line, clear of the continuous-mode labels at the top right
            ax.annotate(f"micro-batch sustained\n{bound}{sus['median']:,.0f} records/s", (sus["median"], ax.get_ylim()[0]),
                        textcoords="offset points", xytext=(-6, 6), ha="right", va="bottom", fontsize=8, color=INK)  # fmt: skip
        style(ax, "E1b  Pass-through latency vs input rate (Armbrust et al., Fig. 7)", "latency, ms (log scale)",
              "input rate, records per second")  # fmt: skip
        save(
            fig,
            out / "e1b_continuous_vs_microbatch.png",
            "Median over 3 runs of each run's percentile. 4 partitions, local[4].",
        )
        plt.close(fig)
        made.append("e1b_continuous_vs_microbatch.png")

    # E1c: sustained throughput by partitions and by trigger (RQ1)
    part = [s for s in ("p1-1s", "p3-1s", "p6-1s") if t.get("e1c", s, "sustained_throughput")]
    trig = [s for s in ("p6-100ms", "p6-500ms", "p6-1s", "p6-5s") if t.get("e1c", s, "sustained_throughput")]
    if part or trig:
        plt, fig, axes = new_fig(2, width=8.5)
        if part:
            bars(
                axes[0],
                [s.split("-")[0].removeprefix("p") for s in part],
                [t.get("e1c", s, "sustained_throughput") for s in part],
                prefixes=["≥ " if lower_bound(t, "e1c", s) else "" for s in part],
            )
            style(axes[0], "by partitions (trigger 1 s)", "sustained records per second", "Kafka partitions")
        if trig:
            bars(
                axes[1],
                [s.split("-")[1] for s in trig],
                [t.get("e1c", s, "sustained_throughput") for s in trig],
                color=BLUE,
                prefixes=["≥ " if lower_bound(t, "e1c", s) else "" for s in trig],
            )
            style(
                axes[1],
                "by trigger interval (6 partitions)",
                "sustained records per second",
                "trigger interval",
            )
        fig.suptitle(
            "E1c  Bars pipeline: sustained throughput (RQ1)", x=0.01, ha="left", fontsize=11, color=INK
        )
        save(
            fig,
            out / "e1c_sustained_throughput.png",
            note + " ≥: no failing rate found (ladder top or input limit).",
        )
        plt.close(fig)
        made.append("e1c_sustained_throughput.png")

        # bar latency at a rate every setting was run at
        settings = [
            s
            for s in ("p1-1s", "p3-1s", "p6-100ms", "p6-500ms", "p6-1s", "p6-5s")
            if s in data.get("e1c", {})
        ]
        common = None
        for rate in sorted({r["rate"] for r in t.rows if r["experiment"] == "e1c" and r["rate"] != ""}):
            if all(t.get("e1c", s, "latency_p50", rate) for s in settings):
                common = rate  # the highest common rate wins
        if common:
            plt, fig, (ax,) = new_fig(width=8.0)
            xs = list(range(len(settings)))
            for i, (p, color) in enumerate((("p50", BLUE), ("p95", ORANGE), ("p99", AQUA))):
                ys = [t.get("e1c", s, f"latency_{p}", common)["median"] / 1000 for s in settings]
                ax.bar([x + (i - 1) * 0.26 for x in xs], ys, width=0.24, color=color, label=p, zorder=2)
            ax.set_xticks(xs, settings)
            ax.legend(frameon=False, fontsize=8, ncols=3, loc="upper left")
            style(ax, f"E1c  Bar latency at {common:,} records/s (window end + watermark to row written)", "seconds",
                  "setting (partitions-trigger)")  # fmt: skip
            save(fig, out / "e1c_bar_latency.png", "Median over 3 runs of each run's percentile.")
            plt.close(fig)
            made.append("e1c_bar_latency.png")

    # E2: broker crash (RQ2)
    e2 = [s for s in ("acks-1", "acks-all", "acks-all-no-idempotence") if t.get("e2", s, "lost_trades")]
    if e2:
        plt, fig, axes = new_fig(2, width=8.5)
        bars(axes[0], e2, [t.get("e2", s, "write_stall") for s in e2], fmt="{:.1f} s")
        style(axes[0], "write stall (longest ack gap in a partition)", "seconds")
        bars(axes[1], e2, [t.get("e2", s, "lost_trades") for s in e2], color=ORANGE, fmt="{:.0f}")
        style(axes[1], "acknowledged trades lost", "trades")
        if all(t.get("e2", s, "lost_trades")["max"] == 0 for s in e2):
            kills = sum(len(data["e2"][s]) for s in e2)
            axes[1].text(0.5, 0.55, f"no acknowledged trade lost\nin {kills} broker kills", transform=axes[1].transAxes,
                         ha="center", va="center", fontsize=10, color=INK)  # fmt: skip
        fig.suptitle(
            "E2  Killing the partition-0 leader broker mid-run (RF 3, min ISR 2) (RQ2)",
            x=0.01,
            ha="left",
            fontsize=11,
            color=INK,
        )
        save(
            fig,
            out / "e2_broker_crash.png",
            "Bars: median; whisker: min to max; dots: individual kills ("
            + ", ".join(f"{s}: {len(data['e2'][s])} kills" for s in e2)
            + ").",
        )
        plt.close(fig)
        made.append("e2_broker_crash.png")

    # E3: Spark crash recovery (RQ2)
    e3 = [s for s in ("live", "deterministic") if t.get("e3", s, "kill_to_first_bar")]
    if e3:
        plt, fig, (ax,) = new_fig(width=6.0)
        bars(ax, e3, [t.get("e3", s, "kill_to_first_bar") for s in e3], fmt="{:.1f} s")
        style(ax, "E3  Spark SIGKILL to first new bar written (RQ2)", "seconds")
        save(fig, out / "e3_spark_recovery.png", note)
        plt.close(fig)
        made.append("e3_spark_recovery.png")

    # E4: exactly-once cost (RQ3)
    if t.get("e4", "insert-crash", "duplicate_bar_rows") or t.get("e4", "insert-cost", "batch_duration_p50"):
        plt, fig, axes = new_fig(2, width=8.5)
        crash = [s for s in ("insert-crash", "upsert-crash") if t.get("e4", s, "duplicate_bar_rows")]
        if crash:
            bars(
                axes[0],
                [s.split("-")[0] for s in crash],
                [t.get("e4", s, "duplicate_bar_rows") for s in crash],
                color=ORANGE,
            )
            style(axes[0], "duplicate bar rows after a crash", "rows")
        cost = [s for s in ("insert-cost", "upsert-cost") if t.get("e4", s, "bar_latency_p50")]
        if cost:
            bars(
                axes[1],
                [s.split("-")[0] for s in cost],
                [t.get("e4", s, "bar_latency_p50") for s in cost],
                fmt="{:,.0f} ms",
            )
            rate = t.get("e4", cost[0], "bar_latency_p50")["rate"]
            style(axes[1], f"crash-free bar latency p50 at {rate:,} records/s", "ms")
        fig.suptitle(
            "E4  Plain insert versus insert-or-update (RQ3)", x=0.01, ha="left", fontsize=11, color=INK
        )
        save(fig, out / "e4_exactly_once.png", note)
        plt.close(fig)
        made.append("e4_exactly_once.png")
    return made


# --- markdown --------------------------------------------------------------------------------------
TITLES = {
    "e1a": "E1a Producer batching (reproduction a)",
    "e1b": "E1b Pass-through, continuous versus micro-batch (reproduction b)",
    "e1c": "E1c Bars pipeline: partitions and trigger interval (RQ1)",
    "e2": "E2 Broker crash (RQ2)",
    "e3": "E3 Spark crash (RQ2)",
    "e4": "E4 Exactly-once cost (RQ3)",
}


def markdown(t: Rows, figs: list[str], raw: Path) -> str:
    lines = [
        "# Results",
        "",
        f"Generated by `uv run python -m bench.summarize` from `{raw.relative_to(ROOT) if raw.is_relative_to(ROOT) else raw}`. "
        "Each cell: median over the runs [min to max], number of runs in brackets after the metric. "
        "Machine and versions: [MACHINE.md](MACHINE.md). Every number: [summary.csv](summary.csv).",
        "",
    ]
    for exp, title in TITLES.items():
        rows = [r for r in t.rows if r["experiment"] == exp]
        if not rows:
            continue
        lines += [
            f"## {title}",
            "",
            "| setting | rate | metric | unit | median | min to max |",
            "|---|---|---|---|---|---|",
        ]
        for r in rows:
            rate = f"{r['rate']:,}" if r["rate"] != "" else ""
            lines.append(
                f"| {r['setting']} | {rate} | {r['metric']} ({r['runs']}) | {r['unit']} | {r['median']:,} | {r['min']:,} to {r['max']:,} |"
            )
        lines.append("")
    if figs:
        lines += ["## Figures", ""] + [f"![{f}](figures/{f})" for f in figs] + [""]
    return "\n".join(lines)


def machine_md(raw: Path) -> str:
    """results/MACHINE.md: the machine and versions every run in `raw` recorded (PLAN.md 4.5, 5.9)."""
    configs = [json.loads(f.read_text()) for f in sorted(raw.glob("*/*/rep*/config.json"))]
    machines = {json.dumps(c.get("machine", {}), sort_keys=True) for c in configs}
    commits = sorted({c.get("git_commit") or "?" for c in configs})
    started = sorted(c.get("started_at", "") for c in configs)
    env = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        if (
            "=" in line
            and not line.startswith("#")
            and line.split("=", 1)[0].endswith(("IMAGE", "CONNECTOR"))
        ):
            k, v = line.split("=", 1)
            env[k] = v
    import tomllib

    packages = {p["name"]: p["version"] for p in tomllib.loads((ROOT / "uv.lock").read_text())["package"]}

    def locked(pkg):
        return packages.get(pkg, "?")

    lines = ["# Benchmark machine and versions", "",
             "Generated by `uv run python -m bench.summarize` from the runs' own `config.json` files.", ""]  # fmt: skip
    if len(machines) == 1 and configs:
        m = json.loads(next(iter(machines)))
        lines += ["All runs recorded the same machine:", "", "| | |", "|---|---|"]
        lines += [f"| {k} | {v} |" for k, v in m.items()]
    else:
        lines += [
            f"**Warning: runs recorded {len(machines)} different machines.** PLAN.md 5.9 asks for one.",
            "",
        ]
        lines += [f"- `{m}`" for m in sorted(machines)]
    lines += ["", f"Runs: {len(configs)}, from {started[0] if started else '?'} to {started[-1] if started else '?'} (UTC).",
              f"Git commits: {', '.join(c[:10] for c in commits)}.", "", "## Versions", "", "| Component | Version |", "|---|---|"]  # fmt: skip
    lines += [f"| {k} | `{v}` |" for k, v in env.items()]
    lines += [
        f"| {p} (Python, host) | {locked(p)} |" for p in ("confluent-kafka", "numpy", "pandas", "psycopg")
    ]
    lines += ["", "## Spark and pipeline settings", "",
              "`local[4]` in one container limited to 4 CPUs and 3 GB, driver 2 GB, `spark.sql.shuffle.partitions=4`, "
              "Kafka heaps 1 GB, 3 brokers, RF 3, min ISR 2. Details: docker-compose.yml and docs/decisions.md.", ""]  # fmt: skip
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", type=Path, default=ROOT / "results" / "raw")
    ap.add_argument("--out", type=Path, default=ROOT / "results")
    a = ap.parse_args(argv)
    data = load(a.raw)
    t = summarize(data)
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "figures").mkdir(exist_ok=True)
    with open(a.out / "summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(t.rows[0]) if t.rows else ["experiment"])
        w.writeheader()
        w.writerows(t.rows)
    figs = figures(t, data, a.out / "figures")
    (a.out / "RESULTS.md").write_text(markdown(t, figs, a.raw))
    (a.out / "MACHINE.md").write_text(machine_md(a.raw))
    print(f"{len(t.rows)} summary rows, {len(figs)} figures -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
