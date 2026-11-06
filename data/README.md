# Data

## Golden sample dataset (`data/sample/`)

A small, fully synthetic reference dataset. It is the contract between the generator, the Spark job and the verifier. It is committed to the repo (about 1.1 MB).

| File | Contents |
|---|---|
| `trades.csv.gz` | 60,163 trades: `trade_id,symbol,event_time_ms,price,size`. 100 symbols (`SYM001` to `SYM100`), start 2026-01-05T14:30:00Z, 40 virtual minutes, seed 42. |
| `bars_expected.csv` | 4,000 one-minute bars (100 symbols by 40 minutes): open, high, low, close, volume, vwap, trade_count. |
| `alerts_expected.csv` | 23 alerts under the project's alert rule (PLAN.md section 5.5). |
| `shocks.json` | The 20 injected price jumps. All 20 are detected. 3 further alerts are ordinary statistical exceedances. |
| `symbols.csv` | Per-symbol start price, 1-minute volatility and arrival rate. |
| `MANIFEST.json` | Parameters, row counts, SHA-256 of each file's uncompressed content, numpy and pandas versions. |

Regenerate or verify:

```bash
python tools/gen_sample_dataset.py --out data/sample           # regenerate
python tools/gen_sample_dataset.py --out data/sample --check   # prints MATCH if identical
```

The numpy random streams are only guaranteed stable for a given numpy version. `MANIFEST.json` records the versions used. If `--check` reports a mismatch on a different numpy version, compare against the committed files rather than regenerating.

## Real data

The benchmarks use synthetic data only (the proposal's choice, so the correct answer is known exactly). The live demo uses Coinbase's public trade feed. Captured live data and large run outputs are not committed. Files over about 5 MB go in a GitHub release with a download script.
