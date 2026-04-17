"""
Depth Filter Sweep — Experiment 1
==================================
Does filtering by depth_pct (how much a stock has fallen from its 20-day high)
improve CAGR, drawdown, and Sharpe for the VWAP Reclaim reversal strategy?

Tests depth buckets:
  - No filter (baseline, current live config)
  - 0–5%, 5–10%, 10–15%, 15–20%, 20–25%, 25–30%, >30%
  - Also tests depth_min only (skip shallow dips)
  - Also tests depth_max only (skip deep crashes)

Walk-forward sweep with parallel workers.

Usage:
    uv run python scripts/sweep_depth_filter.py
    uv run python scripts/sweep_depth_filter.py --workers 8
"""

import argparse
import logging
import multiprocessing as mp
import shutil
import sys
import tempfile
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

log = logging.getLogger("depth_sweep")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
)

# ── Walk-forward windows ──
WALK_FORWARD_WINDOWS = [
    (date(2016, 1, 1), date(2019, 12, 31), date(2020, 1, 1), date(2021, 12, 31)),
    (date(2018, 1, 1), date(2021, 12, 31), date(2022, 1, 1), date(2023, 12, 31)),
    (date(2020, 1, 1), date(2023, 12, 31), date(2024, 1, 1), date(2025, 12, 31)),
]
HOLDOUT_START = date(2023, 1, 1)
HOLDOUT_END = date(2025, 12, 31)

TXN_COST_PCT = 0.003
MIN_TRADES = 10
MAX_HOLD = 10


# ══════════════════════════════════════════════════════════
#  DATA LOADING (with DB copy to avoid lock conflicts)
# ══════════════════════════════════════════════════════════

def _copy_db(src: Path, tmp_dir: Path) -> Path:
    """Copy a DuckDB file + WAL to a temp dir so we don't conflict with server."""
    dst = tmp_dir / src.name
    shutil.copy2(src, dst)
    wal = src.with_suffix(".duckdb.wal")
    if wal.exists():
        shutil.copy2(wal, tmp_dir / wal.name)
    return dst


def load_and_prepare(tmp_dir: Path):
    """Load daily + intraday data, compute features and forward returns."""
    market_db = _copy_db(ROOT / "db" / "market.duckdb", tmp_dir)
    intra_db = _copy_db(ROOT / "db" / "intraday.duckdb", tmp_dir)

    con = duckdb.connect(str(market_db), read_only=True)
    daily = con.execute(
        "SELECT date, ticker, open, high, low, close, volume FROM prices ORDER BY ticker, date"
    ).df()
    bench = con.execute("SELECT date, close AS bench_close FROM benchmark ORDER BY date").df()
    con.close()
    log.info("Loaded %d daily rows, %d bench rows", len(daily), len(bench))

    # Intraday first-hour VWAP / volume
    icon = duckdb.connect(str(intra_db), read_only=True)
    intra_feat = icon.execute("""
        WITH bars AS (
            SELECT ticker, CAST(ts AS DATE) AS date,
                   EXTRACT(HOUR FROM ts) AS hr, EXTRACT(MINUTE FROM ts) AS mn,
                   open, high, low, close, volume
            FROM candles
            WHERE interval = '15minute' AND EXTRACT(HOUR FROM ts) BETWEEN 9 AND 15
        ),
        day_agg AS (
            SELECT ticker, date,
                SUM(volume) AS day_volume, COUNT(*) AS n_bars,
                LAST(close ORDER BY hr, mn) AS day_close,
                SUM(CASE WHEN hr = 9 OR (hr = 10 AND mn < 15) THEN volume ELSE 0 END) AS fh_vol,
                SUM(CASE WHEN hr = 9 OR (hr = 10 AND mn < 15)
                    THEN ((high + low + close) / 3.0) * volume ELSE 0 END) AS fh_tp_vol
            FROM bars GROUP BY ticker, date
            HAVING n_bars >= 4 AND day_volume > 0
        )
        SELECT ticker, date, day_volume, fh_vol,
               CASE WHEN fh_vol > 0 THEN fh_tp_vol / fh_vol END AS vwap_1h
        FROM day_agg ORDER BY ticker, date
    """).df()
    icon.close()
    log.info("Intraday features: %d rows", len(intra_feat))

    # Rolling RVOL
    intra_feat = intra_feat.sort_values(["ticker", "date"])
    intra_feat["rvol_1h"] = intra_feat.groupby("ticker")["fh_vol"].transform(
        lambda x: x / x.rolling(20, min_periods=5).mean()
    )

    # Daily features per ticker
    results = []
    for ticker, grp in daily.groupby("ticker"):
        grp = grp.sort_values("date").copy()
        if len(grp) < 200:
            continue
        close = grp["close"]

        is_down = close.diff() < 0
        groups = (~is_down).cumsum()
        grp["down_days"] = is_down.groupby(groups).cumsum().astype(int)
        grp["depth_pct"] = (close - close.rolling(20).max()) / close.rolling(20).max()
        grp["prev_close"] = close.shift(1)
        grp["day_change_pct"] = close / grp["prev_close"] - 1

        # Forward returns for trailing-stop sim
        for k in range(1, MAX_HOLD + 1):
            grp[f"fwd_close_{k}"] = close.shift(-k)
            grp[f"fwd_high_{k}"] = grp["high"].shift(-k)

        results.append(grp)

    daily_ctx = pd.concat(results, ignore_index=True)
    log.info("Daily features: %d rows", len(daily_ctx))

    # Merge all
    daily_ctx["date"] = pd.to_datetime(daily_ctx["date"]).dt.date
    intra_feat["date"] = pd.to_datetime(intra_feat["date"]).dt.date
    bench["date"] = pd.to_datetime(bench["date"]).dt.date

    bench = bench.sort_values("date")
    bench["bench_ma200"] = bench["bench_close"].rolling(200).mean()
    bench["is_bear"] = bench["bench_close"] < bench["bench_ma200"]

    merged = daily_ctx.merge(intra_feat, on=["date", "ticker"], how="inner",
                             suffixes=("", "_intra"))
    merged = merged.merge(bench[["date", "is_bear"]], on="date", how="left")
    merged = merged[merged["day_volume"] >= 50000].copy()

    log.info("Merged: %d rows", len(merged))
    return merged, bench


# ══════════════════════════════════════════════════════════
#  SIGNAL GENERATION & TRADE SIMULATION
# ══════════════════════════════════════════════════════════

def generate_signals(merged, params):
    """Generate VWAP-reclaim signals with depth_min / depth_max filters."""
    p = params.get("vwap_reclaim", {})
    has_vwap = merged["vwap_1h"].notna() & merged["close"].notna()

    sig = (
        has_vwap
        & (merged["down_days"] >= p.get("down_days", 3))
        & (merged["rvol_1h"] >= p.get("rvol_thresh", 1.5))
        & (merged["close"] > merged["vwap_1h"])
    )

    # Depth filters — depth_pct is negative (e.g., -0.15 means stock is 15% below 20d high)
    depth_min = p.get("depth_min")  # minimum depth (e.g., -0.05 means at least 5% drop)
    depth_max = p.get("depth_max")  # maximum depth (e.g., -0.15 means no more than 15% drop)

    if depth_min is not None:
        sig = sig & (merged["depth_pct"] <= depth_min)
    if depth_max is not None:
        sig = sig & (merged["depth_pct"] >= depth_max)

    return sig


def simulate_trades(signal_rows, trail_pct, max_hold, txn_cost=TXN_COST_PCT):
    """Trailing stop simulator — vectorized per-trade."""
    n = len(signal_rows)
    entry_prices = signal_rows["close"].values
    trade_rets = np.full(n, np.nan)

    fwd_closes = np.column_stack([
        signal_rows[f"fwd_close_{k}"].values for k in range(1, max_hold + 1)
    ])
    fwd_highs = np.column_stack([
        signal_rows[f"fwd_high_{k}"].values for k in range(1, max_hold + 1)
    ])

    for i in range(n):
        entry = entry_prices[i]
        if np.isnan(entry) or entry <= 0:
            continue
        peak = entry
        exit_price = None
        for d in range(max_hold):
            close_d = fwd_closes[i, d]
            high_d = fwd_highs[i, d]
            if np.isnan(close_d):
                break
            peak = max(peak, high_d)
            drop_from_peak = close_d / peak - 1
            if trail_pct > 0 and drop_from_peak <= -trail_pct:
                exit_price = peak * (1 - trail_pct)
                break
        if exit_price is None:
            last_valid = None
            for d2 in range(min(d + 1, max_hold) - 1, -1, -1):
                if not np.isnan(fwd_closes[i, d2]):
                    last_valid = fwd_closes[i, d2]
                    break
            if last_valid is None:
                continue
            exit_price = last_valid
        trade_rets[i] = exit_price / entry - 1 - txn_cost

    valid = ~np.isnan(trade_rets)
    return trade_rets, valid


def run_backtest(merged, params, trail_pct=0.02, max_hold=10, max_positions=5):
    """Generate signals, simulate trades, compute metrics."""
    sig_mask = generate_signals(merged, params)
    needed_col = f"fwd_close_{max_hold}"
    if needed_col not in merged.columns:
        return None

    signal_rows = merged[sig_mask & merged[needed_col].notna()].copy()
    if signal_rows.empty or len(signal_rows) < MIN_TRADES:
        return None

    # Rank by rvol (best first), limit to max_positions per day
    signal_rows = signal_rows.sort_values(["date", "rvol_1h"], ascending=[True, False])
    signal_rows["_rank"] = signal_rows.groupby("date").cumcount()
    signal_rows = signal_rows[signal_rows["_rank"] < max_positions]

    trade_rets, valid = simulate_trades(signal_rows, trail_pct, max_hold)
    valid_rows = signal_rows[valid].copy()
    valid_rets = trade_rets[valid]

    if len(valid_rows) < MIN_TRADES:
        return None

    valid_rows["_ret"] = valid_rets
    daily_sig = valid_rows.groupby("date").agg(
        avg_ret=("_ret", "mean"),
        n_positions=("ticker", "count"),
    ).reset_index().sort_values("date")

    all_dates = sorted(merged["date"].unique())
    full_cal = pd.DataFrame({"date": all_dates})
    full_cal = full_cal.merge(daily_sig[["date", "avg_ret", "n_positions"]],
                              on="date", how="left")
    full_cal["avg_ret"] = full_cal["avg_ret"].fillna(0.0)
    full_cal["cum_ret"] = (1 + full_cal["avg_ret"]).cumprod()

    start_eq = 1_000_000
    end_eq = full_cal["cum_ret"].iloc[-1] * start_eq
    date_col = pd.to_datetime(full_cal["date"])
    total_days = (date_col.iloc[-1] - date_col.iloc[0]).days
    years = max(total_days / 365.25, 0.01)
    xirr = (end_eq / start_eq) ** (1 / years) - 1

    cum_peak = full_cal["cum_ret"].cummax()
    dd = (full_cal["cum_ret"] - cum_peak) / cum_peak
    max_dd = dd.min()

    rets = full_cal["avg_ret"]
    sharpe = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0
    win_rate = (valid_rets > 0).mean()

    return {
        "xirr": round(xirr * 100, 1),
        "max_dd": round(max_dd * 100, 1),
        "sharpe": round(sharpe, 2),
        "n_trades": len(valid_rows),
        "win_rate": round(win_rate * 100, 1),
    }


# ══════════════════════════════════════════════════════════
#  SWEEP GRID
# ══════════════════════════════════════════════════════════

# Depth buckets — depth_pct is NEGATIVE (e.g., -0.10 = 10% below 20d high)
DEPTH_BUCKETS = {
    "no_filter":   (None,  None),     # baseline: current live config
    "0-5%":        (None,  -0.05),    # shallow dips only (0 to -5%)
    "5-10%":       (-0.05, -0.10),    # moderate dips
    "10-15%":      (-0.10, -0.15),    # meaningful pullbacks
    "15-20%":      (-0.15, -0.20),    # deep pullbacks
    "20-25%":      (-0.20, -0.25),    # corrections
    "25-30%":      (-0.25, -0.30),    # deep corrections
    ">30%":        (-0.30, None),     # crashes — skip everything less than 30%
    # Also test depth_max only (skip deep crashes)
    "max_10%":     (None,  -0.10),    # only trade if <10% drop
    "max_15%":     (None,  -0.15),    # only trade if <15% drop
    "max_20%":     (None,  -0.20),    # only trade if <20% drop
    "max_25%":     (None,  -0.25),    # only trade if <25% drop
    # Skip shallow dips (require minimum depth)
    "min_5%":      (-0.05, None),     # must have dropped at least 5%
    "min_10%":     (-0.10, None),     # must have dropped at least 10%
    "min_15%":     (-0.15, None),     # must have dropped at least 15%
}


def build_sweep_configs():
    """Build parameter grid: depth buckets × core params."""
    configs = []
    for bucket_name, (depth_min, depth_max) in DEPTH_BUCKETS.items():
        for dd in [3, 4, 5]:
            for rvol in [1.0, 1.5, 2.0]:
                for trail in [0.02, 0.03, 0.05]:
                    for hold in [5, 7, 10]:
                        label = f"{bucket_name}_D{dd}_R{rvol}_T{trail}_H{hold}"
                        configs.append({
                            "vwap_reclaim": {
                                "down_days": dd,
                                "rvol_thresh": rvol,
                                "bear_only": False,
                                "depth_min": depth_min,
                                "depth_max": depth_max,
                            },
                            "_trail": trail,
                            "_hold": hold,
                            "_label": label,
                            "_bucket": bucket_name,
                        })
    return configs


def _eval_one(args):
    """Worker function for parallel evaluation."""
    merged_slice, cfg, label = args
    try:
        res = run_backtest(
            merged_slice, cfg,
            trail_pct=cfg["_trail"],
            max_hold=cfg["_hold"],
        )
        if res:
            res["label"] = label
            res["bucket"] = cfg["_bucket"]
            res["down_days"] = cfg["vwap_reclaim"]["down_days"]
            res["rvol"] = cfg["vwap_reclaim"]["rvol_thresh"]
            res["trail"] = cfg["_trail"]
            res["hold"] = cfg["_hold"]
        return res
    except Exception:
        return None


# ══════════════════════════════════════════════════════════
#  WALK-FORWARD + HOLDOUT
# ══════════════════════════════════════════════════════════

def find_stable_configs(merged, configs, windows, n_workers):
    """Config is stable if profitable (XIRR>0) in ALL walk-forward windows."""
    window_results = {}

    for wi, (train_s, train_e, test_s, test_e) in enumerate(windows):
        test_dates = merged["date"].apply(lambda d: test_s <= d <= test_e)
        test_data = merged[test_dates].copy()
        log.info("WF %d: test %s→%s (%d rows)", wi, test_s, test_e, len(test_data))

        if n_workers > 1:
            with mp.Pool(n_workers) as pool:
                results = pool.map(
                    _eval_one,
                    [(test_data, c, c["_label"]) for c in configs],
                )
        else:
            results = [_eval_one((test_data, c, c["_label"])) for c in configs]

        for r in results:
            if r is not None:
                label = r["label"]
                if label not in window_results:
                    window_results[label] = []
                window_results[label].append(r)

    stable = {}
    for label, wr in window_results.items():
        if len(wr) == len(windows) and all(r["xirr"] > 0 for r in wr):
            stable[label] = {
                "avg_xirr": round(np.mean([r["xirr"] for r in wr]), 1),
                "avg_dd": round(np.mean([r["max_dd"] for r in wr]), 1),
                "avg_sharpe": round(np.mean([r["sharpe"] for r in wr]), 2),
                "avg_trades": round(np.mean([r["n_trades"] for r in wr]), 0),
                "avg_win_rate": round(np.mean([r["win_rate"] for r in wr]), 1),
                "bucket": wr[0]["bucket"],
                "down_days": wr[0]["down_days"],
                "rvol": wr[0]["rvol"],
                "trail": wr[0]["trail"],
                "hold": wr[0]["hold"],
            }
    return stable


def main():
    parser = argparse.ArgumentParser(description="Depth filter sweep for VWAP Reclaim")
    parser.add_argument("--workers", type=int, default=mp.cpu_count())
    args = parser.parse_args()
    n_workers = min(args.workers, mp.cpu_count())

    with tempfile.TemporaryDirectory(prefix="depth_sweep_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        merged, bench = load_and_prepare(tmp_path)

    configs = build_sweep_configs()
    log.info("Sweep grid: %d configs (%d depth buckets × core params)",
             len(configs), len(DEPTH_BUCKETS))

    # ── Walk-forward stability ──
    stable = find_stable_configs(merged, configs, WALK_FORWARD_WINDOWS, n_workers)
    log.info("Stable configs: %d / %d", len(stable), len(configs))

    # ── Holdout test ──
    holdout = merged[merged["date"].apply(lambda d: HOLDOUT_START <= d <= HOLDOUT_END)].copy()
    log.info("Holdout: %s→%s (%d rows)", HOLDOUT_START, HOLDOUT_END, len(holdout))

    final = []
    for label, info in stable.items():
        cfg = next(c for c in configs if c["_label"] == label)
        res = run_backtest(holdout, cfg, trail_pct=cfg["_trail"], max_hold=cfg["_hold"])
        if res and res["xirr"] > 0:
            final.append({
                **info,
                "label": label,
                "holdout_xirr": res["xirr"],
                "holdout_dd": res["max_dd"],
                "holdout_sharpe": res["sharpe"],
                "holdout_trades": res["n_trades"],
                "holdout_wr": res["win_rate"],
            })

    if not final:
        print("\n⚠️  No configs survived walk-forward + holdout.")
        return

    df = pd.DataFrame(final).sort_values("holdout_sharpe", ascending=False)

    # ── Summary by depth bucket ──
    print("\n" + "=" * 100)
    print("DEPTH FILTER SWEEP RESULTS — VWAP Reclaim Reversal Strategy")
    print("=" * 100)
    print(f"Total configs: {len(configs)} | Stable: {len(stable)} | Holdout survivors: {len(final)}")

    print("\n┌─────────────────┬──────────┬────────┬─────────┬────────┬────────┬─────────────────────────────┐")
    print("│ Depth Bucket    │ # Stable │  XIRR  │ Max DD  │ Sharpe │ WinRate│  Best Config                │")
    print("├─────────────────┼──────────┼────────┼─────────┼────────┼────────┼─────────────────────────────┤")

    for bucket_name in DEPTH_BUCKETS:
        subset = df[df["bucket"] == bucket_name]
        if subset.empty:
            print(f"│ {bucket_name:15s} │    {'–':>4s}  │  {'–':>5s} │  {'–':>5s}  │  {'–':>4s}  │  {'–':>4s}  │ NO STABLE CONFIGS           │")
            continue
        best = subset.iloc[0]
        n_stable = len(subset)
        cfg_str = f"D{int(best['down_days'])} R{best['rvol']} T{best['trail']} H{int(best['hold'])}"
        print(f"│ {bucket_name:15s} │   {n_stable:>4d}   │{best['holdout_xirr']:6.1f}% │{best['holdout_dd']:6.1f}% │ {best['holdout_sharpe']:5.2f} │{best['holdout_wr']:5.1f}% │ {cfg_str:27s} │")

    print("└─────────────────┴──────────┴────────┴─────────┴────────┴────────┴─────────────────────────────┘")

    # ── Averages across all stable configs per bucket ──
    print("\n── Average across ALL stable configs per bucket ──")
    print(f"{'Bucket':17s}  {'Configs':>7s}  {'Avg XIRR':>9s}  {'Avg DD':>7s}  {'Avg Sharpe':>11s}  {'Avg WR':>7s}  {'Avg Trades':>10s}")
    print("─" * 75)
    for bucket_name in DEPTH_BUCKETS:
        subset = df[df["bucket"] == bucket_name]
        if subset.empty:
            continue
        print(f"{bucket_name:17s}  {len(subset):>7d}  "
              f"{subset['holdout_xirr'].mean():>8.1f}%  {subset['holdout_dd'].mean():>6.1f}%  "
              f"{subset['holdout_sharpe'].mean():>10.2f}  {subset['holdout_wr'].mean():>6.1f}%  "
              f"{subset['holdout_trades'].mean():>10.0f}")

    # ── Top 10 overall ──
    print("\n── Top 10 configs overall (sorted by Sharpe) ──")
    top10 = df.head(10)
    for _, r in top10.iterrows():
        print(f"  {r['label']:50s}  XIRR={r['holdout_xirr']:6.1f}%  DD={r['holdout_dd']:5.1f}%"
              f"  Sharpe={r['holdout_sharpe']:5.2f}  WR={r['holdout_wr']:5.1f}%"
              f"  Trades={r['holdout_trades']}")

    # ── Save full results ──
    out_path = ROOT / "results"
    out_path.mkdir(exist_ok=True)
    df.to_csv(out_path / "depth_filter_sweep.csv", index=False)
    print(f"\nFull results saved to results/depth_filter_sweep.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()
