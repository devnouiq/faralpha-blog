"""
Breadth Reduced-Size Sweep
===========================
Instead of SKIPPING signals when breadth is in the choppy zone (which kills CAGR),
test using REDUCED position size (0.4x–1.0x) in the choppy zone.

Also tests interaction with depth_30% filter (optional add-on for scaling capital).

Fixed params: gap_5% (already applied as primary filter).

Usage:
    uv run python scripts/sweep_breadth_reduce.py
    uv run python scripts/sweep_breadth_reduce.py --workers 8
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

log = logging.getLogger("breadth_reduce_sweep")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
)

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


def _copy_db(src: Path, tmp_dir: Path) -> Path:
    dst = tmp_dir / src.name
    shutil.copy2(src, dst)
    wal = src.with_suffix(".duckdb.wal")
    if wal.exists():
        shutil.copy2(wal, tmp_dir / wal.name)
    return dst


def load_and_prepare(tmp_dir: Path):
    market_db = _copy_db(ROOT / "db" / "market.duckdb", tmp_dir)
    intra_db = _copy_db(ROOT / "db" / "intraday.duckdb", tmp_dir)

    con = duckdb.connect(str(market_db), read_only=True)
    daily = con.execute(
        "SELECT date, ticker, open, high, low, close, volume FROM prices ORDER BY ticker, date"
    ).df()
    bench = con.execute("SELECT date, close AS bench_close FROM benchmark ORDER BY date").df()
    breadth = con.execute(
        "SELECT date, breadth_pct FROM regime WHERE market = 'india' ORDER BY date"
    ).df()
    con.close()
    log.info("Loaded %d daily, %d bench, %d breadth rows", len(daily), len(bench), len(breadth))

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

    intra_feat = intra_feat.sort_values(["ticker", "date"])
    intra_feat["rvol_1h"] = intra_feat.groupby("ticker")["fh_vol"].transform(
        lambda x: x / x.rolling(20, min_periods=5).mean()
    )

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
        grp["gap_pct"] = grp["open"] / grp["prev_close"] - 1

        for k in range(1, MAX_HOLD + 1):
            grp[f"fwd_close_{k}"] = close.shift(-k)
            grp[f"fwd_high_{k}"] = grp["high"].shift(-k)

        results.append(grp)

    daily_ctx = pd.concat(results, ignore_index=True)
    log.info("Daily features: %d rows", len(daily_ctx))

    daily_ctx["date"] = pd.to_datetime(daily_ctx["date"]).dt.date
    intra_feat["date"] = pd.to_datetime(intra_feat["date"]).dt.date
    bench["date"] = pd.to_datetime(bench["date"]).dt.date
    breadth["date"] = pd.to_datetime(breadth["date"]).dt.date

    bench = bench.sort_values("date")
    bench["bench_ma200"] = bench["bench_close"].rolling(200).mean()
    bench["is_bear"] = bench["bench_close"] < bench["bench_ma200"]

    merged = daily_ctx.merge(intra_feat, on=["date", "ticker"], how="inner",
                             suffixes=("", "_intra"))
    merged = merged.merge(bench[["date", "is_bear"]], on="date", how="left")
    merged = merged.merge(breadth[["date", "breadth_pct"]], on="date", how="left")
    merged = merged[merged["day_volume"] >= 50000].copy()

    log.info("Merged: %d rows, breadth coverage: %.0f%%",
             len(merged), merged["breadth_pct"].notna().mean() * 100)
    return merged


def generate_signals(merged, params):
    """Generate signals with gap_5% baked in (primary filter)."""
    p = params.get("vwap_reclaim", {})
    has_vwap = merged["vwap_1h"].notna() & merged["close"].notna()

    sig = (
        has_vwap
        & (merged["down_days"] >= p.get("down_days", 3))
        & (merged["rvol_1h"] >= p.get("rvol_thresh", 1.5))
        & (merged["close"] > merged["vwap_1h"])
        & (merged["gap_pct"] >= -0.05)  # gap_5% always on
    )

    depth_max = p.get("depth_max")
    if depth_max is not None:
        sig = sig & (merged["depth_pct"] >= depth_max)

    return sig


def simulate_trades_with_breadth_sizing(signal_rows, trail_pct, max_hold,
                                         breadth_reduce_lo, breadth_reduce_hi,
                                         breadth_reduce_factor,
                                         txn_cost=TXN_COST_PCT):
    """
    Trailing stop simulator with breadth-based position sizing.
    Returns are weighted by position size factor.
    """
    n = len(signal_rows)
    entry_prices = signal_rows["close"].values
    breadth_vals = signal_rows["breadth_pct"].values

    trade_rets = np.full(n, np.nan)
    size_factors = np.ones(n)

    # Compute size factors based on breadth
    if breadth_reduce_lo is not None and breadth_reduce_factor < 1.0:
        for i in range(n):
            b = breadth_vals[i]
            if not np.isnan(b) and breadth_reduce_lo <= b <= breadth_reduce_hi:
                size_factors[i] = breadth_reduce_factor

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
        # Trade return weighted by size factor
        raw_ret = exit_price / entry - 1 - txn_cost
        trade_rets[i] = raw_ret * size_factors[i]

    valid = ~np.isnan(trade_rets)
    raw_rets = np.where(valid, (np.column_stack([signal_rows["close"].values])[:, 0] * 0 +
                                 trade_rets / np.where(size_factors > 0, size_factors, 1)), np.nan)
    return trade_rets, valid, size_factors


def run_backtest(merged, params, trail_pct, max_hold, max_positions,
                 breadth_reduce_lo, breadth_reduce_hi, breadth_reduce_factor):
    sig_mask = generate_signals(merged, params)
    needed_col = f"fwd_close_{max_hold}"
    if needed_col not in merged.columns:
        return None

    signal_rows = merged[sig_mask & merged[needed_col].notna()].copy()
    if signal_rows.empty or len(signal_rows) < MIN_TRADES:
        return None

    signal_rows = signal_rows.sort_values(["date", "rvol_1h"], ascending=[True, False])
    signal_rows["_rank"] = signal_rows.groupby("date").cumcount()
    signal_rows = signal_rows[signal_rows["_rank"] < max_positions]

    trade_rets, valid, size_factors = simulate_trades_with_breadth_sizing(
        signal_rows, trail_pct, max_hold,
        breadth_reduce_lo, breadth_reduce_hi, breadth_reduce_factor,
    )
    valid_rows = signal_rows[valid].copy()
    valid_rets = trade_rets[valid]
    valid_sizes = size_factors[valid]

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

    # Win rate on raw (unweighted) returns
    raw_rets_valid = valid_rets / np.where(valid_sizes > 0, valid_sizes, 1)
    win_rate = (raw_rets_valid > 0).mean()

    # Effective avg position size
    avg_size_factor = valid_sizes.mean()

    return {
        "xirr": round(xirr * 100, 1),
        "max_dd": round(max_dd * 100, 1),
        "sharpe": round(sharpe, 2),
        "n_trades": len(valid_rows),
        "win_rate": round(win_rate * 100, 1),
        "avg_size_factor": round(avg_size_factor, 3),
    }


# ══════════════════════════════════════════════════════════
#  SWEEP GRID
# ══════════════════════════════════════════════════════════

# Breadth reduce options: (name, lo, hi, factor)
BREADTH_OPTS = [
    ("none",          None, None, 1.0),    # no breadth filter at all
    ("reduce_30-50_1.0", 0.30, 0.50, 1.0), # zone defined but full size (=no effect)
    ("reduce_30-50_0.8", 0.30, 0.50, 0.8),
    ("reduce_30-50_0.6", 0.30, 0.50, 0.6),
    ("reduce_30-50_0.5", 0.30, 0.50, 0.5),
    ("reduce_30-50_0.4", 0.30, 0.50, 0.4),
    ("reduce_30-55_0.8", 0.30, 0.55, 0.8),
    ("reduce_30-55_0.6", 0.30, 0.55, 0.6),
    ("reduce_30-55_0.5", 0.30, 0.55, 0.5),
    ("reduce_30-55_0.4", 0.30, 0.55, 0.4),
    ("reduce_25-55_0.8", 0.25, 0.55, 0.8),
    ("reduce_25-55_0.6", 0.25, 0.55, 0.6),
    ("reduce_25-55_0.5", 0.25, 0.55, 0.5),
    ("reduce_25-50_0.6", 0.25, 0.50, 0.6),
    ("reduce_25-50_0.5", 0.25, 0.50, 0.5),
    ("skip_30-50",    0.30, 0.50, 0.0),    # full skip (old behavior) for comparison
    ("skip_30-55",    0.30, 0.55, 0.0),
]

DEPTH_OPTS = [None, -0.30]  # None = no depth filter, -0.30 = cap at 30%

# Core params — focus on the live config neighborhood
CORE_PARAMS = [
    (3, 1.5, 0.02, 10),  # current live
    (3, 1.5, 0.02, 7),
    (3, 1.0, 0.02, 10),
    (3, 1.0, 0.02, 7),
    (3, 1.5, 0.03, 10),
    (3, 1.0, 0.03, 10),
]


def build_sweep_configs():
    configs = []
    for dd, rvol, trail, hold in CORE_PARAMS:
        for depth_max in DEPTH_OPTS:
            for bname, blo, bhi, bfactor in BREADTH_OPTS:
                vwap = {
                    "down_days": dd,
                    "rvol_thresh": rvol,
                    "bear_only": False,
                }
                if depth_max is not None:
                    vwap["depth_max"] = depth_max

                depth_s = f"d{abs(int(depth_max*100))}" if depth_max else "dNone"
                label = f"{depth_s}_{bname}_D{dd}_R{rvol}_T{trail}_H{hold}"

                configs.append({
                    "vwap_reclaim": vwap,
                    "_trail": trail,
                    "_hold": hold,
                    "_label": label,
                    "_depth": depth_max,
                    "_breadth_name": bname,
                    "_breadth_lo": blo,
                    "_breadth_hi": bhi,
                    "_breadth_factor": bfactor,
                })
    return configs


def _eval_one(args):
    merged_slice, cfg = args
    try:
        res = run_backtest(
            merged_slice, cfg,
            trail_pct=cfg["_trail"],
            max_hold=cfg["_hold"],
            max_positions=5,
            breadth_reduce_lo=cfg["_breadth_lo"],
            breadth_reduce_hi=cfg["_breadth_hi"],
            breadth_reduce_factor=cfg["_breadth_factor"],
        )
        if res:
            res["label"] = cfg["_label"]
            res["depth"] = cfg["_depth"]
            res["breadth_mode"] = cfg["_breadth_name"]
            res["breadth_factor"] = cfg["_breadth_factor"]
            res["down_days"] = cfg["vwap_reclaim"]["down_days"]
            res["rvol"] = cfg["vwap_reclaim"]["rvol_thresh"]
            res["trail"] = cfg["_trail"]
            res["hold"] = cfg["_hold"]
        return res
    except Exception:
        return None


def find_stable_configs(merged, configs, windows, n_workers):
    window_results = {}

    for wi, (train_s, train_e, test_s, test_e) in enumerate(windows):
        test_dates = merged["date"].apply(lambda d: test_s <= d <= test_e)
        test_data = merged[test_dates].copy()
        log.info("WF %d: test %s→%s (%d rows, %d configs)",
                 wi, test_s, test_e, len(test_data), len(configs))

        # Single-threaded: avoids massive DataFrame pickling overhead
        results = [_eval_one((test_data, c)) for c in configs]

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
                "depth": wr[0]["depth"],
                "breadth_mode": wr[0]["breadth_mode"],
                "breadth_factor": wr[0]["breadth_factor"],
                "down_days": wr[0]["down_days"],
                "rvol": wr[0]["rvol"],
                "trail": wr[0]["trail"],
                "hold": wr[0]["hold"],
            }
    return stable


def main():
    parser = argparse.ArgumentParser(description="Breadth reduced-size sweep")
    parser.add_argument("--workers", type=int, default=mp.cpu_count())
    args = parser.parse_args()
    n_workers = min(args.workers, mp.cpu_count())

    with tempfile.TemporaryDirectory(prefix="breadth_reduce_") as tmp_dir:
        merged = load_and_prepare(Path(tmp_dir))

    configs = build_sweep_configs()
    log.info("Sweep grid: %d configs (%d breadth × %d depth × %d core)",
             len(configs), len(BREADTH_OPTS), len(DEPTH_OPTS), len(CORE_PARAMS))

    stable = find_stable_configs(merged, configs, WALK_FORWARD_WINDOWS, n_workers)
    log.info("Stable configs: %d / %d", len(stable), len(configs))

    holdout = merged[merged["date"].apply(lambda d: HOLDOUT_START <= d <= HOLDOUT_END)].copy()
    log.info("Holdout: %s→%s (%d rows)", HOLDOUT_START, HOLDOUT_END, len(holdout))

    final = []
    for label, info in stable.items():
        cfg = next(c for c in configs if c["_label"] == label)
        res = run_backtest(
            holdout, cfg,
            trail_pct=cfg["_trail"],
            max_hold=cfg["_hold"],
            max_positions=5,
            breadth_reduce_lo=cfg["_breadth_lo"],
            breadth_reduce_hi=cfg["_breadth_hi"],
            breadth_reduce_factor=cfg["_breadth_factor"],
        )
        if res and res["xirr"] > 0:
            final.append({
                **info,
                "label": label,
                "holdout_xirr": res["xirr"],
                "holdout_dd": res["max_dd"],
                "holdout_sharpe": res["sharpe"],
                "holdout_trades": res["n_trades"],
                "holdout_wr": res["win_rate"],
                "holdout_avg_size": res["avg_size_factor"],
            })

    if not final:
        print("\n⚠️  No configs survived walk-forward + holdout.")
        return

    df = pd.DataFrame(final)

    # ── Baseline: gap_5% only, no breadth, no depth, D3 R1.5 T0.02 H10 ──
    base_mask = (
        (df["depth"].isna()) & (df["breadth_mode"] == "none")
        & (df["down_days"] == 3) & (df["rvol"] == 1.5)
        & (df["trail"] == 0.02) & (df["hold"] == 10)
    )
    base_rows = df[base_mask]
    if not base_rows.empty:
        base = base_rows.iloc[0]
        base_xirr = base["holdout_xirr"]
        base_dd = base["holdout_dd"]
        base_wr = base["holdout_wr"]
        base_sharpe = base["holdout_sharpe"]
    else:
        base_xirr = df["holdout_xirr"].median()
        base_dd = df["holdout_dd"].median()
        base_wr = df["holdout_wr"].median()
        base_sharpe = df["holdout_sharpe"].median()

    df["xirr_pct_of_base"] = df["holdout_xirr"] / base_xirr * 100
    df["dd_improvement"] = base_dd - df["holdout_dd"]  # positive = less DD
    df["wr_improvement"] = df["holdout_wr"] - base_wr

    # ══════════════════════════════════════════════════════════
    #  OUTPUT
    # ══════════════════════════════════════════════════════════
    print("\n" + "=" * 130)
    print("BREADTH REDUCED-SIZE SWEEP — gap_5% always on")
    print("=" * 130)
    print(f"Total configs: {len(configs)} | Stable: {len(stable)} | Holdout survivors: {len(final)}")
    print(f"\nBASELINE (gap_5%, no breadth reduce, no depth, D3 R1.5 T0.02 H10):")
    print(f"  XIRR={base_xirr:.1f}%  DD={base_dd:.1f}%  Sharpe={base_sharpe:.2f}  WR={base_wr:.1f}%")

    # ── D3 R1.5 T0.02 H10 comparison (live core) ──
    live_core = df[(df["down_days"] == 3) & (df["rvol"] == 1.5)
                   & (df["trail"] == 0.02) & (df["hold"] == 10)].copy()
    live_core = live_core.sort_values("holdout_sharpe", ascending=False)

    print(f"\n── D3 R1.5 T0.02 H10 — Breadth Reduce × Depth (live core params) ──")
    print(f"{'Depth':>6s}  {'Breadth Mode':>22s}  {'Factor':>6s}  "
          f"{'XIRR':>8s}  {'%Base':>6s}  {'DD':>7s}  {'ΔDD':>5s}  {'Sharpe':>7s}  "
          f"{'WR':>6s}  {'ΔWR':>5s}  {'Trades':>6s}  {'AvgSz':>5s}")
    print("─" * 115)
    for _, r in live_core.iterrows():
        depth_s = f"{abs(int(r['depth']*100))}%" if pd.notna(r['depth']) else "—"
        dd_d = f"{r['dd_improvement']:+.1f}" if r['dd_improvement'] != 0 else "  0.0"
        wr_d = f"{r['wr_improvement']:+.1f}" if r['wr_improvement'] != 0 else "  0.0"
        marker = " ◀" if (pd.isna(r['depth']) and r['breadth_mode'] == 'none') else ""
        print(f"{depth_s:>6s}  {r['breadth_mode']:>22s}  {r['breadth_factor']:>5.1f}x  "
              f"{r['holdout_xirr']:>7.1f}%  {r['xirr_pct_of_base']:>5.0f}%  "
              f"{r['holdout_dd']:>6.1f}%  {dd_d:>5s}  {r['holdout_sharpe']:>6.2f}  "
              f"{r['holdout_wr']:>5.1f}%  {wr_d:>5s}  {r['holdout_trades']:>6.0f}  "
              f"{r['holdout_avg_size']:>4.2f}{marker}")

    # ── Summary: avg across all core params per breadth mode × depth ──
    print(f"\n── Average metrics per (depth, breadth_mode) across all core params ──")
    combo = df.groupby(["depth", "breadth_mode", "breadth_factor"]).agg(
        avg_xirr=("holdout_xirr", "mean"),
        avg_dd=("holdout_dd", "mean"),
        avg_sharpe=("holdout_sharpe", "mean"),
        avg_wr=("holdout_wr", "mean"),
        n=("holdout_xirr", "count"),
    ).reset_index()
    combo["xirr_pct"] = combo["avg_xirr"] / base_xirr * 100
    combo["dd_imp"] = base_dd - combo["avg_dd"]
    combo["wr_imp"] = combo["avg_wr"] - base_wr
    combo = combo.sort_values("avg_sharpe", ascending=False)

    print(f"{'Depth':>6s}  {'Breadth Mode':>22s}  {'Factor':>6s}  {'N':>3s}  "
          f"{'Avg XIRR':>9s}  {'%Base':>6s}  {'Avg DD':>7s}  {'ΔDD':>5s}  "
          f"{'Sharpe':>7s}  {'Avg WR':>7s}  {'ΔWR':>5s}")
    print("─" * 110)
    for _, r in combo.iterrows():
        depth_s = f"{abs(int(r['depth']*100))}%" if pd.notna(r['depth']) else "—"
        print(f"{depth_s:>6s}  {r['breadth_mode']:>22s}  {r['breadth_factor']:>5.1f}x  "
              f"{int(r['n']):>3d}  {r['avg_xirr']:>8.1f}%  {r['xirr_pct']:>5.0f}%  "
              f"{r['avg_dd']:>6.1f}%  {r['dd_imp']:+5.1f}  {r['avg_sharpe']:>6.2f}  "
              f"{r['avg_wr']:>6.1f}%  {r['wr_imp']:+5.1f}")

    # ── Top 20 by Sharpe ──
    top = df.sort_values("holdout_sharpe", ascending=False).head(20)
    print(f"\n── Top 20 configs by Sharpe ──")
    for _, r in top.iterrows():
        depth_s = f"d{abs(int(r['depth']*100))}" if pd.notna(r['depth']) else "d—"
        print(f"  {depth_s} {r['breadth_mode']:>22s}  D{int(r['down_days'])} R{r['rvol']} "
              f"T{r['trail']} H{int(r['hold'])}  "
              f"XIRR={r['holdout_xirr']:>7.1f}% ({r['xirr_pct_of_base']:>3.0f}%)  "
              f"DD={r['holdout_dd']:>6.1f}%  Sharpe={r['holdout_sharpe']:>5.2f}  "
              f"WR={r['holdout_wr']:>5.1f}%  Trades={r['holdout_trades']}")

    out_path = ROOT / "results"
    out_path.mkdir(exist_ok=True)
    df.to_csv(out_path / "breadth_reduce_sweep.csv", index=False)
    print(f"\nFull results saved to results/breadth_reduce_sweep.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()
