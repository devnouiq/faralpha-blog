"""
Combined Sweet-Spot Sweep — Depth × Gap × Breadth
===================================================
Goal: Find the parameter combo that MAINTAINS ~2000%+ CAGR while
reducing drawdown and improving win rate.

Sweeps three filter axes simultaneously:
  1. Depth filter: depth_max (cap how deep a crash can be)
  2. Gap filter: max_gap_down (skip big gap-down days)
  3. Breadth filter: skip signals when market breadth is in choppy zone

All crossed with core params (down_days, rvol, trail, hold).

Usage:
    uv run python scripts/sweep_combined.py
    uv run python scripts/sweep_combined.py --workers 8
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

log = logging.getLogger("combined_sweep")
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
#  DATA LOADING
# ══════════════════════════════════════════════════════════

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

    # Breadth from regime table
    breadth = con.execute(
        "SELECT date, breadth_pct FROM regime WHERE market = 'india' ORDER BY date"
    ).df()
    con.close()
    log.info("Loaded %d daily rows, %d bench rows, %d breadth rows",
             len(daily), len(bench), len(breadth))

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
        grp["gap_pct"] = grp["open"] / grp["prev_close"] - 1

        for k in range(1, MAX_HOLD + 1):
            grp[f"fwd_close_{k}"] = close.shift(-k)
            grp[f"fwd_high_{k}"] = grp["high"].shift(-k)

        results.append(grp)

    daily_ctx = pd.concat(results, ignore_index=True)
    log.info("Daily features: %d rows", len(daily_ctx))

    # Merge
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

    log.info("Merged: %d rows, breadth coverage: %d/%d (%.0f%%)",
             len(merged),
             merged["breadth_pct"].notna().sum(), len(merged),
             merged["breadth_pct"].notna().mean() * 100)
    return merged, bench


# ══════════════════════════════════════════════════════════
#  SIGNAL GENERATION — combined filters
# ══════════════════════════════════════════════════════════

def generate_signals(merged, params):
    p = params.get("vwap_reclaim", {})
    has_vwap = merged["vwap_1h"].notna() & merged["close"].notna()

    sig = (
        has_vwap
        & (merged["down_days"] >= p.get("down_days", 3))
        & (merged["rvol_1h"] >= p.get("rvol_thresh", 1.5))
        & (merged["close"] > merged["vwap_1h"])
    )

    # ── Depth filter ──
    depth_max = p.get("depth_max")  # e.g., -0.25 = skip >25% drops
    if depth_max is not None:
        sig = sig & (merged["depth_pct"] >= depth_max)

    # ── Gap filter ──
    max_gap_down = p.get("max_gap_down")  # e.g., -0.03 = skip >3% gap-downs
    if max_gap_down is not None:
        sig = sig & (merged["gap_pct"] >= max_gap_down)

    # ── Breadth filter (skip choppy zone) ──
    breadth_skip_low = p.get("breadth_skip_low")
    breadth_skip_high = p.get("breadth_skip_high")
    if breadth_skip_low is not None and breadth_skip_high is not None:
        b = merged["breadth_pct"]
        in_skip_zone = b.notna() & (b >= breadth_skip_low) & (b <= breadth_skip_high)
        sig = sig & ~in_skip_zone

    # ── Breadth minimum (only trade when breadth is strong) ──
    breadth_min = p.get("breadth_min")
    if breadth_min is not None:
        sig = sig & ((merged["breadth_pct"].isna()) | (merged["breadth_pct"] >= breadth_min))

    # ── Breadth maximum (only trade in weak markets i.e. mean-reversion) ──
    breadth_max = p.get("breadth_max")
    if breadth_max is not None:
        sig = sig & ((merged["breadth_pct"].isna()) | (merged["breadth_pct"] <= breadth_max))

    return sig


def simulate_trades(signal_rows, trail_pct, max_hold, txn_cost=TXN_COST_PCT):
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
#  SWEEP GRID — depth × gap × breadth × core
# ══════════════════════════════════════════════════════════

# Filter options (None = disabled)
DEPTH_OPTS = [None, -0.20, -0.25, -0.30]           # depth_max
GAP_OPTS = [None, -0.02, -0.03, -0.05]             # max_gap_down
BREADTH_OPTS = [
    ("none", {}),                                                    # no breadth filter
    ("skip_30-50", {"breadth_skip_low": 0.30, "breadth_skip_high": 0.50}),  # current live
    ("skip_25-50", {"breadth_skip_low": 0.25, "breadth_skip_high": 0.50}),
    ("skip_30-55", {"breadth_skip_low": 0.30, "breadth_skip_high": 0.55}),
    ("skip_25-55", {"breadth_skip_low": 0.25, "breadth_skip_high": 0.55}),
    ("only_bear<30", {"breadth_max": 0.30}),                         # only trade when breadth < 30%
    ("only_bear<25", {"breadth_max": 0.25}),
    ("only_broad>50", {"breadth_min": 0.50}),                        # only trade when breadth > 50%
]

# Core params — focus on D3 R1.5 (live), but also test neighbours
CORE_PARAMS = [
    (3, 1.0), (3, 1.5), (3, 2.0),
    (4, 1.0), (4, 1.5),
    (5, 1.5),
]
TRAIL_OPTS = [0.02, 0.03]
HOLD_OPTS = [7, 10]


def build_sweep_configs():
    configs = []
    for dd, rvol in CORE_PARAMS:
        for trail in TRAIL_OPTS:
            for hold in HOLD_OPTS:
                for depth_max in DEPTH_OPTS:
                    for max_gap in GAP_OPTS:
                        for bname, bparams in BREADTH_OPTS:
                            vwap = {
                                "down_days": dd,
                                "rvol_thresh": rvol,
                                "bear_only": False,
                                **bparams,
                            }
                            if depth_max is not None:
                                vwap["depth_max"] = depth_max
                            if max_gap is not None:
                                vwap["max_gap_down"] = max_gap

                            depth_s = f"d{abs(int(depth_max*100))}" if depth_max else "dNone"
                            gap_s = f"g{abs(int(max_gap*100))}" if max_gap else "gNone"
                            label = (f"{depth_s}_{gap_s}_{bname}_"
                                     f"D{dd}_R{rvol}_T{trail}_H{hold}")

                            configs.append({
                                "vwap_reclaim": vwap,
                                "_trail": trail,
                                "_hold": hold,
                                "_label": label,
                                "_depth": depth_max,
                                "_gap": max_gap,
                                "_breadth": bname,
                            })
    return configs


def _eval_one(args):
    merged_slice, cfg = args
    try:
        res = run_backtest(
            merged_slice, cfg,
            trail_pct=cfg["_trail"],
            max_hold=cfg["_hold"],
        )
        if res:
            res["label"] = cfg["_label"]
            res["depth"] = cfg["_depth"]
            res["gap"] = cfg["_gap"]
            res["breadth_mode"] = cfg["_breadth"]
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
        log.info("WF %d: test %s→%s (%d rows)", wi, test_s, test_e, len(test_data))

        if n_workers > 1:
            with mp.Pool(n_workers) as pool:
                results = pool.map(_eval_one, [(test_data, c) for c in configs])
        else:
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
                "gap": wr[0]["gap"],
                "breadth_mode": wr[0]["breadth_mode"],
                "down_days": wr[0]["down_days"],
                "rvol": wr[0]["rvol"],
                "trail": wr[0]["trail"],
                "hold": wr[0]["hold"],
            }
    return stable


def main():
    parser = argparse.ArgumentParser(description="Combined depth × gap × breadth sweep")
    parser.add_argument("--workers", type=int, default=mp.cpu_count())
    args = parser.parse_args()
    n_workers = min(args.workers, mp.cpu_count())

    with tempfile.TemporaryDirectory(prefix="combined_sweep_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        merged, bench = load_and_prepare(tmp_path)

    configs = build_sweep_configs()
    log.info("Sweep grid: %d configs (%d depth × %d gap × %d breadth × %d core × %d trail × %d hold)",
             len(configs), len(DEPTH_OPTS), len(GAP_OPTS), len(BREADTH_OPTS),
             len(CORE_PARAMS), len(TRAIL_OPTS), len(HOLD_OPTS))

    stable = find_stable_configs(merged, configs, WALK_FORWARD_WINDOWS, n_workers)
    log.info("Stable configs: %d / %d", len(stable), len(configs))

    # ── Holdout ──
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

    df = pd.DataFrame(final)

    # ══════════════════════════════════════════════════════════
    #  Find baseline (no filter, D3 R1.5 T0.02 H10)
    # ══════════════════════════════════════════════════════════
    baseline_mask = (
        (df["depth"].isna()) & (df["gap"].isna()) & (df["breadth_mode"] == "none")
        & (df["down_days"] == 3) & (df["rvol"] == 1.5)
        & (df["trail"] == 0.02) & (df["hold"] == 10)
    )
    baseline_rows = df[baseline_mask]
    if baseline_rows.empty:
        # fallback: any no-filter D3 R1.5
        baseline_mask = (
            (df["depth"].isna()) & (df["gap"].isna()) & (df["breadth_mode"] == "none")
            & (df["down_days"] == 3) & (df["rvol"] == 1.5)
        )
        baseline_rows = df[baseline_mask]

    if not baseline_rows.empty:
        base = baseline_rows.iloc[0]
        base_xirr = base["holdout_xirr"]
        base_dd = base["holdout_dd"]
        base_wr = base["holdout_wr"]
        base_sharpe = base["holdout_sharpe"]
    else:
        base_xirr = df["holdout_xirr"].median()
        base_dd = df["holdout_dd"].median()
        base_wr = df["holdout_wr"].median()
        base_sharpe = df["holdout_sharpe"].median()

    # ══════════════════════════════════════════════════════════
    #  SWEET SPOT: maintain CAGR, reduce DD, improve WR
    # ══════════════════════════════════════════════════════════
    #  Score = reward for DD reduction + WR improvement - penalty for CAGR loss
    df["xirr_pct_of_base"] = df["holdout_xirr"] / base_xirr * 100
    df["dd_improvement"] = base_dd - df["holdout_dd"]  # positive = better (less negative DD)
    df["wr_improvement"] = df["holdout_wr"] - base_wr

    # Sweet spot score: Sharpe-like combo
    # Keep at least 80% of CAGR, then maximize DD+WR improvement
    df["sweet_score"] = np.where(
        df["xirr_pct_of_base"] >= 80,
        df["holdout_sharpe"] + df["dd_improvement"] * 0.5 + df["wr_improvement"] * 0.3,
        -999,  # penalize heavy CAGR loss
    )

    df = df.sort_values("sweet_score", ascending=False)

    # ══════════════════════════════════════════════════════════
    #  OUTPUT
    # ══════════════════════════════════════════════════════════
    print("\n" + "=" * 120)
    print("COMBINED SWEET-SPOT SWEEP — Depth × Gap × Breadth")
    print("=" * 120)
    print(f"Total configs: {len(configs)} | Stable: {len(stable)} | Holdout survivors: {len(final)}")
    print(f"\nBASELINE (D3 R1.5 T0.02 H10, no filters):")
    print(f"  XIRR={base_xirr:.1f}%  DD={base_dd:.1f}%  Sharpe={base_sharpe:.2f}  WR={base_wr:.1f}%")

    # ── Top 30 sweet-spot configs ──
    print(f"\n── Top 30 Sweet-Spot Configs (≥80% of baseline CAGR, best DD + WR improvement) ──")
    print(f"{'#':>3s}  {'Depth':>6s}  {'Gap':>5s}  {'Breadth':>12s}  {'D':>1s}{'R':>4s}{'T':>5s}{'H':>3s}"
          f"  {'XIRR':>8s}  {'%Base':>6s}  {'DD':>7s}  {'ΔDD':>5s}  {'Sharpe':>7s}  {'WR':>6s}  {'ΔWR':>5s}  {'Trades':>6s}  {'Score':>6s}")
    print("─" * 120)

    for i, (_, r) in enumerate(df.head(30).iterrows()):
        depth_s = f"{abs(int(r['depth']*100))}%" if pd.notna(r['depth']) else "—"
        gap_s = f"{abs(r['gap']*100):.0f}%" if pd.notna(r['gap']) else "—"
        dd_delta = f"{r['dd_improvement']:+.1f}" if r['dd_improvement'] != 0 else "  0.0"
        wr_delta = f"{r['wr_improvement']:+.1f}" if r['wr_improvement'] != 0 else "  0.0"
        is_live = " ◀ LIVE" if (pd.isna(r['depth']) and pd.isna(r['gap'])
                                 and r['breadth_mode'] == 'none'
                                 and r['down_days'] == 3 and r['rvol'] == 1.5
                                 and r['trail'] == 0.02 and r['hold'] == 10) else ""
        print(f"{i+1:>3d}  {depth_s:>6s}  {gap_s:>5s}  {r['breadth_mode']:>12s}  "
              f"D{int(r['down_days'])} R{r['rvol']:<3g} T{r['trail']:<4g} H{int(r['hold']):>2d}"
              f"  {r['holdout_xirr']:>7.1f}%  {r['xirr_pct_of_base']:>5.0f}%  "
              f"{r['holdout_dd']:>6.1f}%  {dd_delta:>5s}  {r['holdout_sharpe']:>6.2f}  "
              f"{r['holdout_wr']:>5.1f}%  {wr_delta:>5s}  {r['holdout_trades']:>6.0f}  "
              f"{r['sweet_score']:>5.1f}{is_live}")

    # ── Same core (D3 R1.5 T0.02 H10) comparison ──
    live_core = df[(df["down_days"] == 3) & (df["rvol"] == 1.5)
                   & (df["trail"] == 0.02) & (df["hold"] == 10)].copy()
    live_core = live_core.sort_values("sweet_score", ascending=False)

    print(f"\n── D3 R1.5 T0.02 H10 filter comparison (your live core params) ──")
    print(f"{'Depth':>6s}  {'Gap':>5s}  {'Breadth':>12s}  {'XIRR':>8s}  {'%Base':>6s}  {'DD':>7s}  {'ΔDD':>5s}  {'Sharpe':>7s}  {'WR':>6s}  {'ΔWR':>5s}  {'Trades':>6s}")
    print("─" * 95)
    for _, r in live_core.head(25).iterrows():
        depth_s = f"{abs(int(r['depth']*100))}%" if pd.notna(r['depth']) else "—"
        gap_s = f"{abs(r['gap']*100):.0f}%" if pd.notna(r['gap']) else "—"
        dd_delta = f"{r['dd_improvement']:+.1f}" if r['dd_improvement'] != 0 else "  0.0"
        wr_delta = f"{r['wr_improvement']:+.1f}" if r['wr_improvement'] != 0 else "  0.0"
        marker = " ◀" if (pd.isna(r['depth']) and pd.isna(r['gap']) and r['breadth_mode'] == 'none') else ""
        print(f"{depth_s:>6s}  {gap_s:>5s}  {r['breadth_mode']:>12s}  "
              f"{r['holdout_xirr']:>7.1f}%  {r['xirr_pct_of_base']:>5.0f}%  "
              f"{r['holdout_dd']:>6.1f}%  {dd_delta:>5s}  {r['holdout_sharpe']:>6.2f}  "
              f"{r['holdout_wr']:>5.1f}%  {wr_delta:>5s}  {r['holdout_trades']:>6.0f}{marker}")

    # ── Best per filter combo (averaged across core params) ──
    print(f"\n── Average metrics per filter combo (across all core params) ──")
    combo_key = df.groupby(["depth", "gap", "breadth_mode"]).agg(
        avg_xirr=("holdout_xirr", "mean"),
        avg_dd=("holdout_dd", "mean"),
        avg_sharpe=("holdout_sharpe", "mean"),
        avg_wr=("holdout_wr", "mean"),
        count=("holdout_xirr", "count"),
    ).reset_index()
    combo_key["xirr_pct"] = combo_key["avg_xirr"] / base_xirr * 100
    combo_key["dd_imp"] = base_dd - combo_key["avg_dd"]
    combo_key["wr_imp"] = combo_key["avg_wr"] - base_wr
    combo_key = combo_key[combo_key["xirr_pct"] >= 70].sort_values("avg_sharpe", ascending=False)

    print(f"{'Depth':>6s}  {'Gap':>5s}  {'Breadth':>12s}  {'N':>3s}  {'Avg XIRR':>9s}  {'%Base':>6s}  {'Avg DD':>7s}  {'ΔDD':>5s}  {'Sharpe':>7s}  {'Avg WR':>7s}  {'ΔWR':>5s}")
    print("─" * 100)
    for _, r in combo_key.head(25).iterrows():
        depth_s = f"{abs(int(r['depth']*100))}%" if pd.notna(r['depth']) else "—"
        gap_s = f"{abs(r['gap']*100):.0f}%" if pd.notna(r['gap']) else "—"
        print(f"{depth_s:>6s}  {gap_s:>5s}  {r['breadth_mode']:>12s}  {int(r['count']):>3d}  "
              f"{r['avg_xirr']:>8.1f}%  {r['xirr_pct']:>5.0f}%  "
              f"{r['avg_dd']:>6.1f}%  {r['dd_imp']:+5.1f}  {r['avg_sharpe']:>6.2f}  "
              f"{r['avg_wr']:>6.1f}%  {r['wr_imp']:+5.1f}")

    # Save
    out_path = ROOT / "results"
    out_path.mkdir(exist_ok=True)
    df.to_csv(out_path / "combined_sweep.csv", index=False)
    print(f"\nFull results saved to results/combined_sweep.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()
