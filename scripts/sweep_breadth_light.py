"""
Light-touch breadth reduce sweep
=================================
Goal: find breadth reduce config that preserves 85-90% of raw CAGR
while still improving DD/Sharpe.

Tests: factors 0.85–0.95, narrow zones, and zone × factor combos.
gap_5% is always on.
"""

import argparse
import logging
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

log = logging.getLogger("breadth_light")
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


def _copy_db(src, tmp_dir):
    dst = tmp_dir / src.name
    shutil.copy2(src, dst)
    wal = src.with_suffix(".duckdb.wal")
    if wal.exists():
        shutil.copy2(wal, tmp_dir / wal.name)
    return dst


def load_and_prepare(tmp_dir):
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
    p = params.get("vwap_reclaim", {})
    has_vwap = merged["vwap_1h"].notna() & merged["close"].notna()
    sig = (
        has_vwap
        & (merged["down_days"] >= p.get("down_days", 3))
        & (merged["rvol_1h"] >= p.get("rvol_thresh", 1.5))
        & (merged["close"] > merged["vwap_1h"])
        & (merged["gap_pct"] >= -0.05)
    )
    depth_max = p.get("depth_max")
    if depth_max is not None:
        sig = sig & (merged["depth_pct"] >= depth_max)
    return sig


def simulate_trades(signal_rows, trail_pct, max_hold,
                    breadth_lo, breadth_hi, breadth_factor):
    n = len(signal_rows)
    entry_prices = signal_rows["close"].values
    breadth_vals = signal_rows["breadth_pct"].values

    trade_rets = np.full(n, np.nan)
    size_factors = np.ones(n)

    if breadth_lo is not None and breadth_factor < 1.0:
        for i in range(n):
            b = breadth_vals[i]
            if not np.isnan(b) and breadth_lo <= b <= breadth_hi:
                size_factors[i] = breadth_factor

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
            if trail_pct > 0 and (close_d / peak - 1) <= -trail_pct:
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
        raw_ret = exit_price / entry - 1 - TXN_COST_PCT
        trade_rets[i] = raw_ret * size_factors[i]

    valid = ~np.isnan(trade_rets)
    return trade_rets, valid, size_factors


def run_backtest(merged, params, trail_pct, max_hold, max_positions,
                 breadth_lo, breadth_hi, breadth_factor):
    sig_mask = generate_signals(merged, params)
    needed = f"fwd_close_{max_hold}"
    if needed not in merged.columns:
        return None

    signal_rows = merged[sig_mask & merged[needed].notna()].copy()
    if signal_rows.empty or len(signal_rows) < MIN_TRADES:
        return None

    signal_rows = signal_rows.sort_values(["date", "rvol_1h"], ascending=[True, False])
    signal_rows["_rank"] = signal_rows.groupby("date").cumcount()
    signal_rows = signal_rows[signal_rows["_rank"] < max_positions]

    trade_rets, valid, size_factors = simulate_trades(
        signal_rows, trail_pct, max_hold, breadth_lo, breadth_hi, breadth_factor
    )
    valid_rows = signal_rows[valid].copy()
    valid_rets = trade_rets[valid]
    valid_sizes = size_factors[valid]

    if len(valid_rows) < MIN_TRADES:
        return None

    valid_rows["_ret"] = valid_rets
    daily_sig = valid_rows.groupby("date").agg(
        avg_ret=("_ret", "mean"), n_pos=("ticker", "count")
    ).reset_index().sort_values("date")

    all_dates = sorted(merged["date"].unique())
    full_cal = pd.DataFrame({"date": all_dates})
    full_cal = full_cal.merge(daily_sig[["date", "avg_ret"]], on="date", how="left")
    full_cal["avg_ret"] = full_cal["avg_ret"].fillna(0.0)
    full_cal["cum_ret"] = (1 + full_cal["avg_ret"]).cumprod()

    start_eq = 1_000_000
    end_eq = full_cal["cum_ret"].iloc[-1] * start_eq
    date_col = pd.to_datetime(full_cal["date"])
    years = max((date_col.iloc[-1] - date_col.iloc[0]).days / 365.25, 0.01)
    xirr = (end_eq / start_eq) ** (1 / years) - 1

    cum_peak = full_cal["cum_ret"].cummax()
    dd = (full_cal["cum_ret"] - cum_peak) / cum_peak
    max_dd = dd.min()

    rets = full_cal["avg_ret"]
    sharpe = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0

    raw_rets_valid = valid_rets / np.where(valid_sizes > 0, valid_sizes, 1)
    win_rate = (raw_rets_valid > 0).mean()

    return {
        "xirr": round(xirr * 100, 1),
        "max_dd": round(max_dd * 100, 1),
        "sharpe": round(sharpe, 2),
        "n_trades": len(valid_rows),
        "win_rate": round(win_rate * 100, 1),
        "avg_size_factor": round(valid_sizes.mean(), 3),
    }


# ══════════════════════════════════════════════════════════
#  SWEEP GRID — light-touch options
# ══════════════════════════════════════════════════════════

# (name, lo, hi, factor)
BREADTH_OPTS = [
    # Baseline
    ("none",              None, None, 1.0),
    # Very light: 0.95x
    ("reduce_30-55_0.95", 0.30, 0.55, 0.95),
    ("reduce_30-50_0.95", 0.30, 0.50, 0.95),
    ("reduce_35-55_0.95", 0.35, 0.55, 0.95),
    ("reduce_35-50_0.95", 0.35, 0.50, 0.95),
    ("reduce_40-55_0.95", 0.40, 0.55, 0.95),
    # Light: 0.90x
    ("reduce_30-55_0.90", 0.30, 0.55, 0.90),
    ("reduce_30-50_0.90", 0.30, 0.50, 0.90),
    ("reduce_35-55_0.90", 0.35, 0.55, 0.90),
    ("reduce_35-50_0.90", 0.35, 0.50, 0.90),
    ("reduce_40-55_0.90", 0.40, 0.55, 0.90),
    ("reduce_40-50_0.90", 0.40, 0.50, 0.90),
    # Medium: 0.85x
    ("reduce_30-55_0.85", 0.30, 0.55, 0.85),
    ("reduce_30-50_0.85", 0.30, 0.50, 0.85),
    ("reduce_35-55_0.85", 0.35, 0.55, 0.85),
    ("reduce_35-50_0.85", 0.35, 0.50, 0.85),
    ("reduce_40-55_0.85", 0.40, 0.55, 0.85),
    ("reduce_40-50_0.85", 0.40, 0.50, 0.85),
    # Previous best for reference
    ("reduce_30-55_0.80", 0.30, 0.55, 0.80),
    ("reduce_30-55_0.60", 0.30, 0.55, 0.60),
]

# Core: live config only (no need to sweep params, we want breadth tuning)
CORE_PARAMS = [
    (3, 1.5, 0.02, 10),  # current live
]


def build_configs():
    configs = []
    for dd, rvol, trail, hold in CORE_PARAMS:
        for bname, blo, bhi, bfactor in BREADTH_OPTS:
            vwap = {"down_days": dd, "rvol_thresh": rvol, "bear_only": False}
            configs.append({
                "vwap_reclaim": vwap,
                "_trail": trail, "_hold": hold,
                "_label": f"{bname}_D{dd}_R{rvol}_T{trail}_H{hold}",
                "_bname": bname, "_blo": blo, "_bhi": bhi, "_bfactor": bfactor,
            })
    return configs


def main():
    with tempfile.TemporaryDirectory(prefix="breadth_light_") as tmp_dir:
        merged = load_and_prepare(Path(tmp_dir))

    configs = build_configs()
    log.info("Sweep: %d configs", len(configs))

    # Walk-forward stability
    window_results = {}
    for wi, (ts, te, vs, ve) in enumerate(WALK_FORWARD_WINDOWS):
        test = merged[merged["date"].apply(lambda d: vs <= d <= ve)].copy()
        log.info("WF %d: %s→%s (%d rows)", wi, vs, ve, len(test))
        for cfg in configs:
            res = run_backtest(test, cfg, cfg["_trail"], cfg["_hold"], 5,
                               cfg["_blo"], cfg["_bhi"], cfg["_bfactor"])
            if res:
                label = cfg["_label"]
                window_results.setdefault(label, []).append(res)

    stable_labels = set()
    for label, wr in window_results.items():
        if len(wr) == len(WALK_FORWARD_WINDOWS) and all(r["xirr"] > 0 for r in wr):
            stable_labels.add(label)
    log.info("Stable: %d / %d", len(stable_labels), len(configs))

    # Holdout
    holdout = merged[merged["date"].apply(lambda d: HOLDOUT_START <= d <= HOLDOUT_END)].copy()
    log.info("Holdout: %d rows", len(holdout))

    results = []
    for cfg in configs:
        if cfg["_label"] not in stable_labels:
            continue
        res = run_backtest(holdout, cfg, cfg["_trail"], cfg["_hold"], 5,
                           cfg["_blo"], cfg["_bhi"], cfg["_bfactor"])
        if res and res["xirr"] > 0:
            results.append({
                **res,
                "label": cfg["_label"],
                "breadth_mode": cfg["_bname"],
                "factor": cfg["_bfactor"],
                "zone_lo": cfg["_blo"],
                "zone_hi": cfg["_bhi"],
            })

    if not results:
        print("\n⚠️  No configs survived.")
        return

    df = pd.DataFrame(results)

    # Get raw baseline (2189% from no-filter sweep)
    RAW_XIRR = 2189.0
    base_row = df[df["breadth_mode"] == "none"]
    gap_only_xirr = base_row.iloc[0]["xirr"] if not base_row.empty else df["xirr"].max()
    gap_only_dd = base_row.iloc[0]["max_dd"] if not base_row.empty else 0
    gap_only_sharpe = base_row.iloc[0]["sharpe"] if not base_row.empty else 0

    df["pct_of_raw"] = df["xirr"] / RAW_XIRR * 100
    df["pct_of_gap"] = df["xirr"] / gap_only_xirr * 100
    df["dd_vs_gap"] = gap_only_dd - df["max_dd"]

    # Sort by Sharpe descending
    df = df.sort_values("sharpe", ascending=False)

    print("\n" + "=" * 140)
    print("LIGHT-TOUCH BREADTH REDUCE — gap_5% always on, D3 R1.5 T0.02 H10")
    print("=" * 140)
    print(f"RAW baseline (no filters): XIRR=2189%")
    print(f"GAP_5% baseline: XIRR={gap_only_xirr:.1f}%  DD={gap_only_dd:.1f}%  Sharpe={gap_only_sharpe:.2f}")
    print(f"\nTarget: 85-90% of raw = {RAW_XIRR*0.85:.0f}–{RAW_XIRR*0.90:.0f}% XIRR")
    print()

    print(f"{'Breadth Mode':>25s}  {'Factor':>6s}  {'Zone':>10s}  "
          f"{'XIRR':>8s}  {'%Raw':>5s}  {'%Gap':>5s}  "
          f"{'DD':>7s}  {'ΔDD':>6s}  {'Sharpe':>7s}  "
          f"{'WR':>6s}  {'Trades':>6s}  {'AvgSz':>5s}  {'Verdict':>10s}")
    print("─" * 140)

    for _, r in df.iterrows():
        zone = f"{r['zone_lo']:.0%}-{r['zone_hi']:.0%}" if r['zone_lo'] is not None else "—"
        dd_delta = f"{r['dd_vs_gap']:+.1f}" if r['dd_vs_gap'] != 0 else "  0.0"

        # Verdict
        pct_raw = r["pct_of_raw"]
        if pct_raw >= 90:
            verdict = "★★★"
        elif pct_raw >= 85:
            verdict = "★★ sweet"
        elif pct_raw >= 80:
            verdict = "★ ok"
        else:
            verdict = "—"

        # Highlight if DD improved AND CAGR >= 85% raw
        marker = ""
        if r["dd_vs_gap"] < -0.5 and pct_raw >= 85:
            marker = " ◀ BEST"
        elif r["breadth_mode"] == "none":
            marker = " ◀ base"

        print(f"{r['breadth_mode']:>25s}  {r['factor']:>5.2f}x  {zone:>10s}  "
              f"{r['xirr']:>7.1f}%  {pct_raw:>4.0f}%  {r['pct_of_gap']:>4.0f}%  "
              f"{r['max_dd']:>6.1f}%  {dd_delta:>6s}  {r['sharpe']:>6.2f}  "
              f"{r['win_rate']:>5.1f}%  {r['n_trades']:>6.0f}  "
              f"{r['avg_size_factor']:>4.2f}  {verdict}{marker}")

    # Summary: best configs meeting 85% threshold
    meets = df[df["pct_of_raw"] >= 85].copy()
    if not meets.empty:
        best = meets.sort_values("sharpe", ascending=False).iloc[0]
        print(f"\n🏆 RECOMMENDED: {best['breadth_mode']} "
              f"(factor={best['factor']:.2f}, zone={best['zone_lo']}-{best['zone_hi']})")
        print(f"   XIRR={best['xirr']:.1f}% ({best['pct_of_raw']:.0f}% of raw)  "
              f"DD={best['max_dd']:.1f}% ({best['dd_vs_gap']:+.1f} vs gap-only)  "
              f"Sharpe={best['sharpe']:.2f}  WR={best['win_rate']:.1f}%")

    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    df.to_csv(out / "breadth_light_sweep.csv", index=False)
    print(f"\nSaved to results/breadth_light_sweep.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()
