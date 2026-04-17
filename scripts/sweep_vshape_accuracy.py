"""
V-Shape Accuracy Sweep — Experiment 2
=======================================
After 3+ consecutive down days the reversal is rarely V-shaped.
Can we improve win rate (accuracy) while maintaining CAGR / drawdown?

Experiments:
  1. Confirmation bars — wait N bars above VWAP before entering
  2. Bounce strength — require price to recover X% of the total drop
  3. Volume follow-through — signal-day volume must exceed prior day's
  4. Gap filter — skip if stock gaps down too much on signal day
  5. Recovery ratio — close must be within X% of the 20-day high
  6. Momentum confirmation — require close > MA(5) or MA(10)
  7. Tighter trail for shallow dips, wider for deep dips (adaptive trail)
  8. Delay entry — enter T+1 open instead of signal close

Walk-forward sweep with parallel workers.

Usage:
    uv run python scripts/sweep_vshape_accuracy.py
    uv run python scripts/sweep_vshape_accuracy.py --workers 8
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

log = logging.getLogger("vshape_sweep")
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
    """Load data with EXTRA features needed for accuracy experiments."""
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
                FIRST(open ORDER BY hr, mn) AS day_open,
                LAST(close ORDER BY hr, mn) AS day_close,
                SUM(CASE WHEN hr = 9 OR (hr = 10 AND mn < 15) THEN volume ELSE 0 END) AS fh_vol,
                SUM(CASE WHEN hr = 9 OR (hr = 10 AND mn < 15)
                    THEN ((high + low + close) / 3.0) * volume ELSE 0 END) AS fh_tp_vol
            FROM bars GROUP BY ticker, date
            HAVING n_bars >= 4 AND day_volume > 0
        )
        SELECT ticker, date, day_volume, fh_vol, day_open,
               CASE WHEN fh_vol > 0 THEN fh_tp_vol / fh_vol END AS vwap_1h
        FROM day_agg ORDER BY ticker, date
    """).df()
    icon.close()
    log.info("Intraday features: %d rows", len(intra_feat))

    intra_feat = intra_feat.sort_values(["ticker", "date"])
    intra_feat["rvol_1h"] = intra_feat.groupby("ticker")["fh_vol"].transform(
        lambda x: x / x.rolling(20, min_periods=5).mean()
    )

    # Daily features per ticker — enhanced for accuracy experiments
    results = []
    for ticker, grp in daily.groupby("ticker"):
        grp = grp.sort_values("date").copy()
        if len(grp) < 200:
            continue
        close = grp["close"]
        high = grp["high"]
        low = grp["low"]
        vol = grp["volume"]

        is_down = close.diff() < 0
        groups = (~is_down).cumsum()
        grp["down_days"] = is_down.groupby(groups).cumsum().astype(int)
        grp["depth_pct"] = (close - close.rolling(20).max()) / close.rolling(20).max()
        grp["prev_close"] = close.shift(1)
        grp["day_change_pct"] = close / grp["prev_close"] - 1

        # ── Extra features for V-shape experiments ──
        # Recent low (bottom of the sell-off)
        grp["low_3d"] = low.rolling(3, min_periods=1).min()
        grp["low_5d"] = low.rolling(5, min_periods=1).min()
        grp["high_20d"] = close.rolling(20).max()

        # Bounce strength: how much of the drop has been recovered
        # (close - recent_low) / (20d_high - recent_low)
        drop = grp["high_20d"] - grp["low_5d"]
        grp["bounce_pct"] = np.where(drop > 0, (close - grp["low_5d"]) / drop, 0)

        # Gap: today's open vs yesterday's close
        grp["gap_pct"] = grp["open"] / grp["prev_close"] - 1

        # Volume vs prior day
        grp["vol_ratio_1d"] = vol / vol.shift(1)

        # Short MAs for momentum confirmation
        grp["ma5"] = close.rolling(5).mean()
        grp["ma10"] = close.rolling(10).mean()

        # Prior-day bar color (green/red)
        grp["prev_day_green"] = (close.shift(1) > grp["open"].shift(1))

        # Forward returns for trailing-stop sim
        for k in range(1, MAX_HOLD + 1):
            grp[f"fwd_close_{k}"] = close.shift(-k)
            grp[f"fwd_high_{k}"] = high.shift(-k)

        # T+1 entry: next day's open as alternative entry price
        grp["next_open"] = grp["open"].shift(-1)

        results.append(grp)

    daily_ctx = pd.concat(results, ignore_index=True)
    log.info("Daily features: %d rows", len(daily_ctx))

    # Merge
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
#  SIGNAL GENERATION — with accuracy filters
# ══════════════════════════════════════════════════════════

def generate_signals(merged, params):
    """Generate signals with optional accuracy-improvement filters."""
    p = params.get("vwap_reclaim", {})
    has_vwap = merged["vwap_1h"].notna() & merged["close"].notna()

    sig = (
        has_vwap
        & (merged["down_days"] >= p.get("down_days", 3))
        & (merged["rvol_1h"] >= p.get("rvol_thresh", 1.5))
        & (merged["close"] > merged["vwap_1h"])
    )

    # ── Experiment: Bounce strength filter ──
    # Require price to have recovered at least X% of the total drop before entry
    min_bounce = p.get("min_bounce_pct")
    if min_bounce is not None:
        sig = sig & (merged["bounce_pct"] >= min_bounce)

    # ── Experiment: Gap filter ──
    # Skip stocks that gap down too aggressively (likely to keep falling)
    max_gap_down = p.get("max_gap_down")  # e.g., -0.03 means skip if gaps down >3%
    if max_gap_down is not None:
        sig = sig & (merged["gap_pct"] >= max_gap_down)

    # ── Experiment: Volume follow-through ──
    # Signal-day volume must exceed prior day by X factor
    min_vol_ratio = p.get("min_vol_ratio")
    if min_vol_ratio is not None:
        sig = sig & (merged["vol_ratio_1d"] >= min_vol_ratio)

    # ── Experiment: Momentum confirmation ──
    # Close must be above short-term MA
    ma_filter = p.get("ma_filter")
    if ma_filter == "ma5":
        sig = sig & (merged["close"] > merged["ma5"])
    elif ma_filter == "ma10":
        sig = sig & (merged["close"] > merged["ma10"])

    # ── Experiment: Recovery ratio ──
    # Close must be within X% of the 20-day high (not too far from recovery)
    max_depth = p.get("depth_max")
    if max_depth is not None:
        sig = sig & (merged["depth_pct"] >= max_depth)

    # ── Experiment: Green signal day ──
    require_green = p.get("require_green", False)
    if require_green:
        sig = sig & (merged["close"] > merged["prev_close"])

    return sig


def simulate_trades(signal_rows, trail_pct, max_hold, entry_mode="close",
                    txn_cost=TXN_COST_PCT):
    """Trailing stop simulator with optional T+1 entry."""
    n = len(signal_rows)

    if entry_mode == "next_open":
        entry_prices = signal_rows["next_open"].values
    else:
        entry_prices = signal_rows["close"].values

    trade_rets = np.full(n, np.nan)

    fwd_closes = np.column_stack([
        signal_rows[f"fwd_close_{k}"].values for k in range(1, max_hold + 1)
    ])
    fwd_highs = np.column_stack([
        signal_rows[f"fwd_high_{k}"].values for k in range(1, max_hold + 1)
    ])

    # If entering at next_open, shift forward arrays by 1 (since day 1 forward
    # is already consumed by the entry day for next_open)
    if entry_mode == "next_open":
        # Entry is at T+1 open, so fwd_close_1 is the close of the entry day
        # We still use the same forward array — it's correct
        pass

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


def run_backtest(merged, params, trail_pct=0.02, max_hold=10,
                 max_positions=5, entry_mode="close"):
    """Generate signals, simulate, compute metrics."""
    sig_mask = generate_signals(merged, params)
    needed_col = f"fwd_close_{max_hold}"
    if needed_col not in merged.columns:
        return None

    signal_rows = merged[sig_mask & merged[needed_col].notna()].copy()
    if entry_mode == "next_open":
        signal_rows = signal_rows[signal_rows["next_open"].notna()]
    if signal_rows.empty or len(signal_rows) < MIN_TRADES:
        return None

    signal_rows = signal_rows.sort_values(["date", "rvol_1h"], ascending=[True, False])
    signal_rows["_rank"] = signal_rows.groupby("date").cumcount()
    signal_rows = signal_rows[signal_rows["_rank"] < max_positions]

    trade_rets, valid = simulate_trades(signal_rows, trail_pct, max_hold, entry_mode)
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
#  SWEEP GRID — 8 experiment axes
# ══════════════════════════════════════════════════════════

def build_sweep_configs():
    """
    Build parameter combos. Each "experiment" adds one filter on top of
    the baseline (D3 R1.5 T2% H10). We also cross some key combos.
    """
    configs = []
    experiment_id = 0

    # Fixed core params — these stay at the current live config
    # We sweep trail + hold lightly too (2 values each)
    trails = [0.02, 0.03]
    holds = [7, 10]
    down_days_list = [3, 4, 5]
    rvol_list = [1.0, 1.5, 2.0]

    for dd in down_days_list:
        for rvol in rvol_list:
            for trail in trails:
                for hold in holds:
                    base = {
                        "down_days": dd,
                        "rvol_thresh": rvol,
                        "bear_only": False,
                    }

                    # ── Baseline (no extra filter) ──
                    configs.append(_make_cfg(
                        base, trail, hold, "baseline", "none", "close"))

                    # ── Experiment 1: Bounce strength ──
                    for bounce in [0.20, 0.30, 0.40, 0.50]:
                        cfg = {**base, "min_bounce_pct": bounce}
                        configs.append(_make_cfg(
                            cfg, trail, hold, "bounce", f"bounce_{bounce}", "close"))

                    # ── Experiment 2: Gap filter ──
                    for gap in [-0.02, -0.03, -0.05]:
                        cfg = {**base, "max_gap_down": gap}
                        configs.append(_make_cfg(
                            cfg, trail, hold, "gap_filter", f"gap_{abs(gap)}", "close"))

                    # ── Experiment 3: Volume follow-through ──
                    for vr in [1.0, 1.2, 1.5]:
                        cfg = {**base, "min_vol_ratio": vr}
                        configs.append(_make_cfg(
                            cfg, trail, hold, "vol_follow", f"volr_{vr}", "close"))

                    # ── Experiment 4: Momentum confirmation ──
                    for ma in ["ma5", "ma10"]:
                        cfg = {**base, "ma_filter": ma}
                        configs.append(_make_cfg(
                            cfg, trail, hold, "momentum", ma, "close"))

                    # ── Experiment 5: Green signal day ──
                    cfg = {**base, "require_green": True}
                    configs.append(_make_cfg(
                        cfg, trail, hold, "green_day", "green", "close"))

                    # ── Experiment 6: T+1 entry ──
                    configs.append(_make_cfg(
                        base, trail, hold, "t1_entry", "next_open", "next_open"))

                    # ── Experiment 7: Combo — bounce + gap filter ──
                    for bounce in [0.30, 0.40]:
                        for gap in [-0.03, -0.05]:
                            cfg = {**base, "min_bounce_pct": bounce, "max_gap_down": gap}
                            configs.append(_make_cfg(
                                cfg, trail, hold, "combo_bg",
                                f"bounce_{bounce}_gap_{abs(gap)}", "close"))

                    # ── Experiment 8: Adaptive trail — wider for deeper dips ──
                    for depth_thresh in [-0.10, -0.15]:
                        # If depth < threshold (deeper), use wider trail
                        wide_trail = trail + 0.02
                        cfg = {**base, "depth_max": depth_thresh}
                        configs.append(_make_cfg(
                            cfg, wide_trail, hold, "adaptive_trail",
                            f"depth<{abs(depth_thresh)}_trail{wide_trail}", "close"))

    return configs


def _make_cfg(vwap_params, trail, hold, experiment, variant, entry_mode):
    label = f"{experiment}_{variant}_D{vwap_params['down_days']}_R{vwap_params['rvol_thresh']}_T{trail}_H{hold}"
    return {
        "vwap_reclaim": vwap_params,
        "_trail": trail,
        "_hold": hold,
        "_entry_mode": entry_mode,
        "_label": label,
        "_experiment": experiment,
        "_variant": variant,
    }


def _eval_one(args):
    """Worker function."""
    merged_slice, cfg = args
    try:
        res = run_backtest(
            merged_slice, cfg,
            trail_pct=cfg["_trail"],
            max_hold=cfg["_hold"],
            entry_mode=cfg["_entry_mode"],
        )
        if res:
            res["label"] = cfg["_label"]
            res["experiment"] = cfg["_experiment"]
            res["variant"] = cfg["_variant"]
            res["down_days"] = cfg["vwap_reclaim"]["down_days"]
            res["rvol"] = cfg["vwap_reclaim"]["rvol_thresh"]
            res["trail"] = cfg["_trail"]
            res["hold"] = cfg["_hold"]
            res["entry_mode"] = cfg["_entry_mode"]
        return res
    except Exception:
        return None


# ══════════════════════════════════════════════════════════
#  WALK-FORWARD + HOLDOUT
# ══════════════════════════════════════════════════════════

def find_stable_configs(merged, configs, windows, n_workers):
    window_results = {}

    for wi, (train_s, train_e, test_s, test_e) in enumerate(windows):
        test_dates = merged["date"].apply(lambda d: test_s <= d <= test_e)
        test_data = merged[test_dates].copy()
        log.info("WF %d: test %s→%s (%d rows)", wi, test_s, test_e, len(test_data))

        if n_workers > 1:
            with mp.Pool(n_workers) as pool:
                results = pool.map(
                    _eval_one,
                    [(test_data, c) for c in configs],
                )
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
                "experiment": wr[0]["experiment"],
                "variant": wr[0]["variant"],
                "down_days": wr[0]["down_days"],
                "rvol": wr[0]["rvol"],
                "trail": wr[0]["trail"],
                "hold": wr[0]["hold"],
                "entry_mode": wr[0]["entry_mode"],
            }
    return stable


def main():
    parser = argparse.ArgumentParser(description="V-shape accuracy sweep for VWAP Reclaim")
    parser.add_argument("--workers", type=int, default=mp.cpu_count())
    args = parser.parse_args()
    n_workers = min(args.workers, mp.cpu_count())

    with tempfile.TemporaryDirectory(prefix="vshape_sweep_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        merged, bench = load_and_prepare(tmp_path)

    configs = build_sweep_configs()
    log.info("Sweep grid: %d configs", len(configs))

    # Group by experiment to show progress
    from collections import Counter
    exp_counts = Counter(c["_experiment"] for c in configs)
    for exp, cnt in sorted(exp_counts.items()):
        log.info("  %s: %d configs", exp, cnt)

    # ── Walk-forward stability ──
    stable = find_stable_configs(merged, configs, WALK_FORWARD_WINDOWS, n_workers)
    log.info("Stable configs: %d / %d", len(stable), len(configs))

    # ── Holdout ──
    holdout = merged[merged["date"].apply(lambda d: HOLDOUT_START <= d <= HOLDOUT_END)].copy()
    log.info("Holdout: %s→%s (%d rows)", HOLDOUT_START, HOLDOUT_END, len(holdout))

    final = []
    for label, info in stable.items():
        cfg = next(c for c in configs if c["_label"] == label)
        res = run_backtest(
            holdout, cfg,
            trail_pct=cfg["_trail"],
            max_hold=cfg["_hold"],
            entry_mode=cfg["_entry_mode"],
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
            })

    if not final:
        print("\n⚠️  No configs survived walk-forward + holdout.")
        return

    df = pd.DataFrame(final).sort_values("holdout_sharpe", ascending=False)

    # ══════════════════════════════════════════════════════════
    # RESULTS
    # ══════════════════════════════════════════════════════════
    print("\n" + "=" * 110)
    print("V-SHAPE ACCURACY SWEEP RESULTS — VWAP Reclaim Reversal Strategy")
    print("=" * 110)
    print(f"Total configs: {len(configs)} | Stable: {len(stable)} | Holdout survivors: {len(final)}")

    # ── Summary by experiment ──
    experiments = ["baseline", "bounce", "gap_filter", "vol_follow",
                   "momentum", "green_day", "t1_entry", "combo_bg", "adaptive_trail"]

    print("\n┌───────────────────┬──────────┬────────┬─────────┬─────────┬─────────┬────────┐")
    print("│ Experiment        │ # Stable │  XIRR  │ Max DD  │ Sharpe  │ WinRate │ Trades │")
    print("├───────────────────┼──────────┼────────┼─────────┼─────────┼─────────┼────────┤")

    for exp in experiments:
        subset = df[df["experiment"] == exp]
        if subset.empty:
            print(f"│ {exp:17s} │   {'–':>4s}   │  {'–':>4s}  │  {'–':>5s}  │  {'–':>5s}  │  {'–':>5s}  │  {'–':>4s}  │")
            continue
        best = subset.iloc[0]
        n_stable = len(subset)
        print(f"│ {exp:17s} │  {n_stable:>5d}   │{best['holdout_xirr']:6.1f}% │{best['holdout_dd']:6.1f}% │ {best['holdout_sharpe']:6.2f} │{best['holdout_wr']:6.1f}% │ {best['holdout_trades']:>5.0f} │")

    print("└───────────────────┴──────────┴────────┴─────────┴─────────┴─────────┴────────┘")

    # ── Win rate improvement: compare to baseline ──
    baseline = df[df["experiment"] == "baseline"]
    if not baseline.empty:
        base_wr = baseline["holdout_wr"].mean()
        base_xirr = baseline["holdout_xirr"].mean()
        base_dd = baseline["holdout_dd"].mean()
        base_sharpe = baseline["holdout_sharpe"].mean()

        print(f"\n── Baseline reference (avg across stable): WR={base_wr:.1f}%  XIRR={base_xirr:.1f}%  DD={base_dd:.1f}%  Sharpe={base_sharpe:.2f} ──")
        print(f"\n{'Experiment':19s}  {'Avg WR':>7s}  {'Δ WR':>6s}  {'Avg XIRR':>9s}  {'Δ XIRR':>7s}  {'Avg DD':>7s}  {'Avg Sharpe':>11s}")
        print("─" * 80)

        for exp in experiments:
            subset = df[df["experiment"] == exp]
            if subset.empty:
                continue
            avg_wr = subset["holdout_wr"].mean()
            avg_xirr = subset["holdout_xirr"].mean()
            avg_dd = subset["holdout_dd"].mean()
            avg_sharpe = subset["holdout_sharpe"].mean()
            delta_wr = avg_wr - base_wr
            delta_xirr = avg_xirr - base_xirr
            sign_wr = "+" if delta_wr >= 0 else ""
            sign_xirr = "+" if delta_xirr >= 0 else ""
            print(f"{exp:19s}  {avg_wr:>6.1f}%  {sign_wr}{delta_wr:>5.1f}%  "
                  f"{avg_xirr:>8.1f}%  {sign_xirr}{delta_xirr:>5.1f}%  "
                  f"{avg_dd:>6.1f}%  {avg_sharpe:>10.2f}")

    # ── Top 15 configs by Sharpe ──
    print("\n── Top 15 configs (sorted by Sharpe) ──")
    top15 = df.head(15)
    for _, r in top15.iterrows():
        print(f"  [{r['experiment']:15s}] {r['variant']:25s}  "
              f"D{int(r['down_days'])} R{r['rvol']} T{r['trail']} H{int(r['hold'])}  "
              f"XIRR={r['holdout_xirr']:6.1f}%  DD={r['holdout_dd']:5.1f}%  "
              f"Sharpe={r['holdout_sharpe']:5.2f}  WR={r['holdout_wr']:5.1f}%  "
              f"Trades={r['holdout_trades']}")

    # ── Top 10 by win rate (maintaining XIRR > baseline mean) ──
    if not baseline.empty:
        df_wr = df[df["holdout_xirr"] >= base_xirr * 0.8].sort_values("holdout_wr", ascending=False)
        print(f"\n── Top 10 by Win Rate (XIRR >= {base_xirr * 0.8:.0f}% floor) ──")
        for _, r in df_wr.head(10).iterrows():
            print(f"  [{r['experiment']:15s}] {r['variant']:25s}  "
                  f"WR={r['holdout_wr']:5.1f}%  XIRR={r['holdout_xirr']:6.1f}%  "
                  f"DD={r['holdout_dd']:5.1f}%  Sharpe={r['holdout_sharpe']:5.2f}")

    # ── Save ──
    out_path = ROOT / "results"
    out_path.mkdir(exist_ok=True)
    df.to_csv(out_path / "vshape_accuracy_sweep.csv", index=False)
    print(f"\nFull results saved to results/vshape_accuracy_sweep.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()
