#!/usr/bin/env python3
"""
Step 06 — Market Regime + Breadth Filter  v5.2
================================================
Classifies each trading day using:

  1. Dual-MA regime (50-day fast + 150-day slow)
     - bull     = bench > MA50 AND bench > MA150
     - recovery = bench > MA50 but below MA150
     - bear     = bench < MA50

  2. Market breadth (% of stocks above their 50-day MA)
     - healthy   = >50% above MA50
     - weakening = 30-50% above MA50
     - weak      = <30% above MA50

  3. Weak market detection (30-day index return)
     - is_weak = index down >5% in 30 trading days

  4. Breadth improvement (breadth rising from low)
     - breadth_improving = 10d SMA of breadth > 20d SMA
     - Signals internal recovery even before index crosses MA

Minervini: "I look at the overall health of the market through
breadth indicators. When fewer than 50% of stocks are above their
50-day, the market is sick internally."

Outputs ``regime`` table with columns:
  date, market, bench_close, bench_ma200, bench_ma50, bench_ma150,
  is_bull, is_recovery, breadth_pct, is_weak_market, breadth_improving

Usage:
    uv run python -m faralpha.pipeline.s06_regime --market both
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.utils.upsert import upsert_by_market

log = get_logger("s06_regime")


def _compute_breadth(con, market: str) -> pd.DataFrame:
    """Compute market breadth: % of stocks above their 50-day MA per day.

    Also computes advance/decline ratio (stocks going up vs down).
    Uses the features table which already has ma50 computed.
    """
    log.info(f"  Computing market breadth for {market.upper()}…")

    breadth = con.execute("""
        SELECT date,
               COUNT(CASE WHEN close > ma50 AND ma50 IS NOT NULL THEN 1 END) as n_above_ma50,
               COUNT(CASE WHEN ma50 IS NOT NULL THEN 1 END) as n_total,
               COUNT(CASE WHEN close > ma50 AND ma50 IS NOT NULL THEN 1 END) * 1.0
                   / NULLIF(COUNT(CASE WHEN ma50 IS NOT NULL THEN 1 END), 0) as breadth_pct
        FROM features
        WHERE market = ?
        GROUP BY date
        ORDER BY date
    """, [market]).df()

    if breadth.empty:
        return breadth

    breadth["date"] = pd.to_datetime(breadth["date"])

    # Smoothed breadth for trend detection
    win = config.BREADTH.get("breadth_improving_window", 10)
    breadth["breadth_sma10"] = breadth["breadth_pct"].rolling(win, min_periods=win).mean()
    breadth["breadth_sma20"] = breadth["breadth_pct"].rolling(win * 2, min_periods=win * 2).mean()

    # Breadth improving: fast SMA crossing above slow SMA (internal recovery)
    breadth["breadth_improving"] = breadth["breadth_sma10"] > breadth["breadth_sma20"]

    return breadth[["date", "breadth_pct", "n_above_ma50", "n_total", "breadth_improving"]]


def _compute_regime(bench: pd.DataFrame, market: str) -> pd.DataFrame:
    """Compute bull/recovery/bear using dual-MA regime (fast 50 + slow 150).

    Minervini Ch.10: "I want to see the market in a confirmed uptrend
    above the 50-day moving average." He re-enters aggressively once the
    index clears the 50-day, even if still below the 200-day.

    Regimes:
      bull     = bench > MA50 AND bench > MA150 (full uptrend)
      recovery = bench > MA50 but below MA150 (early recovery - trade smaller)
      bear     = bench < MA50 (fully defensive)
    """
    bench = bench.sort_values("date").copy()
    ma_fast = config.REGIME.get("ma_fast", 50)
    ma_slow = config.REGIME.get("ma_slow", 150)

    bench["bench_ma50"] = bench["close"].rolling(ma_fast, min_periods=ma_fast).mean()
    bench["bench_ma150"] = bench["close"].rolling(ma_slow, min_periods=ma_slow).mean()
    # Keep ma200 for backward compat
    ma_win = config.REGIME.get("ma_window", 200)
    bench["bench_ma200"] = bench["close"].rolling(ma_win, min_periods=ma_win).mean()

    # 30-day index return for weak market detection
    bench["bench_return_30d"] = bench["close"].pct_change(30)

    # Weak market: index down >5% in last 30 days
    weak_threshold = config.BREADTH.get("weak_market_30d_pct", -0.05)
    bench["is_weak_market"] = bench["bench_return_30d"] < weak_threshold

    # v6.4: Revert to proven MA200 regime (v5.3 baseline).
    # is_bull = bench > MA200 (confirmed uptrend → full buying)
    # is_recovery = bench > MA50 but < MA200 (info only, used for sizing)
    # Bear = bench < MA200 (force close all positions)
    #
    # The MA50-based entry (v6.0-v6.3) caused either whipsaw or no
    # bear protection. MA200 is proven stable at 4.8% CAGR / -29.7% DD.
    bench["is_bull"] = bench["close"] > bench["bench_ma200"]
    bench["is_confirmed_bull"] = bench["close"] > bench["bench_ma200"]
    bench["is_recovery"] = (
        (bench["close"] > bench["bench_ma50"])
        & (bench["close"] < bench["bench_ma200"])
    )
    # Regime strength: 1.0 when confirmed bull, else scaled 0-1
    ma_range = bench["bench_ma200"] - bench["bench_ma50"]
    bench["regime_strength"] = np.where(
        bench["is_bull"],
        1.0,
        np.where(
            bench["is_recovery"] & (ma_range > 0),
            ((bench["close"] - bench["bench_ma50"]) / ma_range).clip(0, 1),
            0.0
        )
    )

    bench["market"] = market
    bench = bench.rename(columns={"close": "bench_close"})
    bench = bench[["date", "market", "bench_close", "bench_ma200",
                    "bench_ma50", "bench_ma150", "bench_return_30d",
                    "is_bull", "is_confirmed_bull", "is_recovery",
                    "is_weak_market", "regime_strength"]].dropna(subset=["bench_ma50"])
    return bench


def run(market: str = "both") -> None:
    """Compute market regime + breadth and store as ``regime`` table."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]

    frames: list[pd.DataFrame] = []

    for mkt in markets:
        bench_ticker = config.BENCHMARK.get(mkt)
        if not bench_ticker:
            continue

        bench = con.execute(
            "SELECT date, close FROM benchmark WHERE ticker = ?",
            [bench_ticker],
        ).df()

        if bench.empty:
            log.warning(f"No benchmark data for {mkt} ({bench_ticker})")
            continue

        bench["date"] = pd.to_datetime(bench["date"])
        regime = _compute_regime(bench, mkt)

        # Merge breadth data
        breadth = _compute_breadth(con, mkt)
        if not breadth.empty:
            regime = regime.merge(breadth, on="date", how="left")
            regime["breadth_improving"] = regime["breadth_improving"].fillna(False)
        else:
            regime["breadth_pct"] = np.nan
            regime["n_above_ma50"] = 0
            regime["n_total"] = 0
            regime["breadth_improving"] = False

        frames.append(regime)

        bull_pct = regime["is_bull"].mean() * 100
        recovery_pct = regime["is_recovery"].mean() * 100
        bear_pct = 100 - bull_pct
        weak_pct = regime["is_weak_market"].mean() * 100
        avg_breadth = regime["breadth_pct"].mean() * 100
        log.info(f"{mkt.upper()}: {len(regime)} days, "
                 f"{bull_pct:.1f}% bull ({recovery_pct:.1f}% recovery), "
                 f"{bear_pct:.1f}% bear, {weak_pct:.1f}% weak-market days, "
                 f"avg breadth {avg_breadth:.1f}%")

    if not frames:
        log.error("No regime data computed.")
        return

    combined = pd.concat(frames, ignore_index=True)

    upsert_by_market(con, "regime", combined, markets)
    log.info(f"✓ Regime table: {len(combined):,} rows")
    con.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Compute market regime + breadth")
    p.add_argument("--market", default="india", choices=["india"])
    args = p.parse_args()
    run(market=args.market)
