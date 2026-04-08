#!/usr/bin/env python3
"""
Step 03 — Feature Engineering  v2.0
====================================
Computes ALL technical indicators for the Minervini SEPA strategy.
Every feature uses only data available up to that date (no look-ahead).

v2.0 additions:
  - Contraction tracking (pullback depths for VCP)
  - Inside-bar sequence counting
  - Base depth from pivot high
  - Darvas box features
  - Dollar-volume filter

Usage:
    uv run python -m faralpha.pipeline.s03_features --market both
"""

from __future__ import annotations

import pandas as pd
import numpy as np

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.utils.upsert import upsert_by_market

log = get_logger("s03_features")


def _compute_ticker_features(g: pd.DataFrame) -> pd.DataFrame:
    """Compute all features for a single ticker's sorted price history."""
    g = g.copy()
    close = g["close"]
    high = g["high"]
    low = g["low"]
    volume = g["volume"]
    daily_ret = close.pct_change()

    # ── Moving Averages ──────────────────────────────────
    g["ma10"] = close.rolling(10, min_periods=10).mean()
    g["ma21"] = close.rolling(21, min_periods=21).mean()
    g["ma50"] = close.rolling(50, min_periods=50).mean()
    g["ma150"] = close.rolling(150, min_periods=150).mean()
    g["ma200"] = close.rolling(200, min_periods=200).mean()
    g["ma200_slope"] = g["ma200"].pct_change(20)

    # ── 52-week High / Low ───────────────────────────────
    g["high_52w"] = high.rolling(252, min_periods=60).max()
    g["low_52w"] = low.rolling(252, min_periods=60).min()
    g["pct_from_52w_high"] = (close - g["high_52w"]) / g["high_52w"]
    g["pct_above_52w_low"] = (close - g["low_52w"]) / g["low_52w"]

    # ── Base Structure ────────────────────────────────────
    g["base_high_30"] = high.rolling(30, min_periods=20).max()
    g["base_high_60"] = high.rolling(60, min_periods=40).max()
    g["base_low_30"] = low.rolling(30, min_periods=20).min()
    g["base_range_30d"] = (
        (high.rolling(30, min_periods=20).max() - low.rolling(30, min_periods=20).min())
        / close
    )
    g["base_range_20d"] = (
        (high.rolling(20, min_periods=15).max() - low.rolling(20, min_periods=15).min())
        / close
    )

    # ── Pivot High + Base Depth ───────────────────────────
    # Pivot = highest high in lookback window
    pivot_lb = config.VCP.get("pivot_lookback", 65)
    g["pivot_high"] = high.rolling(pivot_lb, min_periods=30).max()
    g["base_depth"] = (g["pivot_high"] - close) / g["pivot_high"]

    # ── Contraction Tracking (for multi-contraction VCP) ──
    # Track the rolling 5-day low relative to pivot
    g["low_5d"] = low.rolling(5, min_periods=3).min()
    g["pullback_depth_5d"] = (g["pivot_high"] - g["low_5d"]) / g["pivot_high"]
    # Previous pullback depth (shifted 10 days)
    g["prev_pullback_depth_10d"] = g["pullback_depth_5d"].shift(10)
    # Is current contraction shallower than previous?
    g["contraction_shallower"] = (
        g["pullback_depth_5d"] < g["prev_pullback_depth_10d"]
    ).astype(float)
    # Rolling count of successive shallower contractions
    g["contraction_count"] = g["contraction_shallower"].rolling(
        40, min_periods=10
    ).sum()

    # ── Inside Bars ───────────────────────────────────────
    # Each bar where high < prev high AND low > prev low
    prev_high = high.shift(1)
    prev_low = low.shift(1)
    g["is_inside_bar"] = ((high < prev_high) & (low > prev_low)).astype(float)
    # Count consecutive inside bars
    g["inside_bar_streak"] = g["is_inside_bar"].rolling(
        10, min_periods=1
    ).sum()

    # ── Darvas Box Features ───────────────────────────────
    # Box top = highest high in confirmation window
    confirm = config.DARVAS.get("confirm_days", 3)
    g["darvas_top"] = high.rolling(
        config.DARVAS.get("box_max_days", 50), min_periods=10
    ).max()
    g["darvas_bottom"] = low.rolling(
        config.DARVAS.get("box_max_days", 50), min_periods=10
    ).min()
    g["darvas_range"] = (g["darvas_top"] - g["darvas_bottom"]) / close
    g["above_darvas_top"] = (close > g["darvas_top"].shift(1)).astype(float)

    # ── Tight Range Detection ─────────────────────────────
    # 15-day range as % of price (for Power Play)
    g["range_15d"] = (
        (high.rolling(15, min_periods=10).max() -
         low.rolling(15, min_periods=10).min()) / close
    )

    # ── Momentum ──────────────────────────────────────────
    g["momentum_3m"] = close.pct_change(63)
    g["momentum_6m"] = close.pct_change(126)
    g["momentum_9m"] = close.pct_change(189)
    g["momentum_12m"] = close.pct_change(252)
    shifted_1m = close.shift(21)
    shifted_12m = close.shift(252)
    g["momentum_12m_skip1m"] = np.where(
        shifted_12m > 0, (shifted_1m / shifted_12m) - 1.0, np.nan,
    )

    # ── Volatility ────────────────────────────────────────
    g["volatility_5d"] = daily_ret.rolling(5, min_periods=4).std()
    g["volatility_10d"] = daily_ret.rolling(10, min_periods=8).std()
    g["volatility_20d"] = daily_ret.rolling(20, min_periods=15).std()

    # ── RSI(2) — Connors mean reversion indicator ─────────
    # Ultra short-term: RSI(2) < 10 = extremely oversold bounce setup
    _delta = close.diff()
    _gain = _delta.clip(lower=0)
    _loss = (-_delta).clip(lower=0)
    _avg_gain = _gain.ewm(span=2, min_periods=2, adjust=False).mean()
    _avg_loss = _loss.ewm(span=2, min_periods=2, adjust=False).mean()
    _rs_val = _avg_gain / _avg_loss.replace(0, np.nan)
    g["rsi_2"] = 100 - (100 / (1 + _rs_val))

    # ── Volume ────────────────────────────────────────────
    g["avg_volume_50d"] = volume.rolling(50, min_periods=30).mean()
    g["avg_volume_10d"] = volume.rolling(10, min_periods=8).mean()
    g["volume_ratio"] = volume / g["avg_volume_50d"]
    g["dollar_volume"] = close * volume

    # ── Institutional Accumulation Tracking ───────────────
    # Accumulation day: price up ≥1.5% on volume ≥1.5× avg
    # Distribution day: price down ≥1.5% on high volume
    # Minervini: "Look for accumulation — institutions leave footprints"
    _vol_ratio = volume / g["avg_volume_50d"]
    g["is_accum_day"] = ((daily_ret >= 0.015) & (_vol_ratio >= 1.5)).astype(float)
    g["is_distrib_day"] = ((daily_ret <= -0.015) & (_vol_ratio >= 1.5)).astype(float)
    # Net accumulation score over 20 trading days
    g["accum_score"] = (
        g["is_accum_day"].rolling(20, min_periods=10).sum()
        - g["is_distrib_day"].rolling(20, min_periods=10).sum()
    )
    # Up-volume vs down-volume dominance (20-day window)
    _up_vol = volume.where(daily_ret > 0, 0)
    _dn_vol = volume.where(daily_ret < 0, 0)
    g["up_volume_20d"] = _up_vol.rolling(20, min_periods=10).sum()
    g["down_volume_20d"] = _dn_vol.rolling(20, min_periods=10).sum()
    g["volume_dominance"] = g["up_volume_20d"] / g["down_volume_20d"].replace(0, np.nan)

    # ── Price vs MA ───────────────────────────────────────
    g["close_to_ma50"] = close / g["ma50"] - 1.0
    g["close_to_ma200"] = close / g["ma200"] - 1.0

    # ── Stage-2 Duration Tracking ─────────────────────────
    # How many consecutive days has the stock been in Stage 2?
    # Stage 2 = price > 150 MA > 200 MA, 200 MA rising
    stage2_today = (
        (close > g["ma150"]) &
        (g["ma150"] > g["ma200"]) &
        (g["ma200_slope"] > 0)
    ).astype(float)
    # Count consecutive days in Stage 2 — reset on any break
    g["stage2_streak"] = stage2_today * (
        stage2_today.groupby((stage2_today != stage2_today.shift()).cumsum()).cumcount() + 1
    )

    # ── Dollar Volume (for liquidity filter) ──────────────
    g["dollar_volume_50d"] = (close * volume).rolling(50, min_periods=30).mean()

    # ── Volume at pivot (dry-up check for VCP) ────────────
    g["vol_at_low_10d"] = volume.rolling(10, min_periods=5).min()
    g["vol_dryup_ratio"] = g["vol_at_low_10d"] / g["avg_volume_50d"]

    return g


def _apply_quality_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove penny stocks and illiquid names (Minervini: $10+ price, institutional liquidity)."""
    n_before = len(df)
    mask_price = df["close"] >= config.DATA_QUALITY["min_price"]
    mask_vol = df["avg_volume_50d"] >= config.DATA_QUALITY["min_avg_volume"]
    # Dollar volume filter (NEW) — Minervini requires institutional-grade liquidity
    min_dv = config.DATA_QUALITY.get("min_dollar_volume", 0)
    if min_dv > 0 and "dollar_volume_50d" in df.columns:
        mask_dv = (df["dollar_volume_50d"] >= min_dv) | df["dollar_volume_50d"].isna()
    else:
        mask_dv = pd.Series(True, index=df.index)
    # Max price filter for India (capital allocation constraint)
    max_price_india = config.DATA_QUALITY.get("max_price_india", 0)
    if max_price_india > 0 and "market" in df.columns:
        mask_max_price = ~((df["market"] == "india") & (df["close"] > max_price_india))
    else:
        mask_max_price = pd.Series(True, index=df.index)
    mask = mask_price & (mask_vol | df["avg_volume_50d"].isna()) & mask_dv & mask_max_price
    df = df[mask].copy()
    extra = f", max_price_india=₹{max_price_india:,.0f}" if max_price_india > 0 else ""
    log.info(f"Quality filter: {n_before:,} → {len(df):,} rows "
             f"({n_before - len(df):,} removed) "
             f"[min_price=${config.DATA_QUALITY['min_price']}, "
             f"min_vol={config.DATA_QUALITY['min_avg_volume']:,}, "
             f"min_$vol=${min_dv:,.0f}{extra}]")
    return df


def _apply_price_coherence_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Detect and remove tickers with Yahoo Finance price scale corruption.

    Yahoo sometimes returns historical prices at a different scale
    (face-value or pre-split) while the latest bar is at market price.
    This creates fake 10x-300x "returns" and ruins RS rankings.

    Detection: For each ticker, compare the last close to the median of
    the previous 50 closes. If the ratio exceeds SCALE_JUMP_THRESHOLD,
    the ticker's data is considered corrupt and removed entirely.
    """
    SCALE_JUMP_THRESHOLD = 3.0  # 3x jump = clearly wrong
    MIN_HISTORY = 20  # Need at least 20 rows to check

    corrupt_tickers = []
    for tkr, g in df.groupby("ticker"):
        if len(g) < MIN_HISTORY:
            continue
        g_sorted = g.sort_values("date")
        closes = g_sorted["close"].values
        last_close = closes[-1]
        # Median of previous 50 closes (excluding the last)
        lookback = closes[max(0, len(closes) - 51):-1]
        if len(lookback) < 10:
            continue
        median_prev = float(np.median(lookback))
        if median_prev <= 0:
            continue
        ratio = last_close / median_prev
        if ratio > SCALE_JUMP_THRESHOLD or ratio < (1.0 / SCALE_JUMP_THRESHOLD):
            corrupt_tickers.append((tkr, last_close, median_prev, ratio))

    if corrupt_tickers:
        log.warning(f"Price coherence: {len(corrupt_tickers)} tickers have "
                    f"scale-corrupted Yahoo data (>{SCALE_JUMP_THRESHOLD}x jump):")
        for tkr, last, med, ratio in corrupt_tickers[:20]:
            log.warning(f"  {tkr}: latest={last:,.2f} vs median50={med:,.2f} "
                        f"(ratio={ratio:.1f}x) — REMOVED")
        if len(corrupt_tickers) > 20:
            log.warning(f"  ... and {len(corrupt_tickers) - 20} more")
        bad_set = {t[0] for t in corrupt_tickers}
        before = len(df)
        df = df[~df["ticker"].isin(bad_set)].copy()
        log.info(f"Price coherence filter: {before:,} → {len(df):,} rows "
                 f"({len(bad_set)} tickers removed)")

    return df


def _apply_ipo_seasoning(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where the stock is too young since its listing date."""
    if "listing_date" not in df.columns or df["listing_date"].isna().all():
        return df
    df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    days_since = (df["date"] - df["listing_date"]).dt.days
    seasoned = (days_since >= config.IPO_SEASONING_DAYS) | df["listing_date"].isna()
    n_removed = (~seasoned).sum()
    df = df[seasoned].copy()
    if n_removed > 0:
        log.info(f"IPO seasoning: removed {n_removed:,} rows "
                 f"(< {config.IPO_SEASONING_DAYS} days)")
    return df


def run(market: str = "both", full_history: bool = False) -> None:
    """Compute features and store in ``features`` table.

    By default, loads the last ~2.5 years of price data (630 trading
    days) for fast daily runs.  When ``full_history=True``, loads ALL
    price data from DATA_START so every year has features — required
    for accurate multi-year backtesting.
    """
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]
    all_frames: list[pd.DataFrame] = []

    # 630 trading days ≈ 2.5 years — enough for 200-day MA warmup
    LOOKBACK_DAYS = 630

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Feature engineering: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        if full_history:
            # Load ALL price data — needed for multi-year backtesting
            log.info("Full-history mode: loading ALL prices (no lookback cutoff)")
            df = con.execute("""
                SELECT p.date, p.ticker, p.open, p.high, p.low, p.close, p.volume,
                       s.sector, s.industry, s.listing_date
                FROM prices p
                JOIN stocks s ON p.ticker = s.ticker AND p.market = s.market
                WHERE p.market = ?
                ORDER BY p.ticker, p.date
            """, [mkt]).df()
        else:
            # Only load recent data (much faster than full history)
            cutoff = (pd.Timestamp.now().normalize()
                      - pd.Timedelta(days=int(LOOKBACK_DAYS * 1.5))).strftime("%Y-%m-%d")
            df = con.execute("""
                SELECT p.date, p.ticker, p.open, p.high, p.low, p.close, p.volume,
                       s.sector, s.industry, s.listing_date
                FROM prices p
                JOIN stocks s ON p.ticker = s.ticker AND p.market = s.market
                WHERE p.market = ? AND p.date >= ?
                ORDER BY p.ticker, p.date
            """, [mkt, cutoff]).df()

        if df.empty:
            log.warning(f"No price data for {mkt}. Skipping.")
            continue

        n_tickers = df["ticker"].nunique()
        log.info(f"Loaded {len(df):,} rows for {n_tickers} tickers")
        log.info("Computing indicators…")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        from tqdm import tqdm
        tickers = df["ticker"].unique()

        # Use ThreadPoolExecutor for parallel feature computation
        max_workers = min(8, len(tickers))
        frames: list[pd.DataFrame] = []

        # Pre-split by ticker for parallel processing
        ticker_groups = {tkr: df[df["ticker"] == tkr] for tkr in tickers}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_compute_ticker_features, group): tkr
                for tkr, group in ticker_groups.items()
            }
            pbar = tqdm(
                as_completed(futures), total=len(futures), desc=f"features [{mkt}]"
            )
            for future in pbar:
                tkr = futures[future]
                try:
                    result_frame = future.result()
                    frames.append(result_frame)
                except Exception as exc:
                    log.debug(f"  {tkr}: feature compute failed — {exc}")
        result = pd.concat(frames, ignore_index=True)
        result["market"] = mkt

        result = _apply_price_coherence_filter(result)
        result = _apply_quality_filters(result)
        result = _apply_ipo_seasoning(result)

        all_frames.append(result)
        log.info(f"✓ {mkt.upper()}: {result['ticker'].nunique()} tickers, "
                 f"{len(result):,} rows after filters")

    if not all_frames:
        log.error("No data to store.")
        return

    combined = pd.concat(all_frames, ignore_index=True)

    log.info(f"Storing {len(combined):,} feature rows…")
    upsert_by_market(con, "features", combined, markets)

    n = con.execute("SELECT COUNT(*) FROM features").fetchone()[0]
    nt = con.execute("SELECT COUNT(DISTINCT ticker) FROM features").fetchone()[0]
    log.info(f"✓ Features table: {n:,} rows, {nt} tickers")
    con.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Compute technical features")
    p.add_argument("--market", default="india", choices=["india"])
    p.add_argument("--full-history", action="store_true",
                   help="Compute features from ALL price data (for backtesting)")
    args = p.parse_args()
    run(market=args.market, full_history=args.full_history)
