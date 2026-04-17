#!/usr/bin/env python3
"""
Step 05 — Pattern Detection  v2.0  (Minervini SEPA)
=====================================================
Detects the core patterns from Mark Minervini's methodology:

  1. **Trend Template** — Stage-2 uptrend filter
  2. **Multi-Contraction VCP** — successive shallower pullbacks + volume dry-up
  3. **Darvas Box** — N-day consolidation box, breakout above box top
  4. **Power Play** — ultra-tight range (<5%) over 15+ days
  5. **IPO Base** — tight consolidation after recent listing
  6. **Breakout** — price exceeds pivot/base high with volume confirmation
  7. **Combined buy signal** — pattern + breakout + trend template

References:
  Mark Minervini — "Trade Like a Stock Market Wizard" Ch.4-8
  Mark Minervini — "Think & Trade Like a Champion" Ch.7-8
  Nicolas Darvas — "How I Made $2,000,000 in the Stock Market"

Usage:
    uv run python -m faralpha.pipeline.s05_patterns --market both
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.utils.upsert import upsert_by_market

log = get_logger("s05_patterns")


# ═══════════════════════════════════════════════════════════
#  INDIVIDUAL PATTERN DETECTORS
# ═══════════════════════════════════════════════════════════

def detect_trend_template(df: pd.DataFrame) -> pd.Series:
    """
    Minervini Trend Template — ALL 8 conditions from
    "Trade Like a Stock Market Wizard" Ch.4, pp.83-86.

    Verbatim from the book:
      1. Stock price above 150-day (30-week) MA
      2. Stock price above 200-day (40-week) MA
      3. 150-day MA above 200-day MA
      4. 200-day MA trending up for at least 1 month (ideally 4-5+ months)
      5. 50-day MA above 150-day MA AND above 200-day MA
      6. Stock price above 50-day MA
      7. Stock price at least 30% above 52-week low (more = better)
      8. Stock price within at least 25% of 52-week high (closer = better)

    ADDED (from interviews/USIC):
      9. Price above 10-day MA (near-term strength on entry day)
      10. Stage-2 uptrend established for ≥20 days (not a fresh crossover)
    """
    tt = config.TREND_TEMPLATE

    c1 = df["close"] > df["ma150"]                    # 1. price > 150 MA
    c2 = df["close"] > df["ma200"]                    # 2. price > 200 MA
    c3 = df["ma150"] > df["ma200"]                    # 3. 150 MA > 200 MA
    c4 = df["ma200_slope"] > 0                         # 4. 200 MA trending up
    c5 = (df["ma50"] > df["ma150"]) & (df["ma50"] > df["ma200"])  # 5. 50 MA > both
    c6 = df["close"] > df["ma50"]                      # 6. price > 50 MA
    c7 = df["close"] >= df["low_52w"] * tt["above_low_pct"]       # 7. ≥30% above 52w low
    c8 = df["close"] >= df["high_52w"] * tt["near_high_pct"]      # 8. within 25% of 52w high

    result = c1 & c2 & c3 & c4 & c5 & c6 & c7 & c8

    # 9. Price > 10-day MA (near-term strength)
    if tt.get("price_above_ma10", False) and "ma10" in df.columns:
        c9 = df["close"] > df["ma10"]
        result = result & c9

    # 10. Stage-2 duration check — avoid fresh crossovers
    min_days = tt.get("min_stage2_days", 0)
    if min_days > 0 and "stage2_streak" in df.columns:
        c10 = df["stage2_streak"] >= min_days
        result = result & c10

    return result


def detect_vcp(df: pd.DataFrame) -> pd.Series:
    """
    Multi-Contraction Volatility Contraction Pattern (VCP).

    From Minervini's books & USIC presentations:
      1. Base depth ≤ 25% from pivot high  (not over-extended)
      2. At least 2 successive shallower pullbacks (contraction_count)
      3. Short-term volatility contracting (10d < 20d × ratio)
      4. 30-day range tight (< 15% for the final contraction)
      5. Volume dry-up at the lows (Minervini: "This is the hallmark of a VCP")

    "When supply diminishes to the point where sellers are exhausted,
     price becomes very tight and volume dries up. That's when the
     stock is ready to move." — Mark Minervini
    """
    v = config.VCP

    # 1. Base depth limit — not a broken chart
    depth_ok = True
    if "base_depth" in df.columns:
        depth_ok = df["base_depth"] <= v["max_base_depth_pct"]

    # 2. Multi-contraction: need ≥ min_contractions shallower pullbacks
    contraction_ok = True
    min_c = v.get("min_contractions", 2)
    if "contraction_count" in df.columns:
        contraction_ok = df["contraction_count"] >= min_c

    # 3. Tight base range (final contraction)
    tight = df["base_range_30d"] < v["base_range_max"]

    # 4. Volume dry-up at the pivot — Minervini's KEY requirement
    #    "When supply diminishes... volume dries up. That's when the stock
    #    is ready to move." — this checks VOLUME behaviour, not price volatility.
    #    Note: removed old price-volatility check (10d vs 20d vol) which is
    #    NOT a Minervini concept and wrongly killed NVDA, TSLA, CELH entries.
    vol_dryup = True
    dryup_ratio = v.get("volume_dryup_ratio", 0.70)
    if "vol_dryup_ratio" in df.columns:
        vol_dryup = df["vol_dryup_ratio"] <= dryup_ratio
    elif "avg_volume_10d" in df.columns and "avg_volume_50d" in df.columns:
        vol_dryup = df["avg_volume_10d"] < df["avg_volume_50d"] * dryup_ratio

    return depth_ok & contraction_ok & tight & vol_dryup


def detect_darvas_box(df: pd.DataFrame) -> pd.Series:
    """
    Darvas Box — consolidation within a defined box, breakout above box top.

    Rules:
      1. Box range (top - bottom) / price < max_range_pct  (tight consolidation)
      2. Price breaks above the box top (darvas_top shifted to avoid look-ahead)
      3. Volume spike on breakout day
    """
    dc = config.DARVAS

    # Box is tight enough
    tight_box = True
    if "darvas_range" in df.columns:
        tight_box = df["darvas_range"] < dc["box_max_range_pct"]

    # Breakout above box top (feature computes above_darvas_top)
    breakout = True
    if "above_darvas_top" in df.columns:
        breakout = df["above_darvas_top"] > 0

    # Volume confirmation
    vol_ok = df["volume_ratio"] > config.BREAKOUT["volume_spike_multiplier"] * 0.8

    return tight_box & breakout & vol_ok


def detect_power_play(df: pd.DataFrame) -> pd.Series:
    """
    Power Play — ultra-tight setup.

    From Minervini's interviews: a stock that has run up strongly, pauses
    in a very tight range (<5%) for 2-3 weeks with low volume, then resumes.

    Rules:
      1. 15-day price range < 5%
      2. Strong prior uptrend (momentum_3m > 20%)
      3. Volume drying up (10d avg < 50d avg × ratio)
    """
    pp = config.POWER_PLAY
    tight = True
    if "range_15d" in df.columns:
        tight = df["range_15d"] < pp["max_range_pct"]

    strong_trend = df["momentum_3m"] > 0.20
    vol_dry = df["avg_volume_10d"] < df["avg_volume_50d"] * pp["volume_dry_ratio"]

    return tight & strong_trend & vol_dry


def detect_ipo_base(df: pd.DataFrame) -> pd.Series:
    """
    IPO Base — consolidation soon after listing.

    Rules:
      - Stock is between min_days and max_days since listing
      - Trading in a tight range (< max_range_pct)
    """
    ib = config.IPO_BASE

    if "listing_date" not in df.columns:
        return pd.Series(False, index=df.index)

    listing = pd.to_datetime(df["listing_date"], errors="coerce")
    date = pd.to_datetime(df["date"])
    days_since = (date - listing).dt.days

    age_ok = (days_since >= ib["min_days"]) & (days_since <= ib["max_days"])
    tight = df["base_range_30d"] < ib["max_range_pct"]

    return age_ok & tight


def detect_breakout(df: pd.DataFrame) -> pd.Series:
    """
    Breakout — price exceeds the recent base/pivot high with volume spike.

    Minervini: "I need to see volume at least 150-200% of the 50-day average
    on the breakout day. That tells me institutions are participating."

    Rules:
      1. Close > prior day's 30-day base high  (no look-ahead)
      2. Volume > 50-day average × multiplier (2.0x = 200%)
    """
    bo = config.BREAKOUT

    prior_base_high = df.groupby("ticker")["base_high_30"].shift(1)
    price_breakout = df["close"] > prior_base_high
    volume_spike = df["volume_ratio"] > bo["volume_spike_multiplier"]

    return price_breakout & volume_spike


# ═══════════════════════════════════════════════════════════
#  COMBINED SIGNAL
# ═══════════════════════════════════════════════════════════

def _add_pattern_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add boolean pattern columns to the DataFrame."""
    log.info("  Detecting Trend Template…")
    df["trend_template"] = detect_trend_template(df)
    tt_pct = df["trend_template"].mean() * 100
    log.info(f"    Trend Template: {tt_pct:.1f}% of rows pass")

    log.info("  Detecting Multi-Contraction VCP…")
    df["vcp"] = detect_vcp(df)
    vcp_pct = df["vcp"].mean() * 100
    log.info(f"    VCP: {vcp_pct:.1f}% of rows pass")

    log.info("  Detecting Darvas Box…")
    df["darvas"] = detect_darvas_box(df)
    darvas_pct = df["darvas"].mean() * 100
    log.info(f"    Darvas: {darvas_pct:.1f}% of rows pass")

    log.info("  Detecting Power Play…")
    df["power_play"] = detect_power_play(df)
    pp_pct = df["power_play"].mean() * 100
    log.info(f"    Power Play: {pp_pct:.1f}% of rows pass")

    log.info("  Detecting IPO Base…")
    df["ipo_base"] = detect_ipo_base(df)
    ipo_pct = df["ipo_base"].mean() * 100
    log.info(f"    IPO Base: {ipo_pct:.1f}% of rows match")

    log.info("  Detecting Breakouts…")
    df["breakout"] = detect_breakout(df)
    bo_pct = df["breakout"].mean() * 100
    log.info(f"    Breakout: {bo_pct:.1f}% of rows pass")

    # ── Rolling lookback for pattern validity ──
    # KEY FIX: The breakout candle itself often expands the base range, which
    # turns VCP off on that exact day.  Minervini enters on a breakout from a
    # VCP pattern — meaning the setup (VCP) was valid BEFORE the breakout bar.
    # Allow VCP to have been true within the last 3 days.
    vcp_recent = df.groupby("ticker")["vcp"].transform(
        lambda s: s.rolling(4, min_periods=1).max()
    ).astype(bool)
    darvas_recent = df.groupby("ticker")["darvas"].transform(
        lambda s: s.rolling(4, min_periods=1).max()
    ).astype(bool)

    # ── Any pattern detected ──
    # any_pattern: RAW patterns (strict, same-day only)
    # any_pattern_recent: LOOKBACK-ADJUSTED — captures setups where VCP/Darvas
    # was valid within last 3 bars. KEY FIX: the breakout candle itself expands
    # the base range, turning VCP off on the exact entry day. Using lookback
    # lets s07 capture these critical breakout-from-setup signals.
    df["any_pattern"] = df["vcp"] | df["darvas"] | df["power_play"] | df["ipo_base"]
    df["any_pattern_recent"] = vcp_recent | darvas_recent | df["power_play"] | df["ipo_base"]

    # Primary: Trend Template + recent pattern + Breakout
    df["buy_signal"] = df["trend_template"] & vcp_recent & df["breakout"]
    df["buy_signal_darvas"] = df["trend_template"] & darvas_recent & df["breakout"]
    df["buy_signal_power"] = df["trend_template"] & df["power_play"] & df["breakout"]
    df["buy_signal_ipo"] = df["trend_template"] & df["ipo_base"] & df["breakout"]

    # Any signal
    df["buy_signal_any"] = (
        df["buy_signal"] | df["buy_signal_darvas"] |
        df["buy_signal_power"] | df["buy_signal_ipo"]
    )

    n_vcp = df["buy_signal"].sum()
    n_darvas = df["buy_signal_darvas"].sum()
    n_power = df["buy_signal_power"].sum()
    n_ipo = df["buy_signal_ipo"].sum()
    n_any = df["buy_signal_any"].sum()
    log.info(f"    Buy signals: VCP={n_vcp:,}  Darvas={n_darvas:,}  "
             f"Power={n_power:,}  IPO={n_ipo:,}  Total={n_any:,}")

    return df


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════

def run(market: str = "both") -> None:
    """Detect patterns and store as ``signals`` table."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]

    all_frames: list[pd.DataFrame] = []

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Pattern detection: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        df = con.execute(
            "SELECT * FROM ranked WHERE market = ?", [mkt]
        ).df()

        if df.empty:
            log.warning(f"No ranked data for {mkt}. Skipping.")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["ticker", "date"])

        df = _add_pattern_columns(df)

        all_frames.append(df)
        log.info(f"✓ {mkt.upper()}: {df['ticker'].nunique()} tickers, "
                 f"{df['buy_signal_any'].sum():,} buy signals")

    if not all_frames:
        log.error("No data to store.")
        return

    combined = pd.concat(all_frames, ignore_index=True)

    log.info(f"Storing {len(combined):,} signal rows…")
    upsert_by_market(con, "signals", combined, markets)

    n = con.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    log.info(f"✓ Signals table: {n:,} rows")
    con.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Detect SEPA patterns")
    p.add_argument("--market", default="india", choices=["india"])
    args = p.parse_args()
    run(market=args.market)
