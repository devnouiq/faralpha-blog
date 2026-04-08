#!/usr/bin/env python3
"""
Step 07 — Signal Engine  v5.2  (Breadth + Watchlist)
=====================================================
Combines pattern detections with RS ranking, sector momentum,
market regime, breadth, AND watchlist building to produce a final
ranked list of buy candidates.

v5.2 enhancements:
  - Market breadth awareness: skip signals when breadth < 30%
  - Relative strength watchlist: during corrections, identify
    stocks resisting the decline (Minervini's "leaders in waiting")
  - Recovery priority: when market transitions from bear→bull,
    watchlist stocks with breakouts become TOP priority candidates
  - Sector exclusion: skip Utilities, Consumer Staples

Filtering hierarchy:
  1. Market regime must be bull (or recovery)
  2. Trend Template must pass
  3. RS composite ≥ top 30% (configurable)
  4. Sector in top 50% (if available)
  4b. Sector exclusion (Utilities, Consumer Staples)
  5. Breakout confirmed (price + volume)
  5b. Chase filter (skip if too extended)
  6. Pattern required (VCP / Darvas / Power Play / IPO Base)
  7. Fundamentals pass (if enabled)

ADDITIONALLY during bear/weak markets:
  - Build a watchlist of leaders resisting decline
  - These become priority candidates when market recovers

Candidates ranked by rs_composite descending.
Stores ``candidates`` + ``watchlist`` tables.

Usage:
    uv run python -m faralpha.pipeline.s07_signals --market both
"""

from __future__ import annotations

import pandas as pd
import numpy as np

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.utils.upsert import upsert_by_market

log = get_logger("s07_signals")


def _load_fundamentals(con, mkt: str) -> pd.DataFrame:
    """Load fundamental data per ticker, including acceleration check."""
    try:
        fdf = con.execute("""
            SELECT ticker, report_date, eps, eps_yoy_growth, revenue_yoy_growth,
                   roe, gross_margin, net_margin
            FROM fundamentals
            WHERE market = ?
            ORDER BY ticker, report_date
        """, [mkt]).df()
    except Exception:
        log.warning("No fundamentals table found — skipping earnings filter")
        return pd.DataFrame()

    if fdf.empty:
        return fdf

    fdf["report_date"] = pd.to_datetime(fdf["report_date"])
    fdf = fdf.sort_values(["ticker", "report_date"])

    # Compute EPS acceleration: is latest Q growth > prior Q growth?
    # Minervini: "I want to see earnings ACCELERATING, not just growing"
    if "eps_yoy_growth" in fdf.columns:
        fdf["prev_eps_yoy_growth"] = fdf.groupby("ticker")["eps_yoy_growth"].shift(1)
        fdf["eps_accelerating"] = fdf["eps_yoy_growth"] > fdf["prev_eps_yoy_growth"]

    # Keep latest 2 reports per ticker (need both for acceleration check)
    latest = fdf.groupby("ticker").tail(2)
    # But for the final join, keep only the very latest per ticker
    latest_one = fdf.sort_values("report_date").drop_duplicates(
        subset=["ticker"], keep="last"
    )
    return latest_one


def _apply_fundamental_filter(
    df: pd.DataFrame, fundamentals: pd.DataFrame, mask: pd.Series
) -> pd.Series:
    """Apply Minervini-exact earnings filter. Returns updated mask.

    Checks (all from *Trade Like a Stock Market Wizard*, Ch.10-11):
      1. EPS YoY growth ≥ min_eps_growth_qoq_pct (default 25%)
      2. Earnings ACCELERATING — latest Q growth > prior Q growth
      3. Positive revenue growth
      4. ROE ≥ min_roe_pct (default 17%)
    """
    # Market-specific override: PORTFOLIO["fundamentals_enabled"] takes precedence
    mkt_override = config.PORTFOLIO.get("fundamentals_enabled")
    if mkt_override is not None and not mkt_override:
        log.info("  Fundamentals filter: DISABLED (market-specific override)")
        return mask

    fc = config.FUNDAMENTALS
    if not fc.get("enabled", True):
        log.info("  Fundamentals filter: DISABLED in config")
        return mask

    if fundamentals.empty:
        log.info("  Fundamentals filter: no data available — skipping")
        return mask

    # Merge latest fundamentals onto signals
    fund_cols = ["ticker", "eps_yoy_growth", "revenue_yoy_growth",
                 "roe", "eps_accelerating"]
    avail = [c for c in fund_cols if c in fundamentals.columns]
    merged = df[["ticker"]].merge(
        fundamentals[avail], on="ticker", how="left"
    )

    # ── 1. EPS growth ≥ threshold ──
    min_eps = fc["min_eps_growth_qoq_pct"] / 100.0
    if "eps_yoy_growth" in merged.columns:
        eps_ok = (merged["eps_yoy_growth"] >= min_eps) | merged["eps_yoy_growth"].isna()
    else:
        eps_ok = pd.Series(True, index=df.index)

    # ── 2. EPS acceleration (latest Q > prior Q) ──
    if fc.get("require_accelerating_eps", False) and "eps_accelerating" in merged.columns:
        accel_ok = merged["eps_accelerating"].fillna(True)  # pass if no data
        log.info(f"  EPS acceleration check: {accel_ok.sum():,} pass / {len(accel_ok):,}")
    else:
        accel_ok = pd.Series(True, index=df.index)

    # ── 3. Revenue growth > 0 ──
    if fc.get("require_positive_revenue_growth", True) and "revenue_yoy_growth" in merged.columns:
        rev_ok = (merged["revenue_yoy_growth"] > 0) | merged["revenue_yoy_growth"].isna()
    else:
        rev_ok = pd.Series(True, index=df.index)

    # ── 4. ROE ≥ threshold (Minervini: ≥17%) ──
    min_roe = fc.get("min_roe_pct", 0) / 100.0
    if min_roe > 0 and "roe" in merged.columns:
        roe_ok = (merged["roe"] >= min_roe) | merged["roe"].isna()
        log.info(f"  ROE ≥ {fc['min_roe_pct']}%: {roe_ok.sum():,} pass / {len(roe_ok):,}")
    else:
        roe_ok = pd.Series(True, index=df.index)

    fund_mask = eps_ok & accel_ok & rev_ok & roe_ok
    new_mask = mask & fund_mask
    log.info(f"  After fundamentals: {new_mask.sum():,}  "
             f"(EPS≥{fc['min_eps_growth_qoq_pct']}% + accel + rev>0 + ROE≥{fc.get('min_roe_pct', 0)}%)")
    return new_mask


def _build_watchlist(
    df: pd.DataFrame, bear_or_weak: pd.Series, bc: dict, mkt: str
) -> pd.DataFrame:
    """Build watchlist of leaders resisting market decline.

    Minervini: "During corrections I watch for stocks that refuse to go down.
    These are the ones institutions are accumulating. When the market turns,
    these stocks are the first to break out."

    Criteria (user-specified Minervini rule):
      1. Market is weak/bear (bear_or_weak mask)
      2. relative_strength = stock_return_30d - market_return_30d > 5%
      3. stock_close > MA50
      4. Within 20% of 52-week high
      5. Volume contracting (supply drying up — base building)
    """
    weak_df = df[bear_or_weak].copy()
    if weak_df.empty:
        return pd.DataFrame()

    min_rs = bc.get("min_relative_strength", 0.05)
    near_high = bc.get("watchlist_near_high_pct", 0.80)

    # Compute stock 30-day return
    # Use momentum_3m as proxy (63 days ~= 3 months, close enough)
    # For more precision, use 30-day momentum from features if available
    if "momentum_3m" in weak_df.columns and "bench_return_30d" in weak_df.columns:
        # Approximate 30d return from 3m: scale roughly
        weak_df["stock_return_approx"] = weak_df["momentum_3m"] / 3.0
        weak_df["relative_strength_vs_mkt"] = (
            weak_df["stock_return_approx"] - weak_df["bench_return_30d"].fillna(0)
        )
    elif "rs_vs_bench_12m" in weak_df.columns:
        weak_df["relative_strength_vs_mkt"] = weak_df["rs_vs_bench_12m"]
    else:
        weak_df["relative_strength_vs_mkt"] = 0.0

    # Apply Minervini watchlist criteria
    wl_mask = pd.Series(True, index=weak_df.index)

    # 1. Relative strength > 5% (outperforming the market)
    wl_mask = wl_mask & (weak_df["relative_strength_vs_mkt"] > min_rs)

    # 2. Stock above its MA50
    if bc.get("watchlist_above_ma50", True) and "ma50" in weak_df.columns:
        wl_mask = wl_mask & (weak_df["close"] > weak_df["ma50"])

    # 3. Within 20% of 52-week high
    if "pct_from_52w_high" in weak_df.columns:
        # pct_from_52w_high is negative (e.g., -0.15 means 15% below high)
        wl_mask = wl_mask & (weak_df["pct_from_52w_high"] > -(1 - near_high))

    # 4. Volume contracting (supply drying up = base building)
    if bc.get("watchlist_volume_dry", True) and "vol_dryup_ratio" in weak_df.columns:
        wl_mask = wl_mask & ((weak_df["vol_dryup_ratio"] < 0.8) | weak_df["vol_dryup_ratio"].isna())

    # 5. RS composite high (top 30%)
    if "rs_composite" in weak_df.columns:
        wl_mask = wl_mask & (weak_df["rs_composite"] >= 0.70)

    watchlist = weak_df[wl_mask].copy()
    if watchlist.empty:
        return pd.DataFrame()

    # Keep useful columns
    keep_cols = ["date", "ticker", "market", "close", "ma50", "high_52w",
                 "pct_from_52w_high", "rs_composite", "relative_strength_vs_mkt",
                 "sector", "momentum_3m"]
    avail = [c for c in keep_cols if c in watchlist.columns]
    watchlist = watchlist[avail].copy()
    watchlist["market"] = mkt
    watchlist = watchlist.rename(columns={"relative_strength_vs_mkt": "relative_strength"})

    # Deduplicate: keep latest per ticker per date
    watchlist = watchlist.sort_values(["date", "rs_composite"], ascending=[True, False])
    watchlist = watchlist.drop_duplicates(subset=["date", "ticker"], keep="first")

    return watchlist


def run(market: str = "both") -> None:
    """Generate final ranked candidates table + watchlist during weak markets."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]

    all_candidates: list[pd.DataFrame] = []
    all_watchlist: list[pd.DataFrame] = []

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Signal engine: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        # Apply market-specific config so PORTFOLIO overrides
        # (e.g. fundamentals_enabled) are visible during signal filtering
        saved_cfg = config.apply_market_config(mkt)

        try:
            _run_signal_engine(con, mkt, all_candidates, all_watchlist)
        finally:
            config.restore_config(saved_cfg)

    if not all_candidates:
        log.warning("No candidates found in any market.")
        _ensure_empty_table(con, "candidates",
                            "date DATE, ticker VARCHAR, market VARCHAR, "
                            "rs_composite DOUBLE, rank_on_day INTEGER, signal_tier VARCHAR",
                            markets)
    else:
        combined = pd.concat(all_candidates, ignore_index=True)
        log.info(f"Storing {len(combined):,} candidate rows…")
        upsert_by_market(con, "candidates", combined, markets)
        n = con.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        log.info(f"✓ Candidates table: {n:,} rows")

    # ── Store watchlist (leaders during corrections) ──
    if all_watchlist:
        wl_combined = pd.concat(all_watchlist, ignore_index=True)
        upsert_by_market(con, "watchlist", wl_combined, markets)
        n_wl = len(wl_combined)
        log.info(f"✓ Watchlist table: {n_wl:,} leader candidates during corrections")
    else:
        _ensure_empty_table(con, "watchlist",
                            "date DATE, ticker VARCHAR, market VARCHAR, "
                            "relative_strength DOUBLE, rs_composite DOUBLE",
                            markets)
        log.info("  No weak-market days — watchlist empty")

    con.close()


def _run_signal_engine(
    con, mkt: str,
    all_candidates: list[pd.DataFrame],
    all_watchlist: list[pd.DataFrame],
) -> None:
    """Core signal engine for one market (config already applied)."""

    # Load signals (patterns + RS already computed)
    df = con.execute(
        "SELECT * FROM signals WHERE market = ?", [mkt]
    ).df()
    if df.empty:
        log.warning(f"No signals for {mkt}. Skipping.")
        return

    df["date"] = pd.to_datetime(df["date"])
    n_total = len(df)
    log.info(f"Loaded {n_total:,} rows, {df['ticker'].nunique()} tickers")

    # ── Load regime (now includes breadth + weak market) ──
    regime_cols = "date, is_bull, is_confirmed_bull, is_recovery, is_weak_market, breadth_pct, breadth_improving, bench_return_30d, bench_close, regime_strength"
    try:
        regime = con.execute(
            f"SELECT {regime_cols} FROM regime WHERE market = ?", [mkt]
        ).df()
    except Exception:
        regime = con.execute(
            "SELECT date, is_bull, is_recovery FROM regime WHERE market = ?", [mkt]
        ).df()

    if not regime.empty:
        regime["date"] = pd.to_datetime(regime["date"])
        df = df.merge(regime, on="date", how="left")
        df["is_bull"] = df["is_bull"].fillna(True)
        df["is_recovery"] = df["is_recovery"].fillna(False)
        if "is_weak_market" not in df.columns:
            df["is_weak_market"] = False
        else:
            df["is_weak_market"] = df["is_weak_market"].fillna(False)
        if "breadth_pct" not in df.columns:
            df["breadth_pct"] = 0.5
        if "breadth_improving" not in df.columns:
            df["breadth_improving"] = False
        if "bench_return_30d" not in df.columns:
            df["bench_return_30d"] = 0.0
    else:
        log.warning("No regime data — assuming bull market for all dates")
        df["is_bull"] = True
        df["is_recovery"] = False
        df["is_weak_market"] = False
        df["breadth_pct"] = 0.5
        df["breadth_improving"] = False
        df["bench_return_30d"] = 0.0

    # ── Load fundamentals ──
    fundamentals = _load_fundamentals(con, mkt)

    # ════════════════════════════════════════════════════
    #  WATCHLIST: Leaders resisting decline during weak markets
    # ════════════════════════════════════════════════════
    bc = config.BREADTH
    bear_or_weak = (~df["is_bull"]) | df["is_weak_market"]

    if bear_or_weak.any():
        wl = _build_watchlist(df, bear_or_weak, bc, mkt)
        if not wl.empty:
            all_watchlist.append(wl)
            n_wl_days = wl["date"].nunique()
            log.info(f"  Watchlist: {len(wl):,} leader candidates across "
                     f"{n_wl_days} correction days")

    # ════════════════════════════════════════════════════
    #  CANDIDATES: Normal buy signals during bull markets
    # ════════════════════════════════════════════════════
    log.info("Applying signal filters…")

    # 1. Bull market only
    mask = df["is_bull"]
    log.info(f"  After regime filter: {mask.sum():,} / {n_total:,}")

    # 1b. Breadth filter: skip if breadth < 30% (market internals weak)
    breadth_weak = config.BREADTH.get("breadth_weak_pct", 0.30)
    if "breadth_pct" in df.columns:
        before = mask.sum()
        breadth_ok = df["breadth_pct"] >= breadth_weak
        breadth_ok = breadth_ok | df["breadth_pct"].isna()
        mask = mask & breadth_ok
        log.info(f"  After breadth ≥ {breadth_weak*100:.0f}%: {mask.sum():,} "
                 f"(removed {before - mask.sum():,})")

    # 2. Trend Template
    mask = mask & df["trend_template"]
    log.info(f"  After Trend Template: {mask.sum():,}")

    # 3. RS composite in top percentile
    min_rs = config.RS["min_rs_percentile"]
    mask = mask & (df["rs_composite"] >= min_rs)
    log.info(f"  After RS ≥ {min_rs}: {mask.sum():,}")

    # 4. Sector momentum
    if "sector_rank" in df.columns and df["sector_rank"].notna().any():
        min_sector = config.SECTOR["min_sector_percentile"]
        sector_ok = df["sector_rank"] >= min_sector
        sector_ok = sector_ok | df["sector_rank"].isna()
        mask = mask & sector_ok
        log.info(f"  After sector ≥ {min_sector}: {mask.sum():,}")

    # 4b. Sector exclusion — Minervini avoids defensive/low-beta sectors
    exclude_sectors = config.DATA_QUALITY.get("exclude_sectors", [])
    if exclude_sectors and "sector" in df.columns:
        before = mask.sum()
        sector_excl = ~df["sector"].isin(exclude_sectors) | df["sector"].isna()
        mask = mask & sector_excl
        log.info(f"  After excluding {exclude_sectors}: {mask.sum():,} "
                 f"(removed {before - mask.sum():,})")

    # 5. Breakout confirmed (price + volume)
    mask = mask & df["breakout"]
    log.info(f"  After breakout: {mask.sum():,}")

    # 5b. Chase filter — skip if close > base_high * (1 + max_chase_pct)
    max_chase = config.PORTFOLIO.get("max_chase_pct", 0.05)
    if max_chase > 0 and "base_high" in df.columns:
        before = mask.sum()
        chase_ok = df["close"] <= df["base_high"] * (1 + max_chase)
        chase_ok = chase_ok | df["base_high"].isna()
        mask = mask & chase_ok
        log.info(f"  After chase filter (≤{max_chase*100:.0f}% above pivot): "
                 f"{mask.sum():,} (removed {before - mask.sum():,})")

    # 6. Pattern required (VCP / Darvas / Power Play / IPO Base)
    pattern_required = config.PORTFOLIO.get("pattern_required", True)
    if pattern_required:
        any_pattern_col = "any_pattern" if "any_pattern" in df.columns else None
        if any_pattern_col:
            pattern_mask = df[any_pattern_col]
        else:
            pattern_mask = pd.Series(False, index=df.index)
            for col in ["vcp", "darvas", "power_play", "ipo_base"]:
                if col in df.columns:
                    pattern_mask = pattern_mask | df[col]

        mask = mask & pattern_mask
        log.info(f"  After pattern filter: {mask.sum():,}")
    else:
        log.info(f"  Pattern filter DISABLED — keeping {mask.sum():,}")

    # 7. Fundamentals (earnings quality)
    mask = _apply_fundamental_filter(df, fundamentals, mask)

    candidates = df[mask].copy()

    # ── Inject watchlist stocks that now have breakouts ──
    if all_watchlist:
        wl_combined = pd.concat(all_watchlist, ignore_index=True)
        wl_tickers = set(wl_combined["ticker"].unique())

        inject_pattern = pd.Series(False, index=df.index)
        for col in ["vcp", "darvas", "power_play", "ipo_base", "any_pattern"]:
            if col in df.columns:
                inject_pattern = inject_pattern | df[col]

        wl_inject_mask = (
            df["is_bull"]
            & df["ticker"].isin(wl_tickers)
            & df.get("breakout", pd.Series(False, index=df.index))
            & inject_pattern
            & df["trend_template"]
            & (df.get("rs_composite", pd.Series(0, index=df.index)) >= 0.60)
            & ~mask
        )
        if exclude_sectors and "sector" in df.columns:
            wl_inject_mask = wl_inject_mask & (
                ~df["sector"].isin(exclude_sectors) | df["sector"].isna()
            )

        if wl_inject_mask.any():
            wl_inject = df[wl_inject_mask].copy()
            wl_inject["signal_tier"] = "watchlist_leader"
            if "rs_composite" in wl_inject.columns:
                wl_inject["rs_composite"] = wl_inject["rs_composite"] + 0.05
            candidates = pd.concat([candidates, wl_inject], ignore_index=True)
            log.info(f"  Injected {wl_inject_mask.sum():,} watchlist leaders "
                     f"as priority candidates")

    # ════════════════════════════════════════════════════
    #  BEAR REVERSAL CANDIDATES: RSI-2 mean reversion
    #  (Larry Connors) — capture oversold bounces in bear regimes
    # ════════════════════════════════════════════════════
    RC = config.BEAR_REVERSAL
    if RC.get("enabled", False) and "rsi_2" in df.columns:
        bear_mask = ~df["is_bull"]
        if "is_recovery" in df.columns:
            bear_mask = bear_mask & ~df["is_recovery"]

        rev_mask = bear_mask & (df["rsi_2"] < RC["rsi_entry_threshold"])
        if RC.get("require_above_ma200", True) and "ma200" in df.columns:
            rev_mask = rev_mask & (df["close"] > df["ma200"])
        if "rs_composite" in df.columns:
            rev_mask = rev_mask & (df["rs_composite"] >= RC["min_rs_percentile"])

        rev_cands = df[rev_mask].copy()
        if not rev_cands.empty:
            rev_cands["signal_tier"] = "reversal"
            rev_cands["signal_type"] = "reversal"
            if "rs_composite" in rev_cands.columns:
                rev_cands["composite_score"] = rev_cands["rs_composite"]
            else:
                rev_cands["composite_score"] = 0.5
            rev_cands["rank_on_day"] = (
                rev_cands.groupby("date")["rsi_2"]
                .rank(ascending=True, method="first")
                .astype(int)
            )
            all_candidates.append(rev_cands)
            log.info(f"  BEAR REVERSAL: {len(rev_cands):,} RSI-2 candidates "
                     f"across {rev_cands['date'].nunique()} bear days")

    if candidates.empty:
        log.warning(f"No momentum candidates for {mkt}")
        return

    # Determine signal tier
    if "signal_tier" not in candidates.columns:
        candidates["signal_tier"] = "vcp"
    tier_unset = candidates["signal_tier"].isna() | (candidates["signal_tier"] == "vcp")
    if "darvas" in candidates.columns:
        candidates.loc[tier_unset & candidates["darvas"], "signal_tier"] = "darvas"
    if "power_play" in candidates.columns:
        candidates.loc[tier_unset & candidates["power_play"], "signal_tier"] = "power_play"
    if "ipo_base" in candidates.columns:
        candidates.loc[tier_unset & candidates["ipo_base"], "signal_tier"] = "ipo_base"
    if "vcp" in candidates.columns:
        candidates.loc[tier_unset & candidates["vcp"], "signal_tier"] = "vcp"

    # ── Merge fundamentals for composite scoring ──
    if not fundamentals.empty and "eps_yoy_growth" in fundamentals.columns:
        _fund_merge = fundamentals[["ticker", "eps_yoy_growth"]].drop_duplicates("ticker")
        candidates = candidates.merge(_fund_merge, on="ticker", how="left",
                                      suffixes=("", "_fund"))
        if "eps_yoy_growth_fund" in candidates.columns:
            candidates["eps_yoy_growth"] = candidates["eps_yoy_growth_fund"]
            candidates.drop(columns=["eps_yoy_growth_fund"], inplace=True)

    # ── Compute Superperformance Composite Score ──
    def _col(name, default=0.5):
        if name in candidates.columns:
            return candidates[name].fillna(default)
        return pd.Series(default, index=candidates.index)

    _rs = _col("rs_composite", 0)
    _base = 0.5 * (1 - _col("base_range_30d", 0.15).clip(0, 0.30) / 0.30) \
          + 0.5 * (1 - _col("vol_dryup_ratio", 0.5).clip(0, 1))
    _sector = _col("sector_rank", 0.5)
    _inst = 0.5 * (_col("accum_score", 0) / 5.0).clip(0, 1) \
          + 0.5 * ((_col("volume_dominance", 1.0) - 0.5) / 2.0).clip(0, 1)
    _eps = (_col("eps_yoy_growth", 0) / 0.50).clip(0, 1)

    candidates["composite_score"] = (
        0.30 * _rs + 0.15 * _eps + 0.20 * _base
        + 0.15 * _sector + 0.20 * _inst
    )
    avg_comp = candidates["composite_score"].mean()
    log.info(f"  Composite score: avg={avg_comp:.3f}")

    # ── Rank candidates by composite superperformance score ──
    candidates["rank_on_day"] = (
        candidates
        .groupby("date")["composite_score"]
        .rank(ascending=False, method="first")
        .astype(int)
    )
    if "is_recovery" not in candidates.columns:
        candidates["is_recovery"] = False

    candidates["signal_type"] = "momentum"
    all_candidates.append(candidates)
    n_days = candidates["date"].nunique()
    tier_counts = candidates["signal_tier"].value_counts().to_dict()
    log.info(
        f"✓ {mkt.upper()}: {len(candidates):,} momentum candidate signals "
        f"across {n_days} days  tiers={tier_counts}"
    )


def _ensure_empty_table(con, table: str, schema: str, markets: list[str]) -> None:
    """Ensure table exists; delete rows for given markets."""
    exists = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema='main' AND table_name='{table}'"
    ).fetchone()[0]
    if not exists:
        con.execute(f"CREATE TABLE {table} ({schema})")
    else:
        for mkt in markets:
            con.execute(f"DELETE FROM {table} WHERE market = ?", [mkt])


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Generate ranked buy candidates")
    p.add_argument("--market", default="india", choices=["india"])
    args = p.parse_args()
    run(market=args.market)
