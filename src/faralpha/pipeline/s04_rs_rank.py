#!/usr/bin/env python3
"""
Step 04 — Relative Strength Ranking
=====================================
Ranks every stock vs the ENTIRE market on each trading day.

Minervini's core principle: only trade stocks that are *already*
outperforming the market. Most superperformers were in the top 10-20%
of relative strength BEFORE their biggest moves.

Computes:
  - Cross-sectional momentum percentile (12m, 6m, 3m)
  - Weighted composite RS score
  - RS vs benchmark (excess return over index)
  - Sector-level momentum rank

Usage:
    uv run python -m faralpha.pipeline.s04_rs_rank --market both
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.utils.upsert import upsert_by_market

log = get_logger("s04_rs_rank")


def _add_benchmark_rs(df: pd.DataFrame, bench: pd.DataFrame) -> pd.DataFrame:
    """Compute relative strength vs the benchmark index."""
    if bench.empty:
        df["rs_vs_bench_12m"] = np.nan
        df["rs_vs_bench_6m"] = np.nan
        return df

    bench = bench.sort_values("date").copy()
    bench["bench_mom_12m"] = bench["close"].pct_change(252)
    bench["bench_mom_6m"] = bench["close"].pct_change(126)
    bench = bench[["date", "bench_mom_12m", "bench_mom_6m"]]

    df = df.merge(bench, on="date", how="left")
    df["rs_vs_bench_12m"] = df["momentum_12m"] - df["bench_mom_12m"]
    df["rs_vs_bench_6m"] = df["momentum_6m"] - df["bench_mom_6m"]
    df.drop(columns=["bench_mom_12m", "bench_mom_6m"], inplace=True)

    return df


def _add_sector_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """Compute sector-level momentum rank."""
    if "sector" not in df.columns or df["sector"].isna().all():
        df["sector_momentum_12m"] = np.nan
        df["sector_rank"] = np.nan
        return df

    sector_mom = (
        df.groupby(["date", "sector"])["momentum_12m"]
        .mean()
        .reset_index()
        .rename(columns={"momentum_12m": "sector_momentum_12m"})
    )
    sector_mom["sector_rank"] = (
        sector_mom.groupby("date")["sector_momentum_12m"]
        .rank(pct=True)
    )
    df = df.merge(
        sector_mom[["date", "sector", "sector_momentum_12m", "sector_rank"]],
        on=["date", "sector"],
        how="left",
    )
    return df


def run(market: str = "both") -> None:
    """Compute relative strength rankings and store as ``ranked`` table."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]

    all_frames: list[pd.DataFrame] = []

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Relative strength ranking: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        df = con.execute(
            "SELECT * FROM features WHERE market = ?", [mkt]
        ).df()

        if df.empty:
            log.warning(f"No features for {mkt}. Skipping.")
            continue

        df["date"] = pd.to_datetime(df["date"])

        # ── Cross-sectional percentile ranks ──
        log.info("Computing cross-sectional RS ranks…")
        df["rs_rank_12m"] = df.groupby("date")["momentum_12m"].rank(pct=True)
        df["rs_rank_9m"] = df.groupby("date")["momentum_9m"].rank(pct=True)
        df["rs_rank_6m"] = df.groupby("date")["momentum_6m"].rank(pct=True)
        df["rs_rank_3m"] = df.groupby("date")["momentum_3m"].rank(pct=True)

        # Composite RS — Minervini weights RECENT performance heaviest
        # "What a stock did in the last quarter tells you far more
        #  than what it did over the past year" — Think & Trade
        df["rs_composite"] = (
            0.40 * df["rs_rank_3m"].fillna(0)
            + 0.30 * df["rs_rank_6m"].fillna(0)
            + 0.20 * df["rs_rank_9m"].fillna(0)
            + 0.10 * df["rs_rank_12m"].fillna(0)
        )

        # ── RS vs benchmark ──
        bench_ticker = config.BENCHMARK.get(mkt)
        bench = pd.DataFrame()
        if bench_ticker:
            bench = con.execute(
                "SELECT date, close FROM benchmark WHERE ticker = ?",
                [bench_ticker],
            ).df()
            bench["date"] = pd.to_datetime(bench["date"])

        df = _add_benchmark_rs(df, bench)
        log.info("  RS vs benchmark computed")

        # ── Sector momentum ──
        df = _add_sector_momentum(df)
        log.info("  Sector momentum computed")

        all_frames.append(df)
        log.info(f"✓ {mkt.upper()}: {df['ticker'].nunique()} tickers ranked")

    if not all_frames:
        log.error("No data to store.")
        return

    combined = pd.concat(all_frames, ignore_index=True)

    log.info(f"Storing {len(combined):,} ranked rows…")
    upsert_by_market(con, "ranked", combined, markets)

    n = con.execute("SELECT COUNT(*) FROM ranked").fetchone()[0]
    log.info(f"✓ Ranked table: {n:,} rows")
    con.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Compute relative strength rankings")
    p.add_argument("--market", default="india", choices=["india"])
    args = p.parse_args()
    run(market=args.market)
