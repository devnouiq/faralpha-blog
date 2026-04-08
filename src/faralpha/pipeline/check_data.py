#!/usr/bin/env python3
"""
Data Quality Check
==================
Quick checks on the downloaded data to catch issues before running
the full analytics pipeline.

Usage:
    uv run python -m faralpha.pipeline.check_data
"""

from __future__ import annotations

import pandas as pd
from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("check_data")


def run() -> None:
    """Run data quality checks."""
    con = get_conn(read_only=True)

    log.info("=" * 60)
    log.info("  DATA QUALITY CHECK")
    log.info("=" * 60)

    # 1. Table existence
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    log.info(f"\nTables found: {tables}")

    issues = 0

    # 2. Stocks table
    if "stocks" in tables:
        n = con.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
        log.info(f"\n[stocks] {n:,} tickers")
        for mkt in config.MARKETS:
            nm = con.execute(
                "SELECT COUNT(*) FROM stocks WHERE market = ?", [mkt]
            ).fetchone()[0]
            log.info(f"  {mkt}: {nm:,}")
    else:
        log.error("stocks table missing!")
        issues += 1

    # 3. Prices table
    if "prices" in tables:
        for mkt in config.MARKETS:
            stats = con.execute("""
                SELECT COUNT(*) AS rows,
                       COUNT(DISTINCT ticker) AS tickers,
                       MIN(date) AS first_date,
                       MAX(date) AS last_date,
                       AVG(close) AS avg_close,
                       SUM(CASE WHEN close <= 0 THEN 1 ELSE 0 END) AS zero_close,
                       SUM(CASE WHEN volume = 0 THEN 1 ELSE 0 END) AS zero_vol
                FROM prices WHERE market = ?
            """, [mkt]).fetchdf()

            log.info(f"\n[prices — {mkt}]")
            for col in stats.columns:
                val = stats[col].iloc[0]
                if isinstance(val, float):
                    log.info(f"  {col}: {val:,.2f}")
                else:
                    log.info(f"  {col}: {val}")

            zero_close = int(stats["zero_close"].iloc[0])
            zero_vol = int(stats["zero_vol"].iloc[0])
            if zero_close > 0:
                log.warning(f"  ⚠ {zero_close} rows with close <= 0")
                issues += 1
            if zero_vol > 100:
                log.warning(f"  ⚠ {zero_vol} rows with zero volume")

        # Check for duplicates
        dupes = con.execute("""
            SELECT date, ticker, market, COUNT(*) AS cnt
            FROM prices
            GROUP BY date, ticker, market
            HAVING cnt > 1
            LIMIT 5
        """).fetchdf()
        if len(dupes) > 0:
            log.warning(f"  ⚠ Found {len(dupes)} duplicate (date, ticker) combos")
            issues += 1
    else:
        log.error("prices table missing!")
        issues += 1

    # 4. Benchmark
    if "benchmark" in tables:
        for mkt, ticker in config.BENCHMARK.items():
            n = con.execute(
                "SELECT COUNT(*) FROM benchmark WHERE ticker = ?", [ticker]
            ).fetchone()[0]
            log.info(f"\n[benchmark — {mkt}] {ticker}: {n:,} rows")
    else:
        log.warning("benchmark table missing")

    # 5. Summary
    log.info(f"\n{'=' * 60}")
    if issues == 0:
        log.info("  ✓ All checks passed — data looks good!")
    else:
        log.warning(f"  ⚠ {issues} issue(s) found — review above")
    log.info("=" * 60)

    con.close()


if __name__ == "__main__":
    run()
