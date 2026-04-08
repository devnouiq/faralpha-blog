#!/usr/bin/env python3
"""
Pipeline Orchestrator
=====================
Runs the full pipeline end-to-end in the correct order.

Usage:
    uv run python -m faralpha.pipeline.run_all --market both
    uv run python -m faralpha.pipeline.run_all --market india --skip-download
"""

from __future__ import annotations

import time

from faralpha.utils.logger import get_logger

log = get_logger("orchestrator")


def run(market: str = "both", skip_download: bool = False,
        full_history: bool = False) -> None:
    """Execute the full pipeline.

    Args:
        market: Which market to process.
        skip_download: Skip universe + price download steps.
        full_history: Compute features from ALL price data (for backtesting).
    """
    t0 = time.time()

    steps = []

    if not skip_download:
        steps.append(("01 Universe",     "faralpha.pipeline.s01_universe",  "run"))
        steps.append(("02 Prices",       "faralpha.pipeline.s02_prices",    "run"))
        steps.append(("02b Fundamentals","faralpha.pipeline.s02b_fundamentals", "run"))

    steps += [
        ("03 Features",      "faralpha.pipeline.s03_features",   "run"),
        ("04 RS Ranking",    "faralpha.pipeline.s04_rs_rank",    "run"),
        ("05 Patterns",      "faralpha.pipeline.s05_patterns",   "run"),
        ("06 Regime",        "faralpha.pipeline.s06_regime",     "run"),
        ("07 Signals",       "faralpha.pipeline.s07_signals",    "run"),
    ]

    # Backtest only runs with --full-history to avoid overwriting
    # full-history results with truncated 2.5-year data
    if full_history:
        steps.append(
            ("08 Backtest",  "faralpha.pipeline.s08_backtest",   "run"),
        )
    else:
        log.info("Skipping backtest (use --full-history for backtesting)")

    for i, (name, module_path, func_name) in enumerate(steps, 1):
        log.info(f"\n{'▓' * 60}")
        log.info(f"  STEP {i}/{len(steps)}: {name}")
        log.info(f"{'▓' * 60}")

        t1 = time.time()
        try:
            import importlib
            mod = importlib.import_module(module_path)
            fn = getattr(mod, func_name)

            # Each step accepts market= kwarg
            if "universe" in module_path:
                fn(market=market, enrich=False)  # enrich is slow; skip by default
            elif "s03_features" in module_path:
                fn(market=market, full_history=full_history)
            else:
                fn(market=market)

            elapsed = time.time() - t1
            log.info(f"  ✓ {name} completed in {elapsed:.1f}s")

        except Exception as exc:
            elapsed = time.time() - t1
            log.error(f"  ✗ {name} FAILED after {elapsed:.1f}s: {exc}")
            raise

    total = time.time() - t0
    log.info(f"\n{'▓' * 60}")
    log.info(f"  PIPELINE COMPLETE — {total:.0f}s total")
    log.info(f"{'▓' * 60}")

    # Print summary
    _print_summary()


def _print_summary() -> None:
    """Print a quick summary of all tables."""
    from faralpha.utils.db import get_conn

    con = get_conn()
    log.info("\n  Table Summary:")
    for tbl in ["stocks", "prices", "benchmark", "fundamentals", "features", "ranked",
                "signals", "regime", "candidates"]:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"    {tbl:20s}  {n:>12,} rows")
        except Exception:
            log.info(f"    {tbl:20s}  (not created)")

    for mkt in ("india",):
        for suffix in ("equity", "trades", "annual"):
            tbl = f"backtest_{suffix}_{mkt}"
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                log.info(f"    {tbl:30s}  {n:>12,} rows")
            except Exception:
                pass

    con.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Run full quant pipeline")
    p.add_argument("--market", default="india", choices=["india"])
    p.add_argument("--skip-download", action="store_true",
                   help="Skip universe + price download (use existing data)")
    p.add_argument("--full-history", action="store_true",
                   help="Compute features from ALL price data (for backtesting)")
    args = p.parse_args()
    run(market=args.market, skip_download=args.skip_download,
        full_history=args.full_history)
