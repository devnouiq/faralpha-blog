#!/usr/bin/env python3
"""
Step 02 — Price Ingestion
=========================
Downloads OHLCV data for EVERY ticker in the ``stocks`` table using
yfinance, plus the benchmark indices (Nifty 500).

Features:
  - Incremental: only fetches dates after the last row already stored
  - Data-quality filters (extreme returns, zero close, etc.)
  - Downloads benchmark indices for regime detection
  - Robust: skips failed tickers, logs progress every 100

Usage:
    uv run python -m faralpha.pipeline.s02_prices --market india
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

from faralpha import config
from faralpha.utils.db import get_conn, init_schema
from faralpha.utils.logger import get_logger

log = get_logger("s02_prices")

# MUST be 1 — yfinance.download() is NOT thread-safe.
# Even 2 threads cause data to mix between tickers (identical prices
# for adjacent tickers, wrong row counts, etc.).
_MAX_WORKERS = 1


class YFinanceAPIError(Exception):
    """Raised when yfinance returns an auth / rate-limit / server error.

    These are transient Yahoo Finance issues — NOT an indicator that
    the ticker is delisted.  Callers should retry later rather than
    incrementing the delist failure counter.
    """
    pass


# ═══════════════════════════════════════════════════════════
#  SINGLE-TICKER DOWNLOAD
# ═══════════════════════════════════════════════════════════

def _yf_symbol(ticker: str, market: str) -> str:
    """Our ticker → Yahoo Finance symbol."""
    return f"{ticker}{config.YF_SUFFIX.get(market, '')}"


def _clean(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Apply data-quality filters to raw OHLCV."""
    if df.empty:
        return df

    df = df[df["close"] > 0].dropna(subset=["close"]).copy()

    # Remove extreme 1-day returns (bad data / circuit-break artefacts)
    ret = df["close"].pct_change().abs()
    bad = ret > config.DATA_QUALITY["max_daily_return"]
    n_bad = int(bad.sum())
    if n_bad:
        log.debug(f"  {ticker}: dropped {n_bad} extreme-return rows")
        df = df[~bad]

    return df


def _detect_scale_jump(
    df_new: pd.DataFrame, con, ticker: str, market: str
) -> bool:
    """Return True if new data has a scale jump vs existing DB prices.

    Yahoo Finance sometimes returns historical data at face-value scale
    while latest prices are at market scale (or vice versa). This creates
    discontinuities like 75 → 27,030 which corrupt all momentum/RS calcs.

    When detected, the caller should purge the ticker's history and
    re-download from scratch.
    """
    if df_new.empty:
        return False

    # Get the last few prices from the DB
    db_prices = con.execute(
        "SELECT close FROM prices WHERE ticker = ? AND market = ? "
        "ORDER BY date DESC LIMIT 10",
        [ticker, market],
    ).fetchall()

    if not db_prices:
        return False

    db_median = float(sorted([r[0] for r in db_prices])[len(db_prices) // 2])
    new_first = float(df_new.sort_values("date").iloc[0]["close"])

    if db_median <= 0:
        return False

    ratio = new_first / db_median
    # A ratio > 3x or < 0.33x means the data scale changed
    if ratio > 3.0 or ratio < 0.33:
        log.warning(
            f"  {ticker}: SCALE JUMP detected — DB median={db_median:,.2f}, "
            f"new first={new_first:,.2f} (ratio={ratio:.1f}x). "
            f"Purging old data and re-downloading."
        )
        return True
    return False


def download_ticker(
    ticker: str,
    market: str,
    start: str,
    end: str | None = None,
) -> pd.DataFrame:
    """Download OHLCV for one ticker. Returns empty DF on failure."""
    symbol = _yf_symbol(ticker, market)
    try:
        raw = yf.download(
            symbol, start=start, end=end,
            progress=False, auto_adjust=True, timeout=20,
        )
        if raw.empty:
            return pd.DataFrame()

        # yfinance sometimes returns MultiIndex columns
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw.reset_index()
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Normalise column names (yfinance v1 vs v2 differences)
        rename = {}
        for c in df.columns:
            if "date" in c:
                rename[c] = "date"
        df = df.rename(columns=rename)

        required = ["date", "open", "high", "low", "close", "volume"]
        if not all(c in df.columns for c in required):
            return pd.DataFrame()

        df = df[required].copy()
        df["ticker"] = ticker
        df["market"] = market
        df["date"] = pd.to_datetime(df["date"]).dt.date

        return _clean(df, ticker)

    except Exception as exc:
        msg = str(exc)
        # Detect transient Yahoo Finance API errors
        if any(k in msg for k in (
            "401", "Unauthorized", "Invalid Crumb",
            "403", "Forbidden", "429", "Too Many",
            "500", "502", "503", "504",
        )):
            raise YFinanceAPIError(
                f"{ticker}: Yahoo API error — {msg}"
            ) from exc
        log.debug(f"  {ticker}: download failed — {exc}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════
#  BENCHMARK DOWNLOAD
# ═══════════════════════════════════════════════════════════

def _download_benchmark(con, market: str) -> None:
    """Download benchmark index (Nifty 50 / S&P 500)."""
    bench = config.BENCHMARK.get(market)
    if not bench:
        return

    log.info(f"Downloading benchmark {bench}")
    try:
        raw = yf.download(
            bench, start=config.DATA_START, progress=False, auto_adjust=True,
        )
        if raw.empty:
            log.warning(f"  Benchmark {bench} returned empty")
            return

        # Flatten MultiIndex columns (yfinance may return ("Close", "^NSEI"))
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        # Build DataFrame directly from index + Close column.
        # This avoids reset_index() / column-rename mismatches when
        # yfinance returns duplicate column names after flattening.
        close_series = raw["Close"]
        if isinstance(close_series, pd.DataFrame):
            close_series = close_series.iloc[:, 0]  # take first if duplicated

        df = pd.DataFrame({
            "date": pd.to_datetime(raw.index).date,
            "ticker": bench,
            "close": close_series.values,
        })
        df = df.dropna(subset=["close"])

        if df.empty:
            log.warning(f"  Benchmark {bench} has no valid close data")
            return

        # Incremental upsert: only replace the date range we downloaded.
        # This preserves historical benchmark data when Yahoo returns
        # partial results (e.g. 401 errors limiting to recent data).
        min_date = df["date"].min()
        max_date = df["date"].max()
        con.execute(
            "DELETE FROM benchmark WHERE ticker = ? AND date >= ? AND date <= ?",
            [bench, min_date, max_date],
        )
        con.execute("INSERT INTO benchmark SELECT date, ticker, close FROM df")
        total = con.execute(
            "SELECT COUNT(*) FROM benchmark WHERE ticker = ?", [bench]
        ).fetchone()[0]
        log.info(f"  Benchmark {bench}: {len(df)} rows upserted ({total} total)")
    except Exception as exc:
        log.error(f"  Benchmark {bench} failed: {exc}")


# ═══════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════

def _bulk_last_dates(con, market: str) -> dict[str, pd.Timestamp | None]:
    """Get the most recent price date for every ticker in one query."""
    df = con.execute("""
        SELECT ticker, MAX(date) AS last_date
        FROM prices
        WHERE market = ?
        GROUP BY ticker
    """, [market]).df()
    return {
        row["ticker"]: pd.Timestamp(row["last_date"]) if row["last_date"] else None
        for _, row in df.iterrows()
    }


# After this many consecutive download failures a ticker is auto-delisted
_DELIST_AFTER_N_FAILS = 3


def _validate_no_scale_jumps(con, market: str, threshold: float = 3.0) -> list[str]:
    """Post-download validation: find tickers with >threshold-x single-day jumps.

    Returns list of ticker names that still have scale corruption.
    """
    df = con.execute("""
        SELECT ticker, date, close,
               LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
        FROM prices
        WHERE market = ?
    """, [market]).df()

    df = df.dropna(subset=["prev_close"])
    df = df[df["prev_close"] > 0]
    df["ratio"] = df["close"] / df["prev_close"]
    bad = df[(df["ratio"] > threshold) | (df["ratio"] < 1.0 / threshold)]
    return sorted(bad["ticker"].unique().tolist())


def run(
    market: str = "both",
    sleep: float = 0.05,
    batch_log: int = 100,
    max_workers: int = _MAX_WORKERS,
    force: bool = False,
) -> None:
    """
    Download prices for all tickers in the universe (multi-threaded).
    Incremental: only fetches dates after the last row already stored.
    Auto-delists tickers after repeated download failures.

    Args:
        market:      'india', 'us', or 'both'
        sleep:       seconds between yfinance calls (rate-limit, per thread)
        batch_log:   log progress every N tickers
        max_workers: number of parallel download threads
        force:       purge ALL existing price data for the market before downloading
    """
    con = get_conn()
    init_schema(con)

    markets = config.MARKETS if market == "both" else [market]

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Price ingestion: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        # Force mode: purge all existing price data for this market
        if force:
            n_purged = con.execute(
                "SELECT COUNT(*) FROM prices WHERE market = ?", [mkt]
            ).fetchone()[0]
            con.execute("DELETE FROM prices WHERE market = ?", [mkt])
            # Also purge downstream tables that depend on prices
            for tbl in ["features", "ranked", "signals", "watchlist", "candidates"]:
                try:
                    con.execute(f"DELETE FROM {tbl} WHERE market = ?", [mkt])
                except Exception:
                    try:
                        con.execute(f"DELETE FROM {tbl}")
                    except Exception:
                        pass
            # Purge backtest tables for this market
            for tbl in [f"backtest_annual_{mkt}", f"backtest_equity_{mkt}", f"backtest_trades_{mkt}"]:
                try:
                    con.execute(f"DELETE FROM {tbl}")
                except Exception:
                    pass
            log.warning(f"  ⚠ FORCE MODE: purged {n_purged:,} price rows + downstream tables for {mkt}")

        tickers = con.execute(
            "SELECT ticker FROM stocks WHERE market = ? AND delisting_date IS NULL ORDER BY ticker",
            [mkt],
        ).df()["ticker"].tolist()

        if not tickers:
            log.error(f"No tickers for {mkt}. Run s01_universe first.")
            continue

        # Bulk-query last dates (single SQL instead of N queries)
        last_dates = _bulk_last_dates(con, mkt)
        ticker_starts = {}
        for t in tickers:
            last = last_dates.get(t)
            if last is not None:
                ticker_starts[t] = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                ticker_starts[t] = config.DATA_START

        log.info(f"Downloading {len(tickers)} tickers ({max_workers} threads)")

        ok, fail, total_rows = 0, 0, 0
        newly_delisted: list[str] = []
        results = []

        def _download_one(ticker):
            start = ticker_starts[ticker]
            time.sleep(sleep)
            return ticker, download_ticker(ticker, mkt, start=start, end=config.DATA_END)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_download_one, t): t for t in tickers}
            pbar = tqdm(as_completed(futures), total=len(tickers), desc=f"prices [{mkt}]")
            for future in pbar:
                ticker = futures[future]
                try:
                    _, df = future.result()
                    results.append((ticker, df))
                except Exception as exc:
                    log.debug(f"  {ticker}: thread failed — {exc}")
                    results.append((ticker, pd.DataFrame()))

        # Insert results sequentially (DuckDB is single-writer)
        scale_redownloads: list[str] = []
        for ticker, df in results:
            if df.empty:
                fail += 1
                # Track consecutive failures → auto-delist
                con.execute("""
                    UPDATE stocks
                    SET sync_fail_count = COALESCE(sync_fail_count, 0) + 1
                    WHERE ticker = ? AND market = ?
                """, [ticker, mkt])
                fc = con.execute(
                    "SELECT sync_fail_count FROM stocks WHERE ticker = ? AND market = ?",
                    [ticker, mkt],
                ).fetchone()
                if fc and fc[0] >= _DELIST_AFTER_N_FAILS:
                    con.execute("""
                        UPDATE stocks SET delisting_date = CURRENT_DATE
                        WHERE ticker = ? AND market = ?
                    """, [ticker, mkt])
                    newly_delisted.append(ticker)
                    log.warning(f"  ⛔ {ticker} marked DELISTED (failed {fc[0]} times)")
            else:
                try:
                    # Check for Yahoo Finance scale corruption before inserting
                    if _detect_scale_jump(df, con, ticker, mkt):
                        # Purge old data — it's at the wrong scale
                        n_purged = con.execute(
                            "SELECT COUNT(*) FROM prices WHERE ticker = ? AND market = ?",
                            [ticker, mkt],
                        ).fetchone()[0]
                        con.execute(
                            "DELETE FROM prices WHERE ticker = ? AND market = ?",
                            [ticker, mkt],
                        )
                        # Re-download full history for this ticker
                        log.info(f"  {ticker}: purged {n_purged} rows, re-downloading full history...")
                        df_full = download_ticker(ticker, mkt, start=config.DATA_START, end=config.DATA_END)
                        if not df_full.empty:
                            con.execute("""
                                INSERT OR IGNORE INTO prices
                                SELECT date, ticker, open, high, low, close, volume, market
                                FROM df_full
                            """)
                            scale_redownloads.append(ticker)
                            ok += 1
                            total_rows += len(df_full)
                        else:
                            log.warning(f"  {ticker}: full re-download returned empty!")
                            fail += 1
                    else:
                        con.execute("""
                            INSERT OR IGNORE INTO prices
                            SELECT date, ticker, open, high, low, close, volume, market
                            FROM df
                        """)
                        ok += 1
                        total_rows += len(df)
                    # Reset failure counter on success
                    con.execute("""
                        UPDATE stocks SET sync_fail_count = 0
                        WHERE ticker = ? AND market = ? AND COALESCE(sync_fail_count, 0) > 0
                    """, [ticker, mkt])
                except Exception as exc:
                    log.debug(f"  {ticker}: insert failed — {exc}")
                    fail += 1

        msg = f"{ok} downloaded, {fail} failed, {total_rows} new rows"
        if scale_redownloads:
            msg += f", {len(scale_redownloads)} scale-corrected: {scale_redownloads[:10]}"
        if newly_delisted:
            msg += f", {len(newly_delisted)} newly delisted: {newly_delisted}"
        log.info(f"✓ {mkt.upper()} done: {msg}")

        # ── Post-download validation: detect any remaining scale jumps ──
        log.info(f"Running post-download scale-jump validation for {mkt}...")
        jump_tickers = _validate_no_scale_jumps(con, mkt)
        if jump_tickers:
            log.error(
                f"⚠ {len(jump_tickers)} tickers still have scale jumps after download! "
                f"First 20: {jump_tickers[:20]}"
            )
        else:
            log.info(f"✓ No scale jumps detected — all {mkt} data looks clean")

        # Benchmark
        _download_benchmark(con, mkt)

    # Summary
    price_count = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    ticker_count = con.execute("SELECT COUNT(DISTINCT ticker) FROM prices").fetchone()[0]
    bench_count = con.execute("SELECT COUNT(*) FROM benchmark").fetchone()[0]
    log.info(f"Database totals: {price_count:,} price rows, {ticker_count} tickers, {bench_count} benchmark rows")
    con.close()


# ═══════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Download OHLCV prices")
    p.add_argument(
        "--market", default="india", choices=["india"],
    )
    p.add_argument(
        "--sleep", type=float, default=0.08,
        help="Seconds between yfinance calls (default: 0.08)",
    )
    p.add_argument(
        "--batch-log", type=int, default=100,
        help="Log progress every N tickers (default: 100)",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Purge ALL existing price data for the market and re-download from scratch",
    )
    args = p.parse_args()
    run(market=args.market, sleep=args.sleep, batch_log=args.batch_log, force=args.force)
