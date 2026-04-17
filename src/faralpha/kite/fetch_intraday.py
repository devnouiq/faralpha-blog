#!/usr/bin/env python3
"""
Kite Intraday Data Fetcher
===========================
Downloads 15min / 30min / 60min OHLCV candles from Zerodha Kite
and stores them in DuckDB's ``intraday_candles`` table.

Kite API limits per request:
  - 15min candles: max 200 days window
  - 30min candles: max 200 days window
  - 60min candles: max 400 days window
  - Rate limit: 3 requests/second

For longer history, use --from-year to loop through date chunks
automatically.  e.g. ``--from-year 2016`` fetches in 200-day chunks
from 2016 to today.

Usage:
    # Fetch recent data (single chunk)
    uv run python -m faralpha.kite.fetch_intraday --interval 15minute

    # Fetch long history in chunks from 2016
    uv run python -m faralpha.kite.fetch_intraday --interval 15minute --from-year 2016

    # All intervals, long history
    uv run python -m faralpha.kite.fetch_intraday --all-intervals --from-year 2016
"""

from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Lock

import pandas as pd
from tqdm import tqdm

from faralpha import config
from faralpha.config import use_postgres_database
from faralpha.kite.intraday_db import (
    get_conn as get_intraday_conn,
    init_schema as init_intraday_schema,
    upsert_candles_from_dataframe,
)
from faralpha.utils.db import get_conn as get_market_conn
from faralpha.utils.logger import get_logger

log = get_logger("kite_intraday")

KITE_CFG = config.KITE

# Kite instrument token cache: ticker -> instrument_token
_instrument_cache: dict[str, int] = {}


def _get_kite():
    """Create authenticated KiteConnect instance."""
    from kiteconnect import KiteConnect

    api_key = KITE_CFG["api_key"]
    access_token = KITE_CFG["access_token"]

    if not api_key or not access_token:
        raise ValueError(
            "KITE_API_KEY and KITE_ACCESS_TOKEN must be set. "
            "Run the Kite login flow first to get an access token."
        )

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _load_instrument_map(kite) -> dict[str, int]:
    """Build ticker -> instrument_token map for NSE stocks."""
    global _instrument_cache
    if _instrument_cache:
        return _instrument_cache

    instruments = kite.instruments("NSE")
    for inst in instruments:
        # Kite uses tradingsymbol (e.g., "RELIANCE"), we store with .NS suffix
        symbol = inst["tradingsymbol"]
        ticker = f"{symbol}.NS"
        _instrument_cache[ticker] = inst["instrument_token"]

    log.info("Loaded %d NSE instrument tokens", len(_instrument_cache))
    return _instrument_cache


def fetch_intraday_candles(
    tickers: list[str] | None = None,
    interval: str = "15minute",
    lookback_days: int | None = None,
) -> int:
    """Fetch intraday candles from Kite and store in DuckDB.

    Args:
        tickers: List of tickers (e.g. ["RELIANCE.NS"]). None = all universe.
        interval: Kite interval string: "15minute", "30minute", "60minute".
        lookback_days: How many calendar days to fetch. None = use config default.

    Returns:
        Number of new candles inserted.
    """
    kite = _get_kite()
    inst_map = _load_instrument_map(kite)

    if lookback_days is None:
        lookback_days = KITE_CFG["intraday_lookback_days"]

    # Intraday data goes to separate database
    icon = get_intraday_conn()
    init_intraday_schema(icon)

    # Get tickers from main market database
    if tickers is None:
        mcon = get_market_conn(read_only=True)
        rows = mcon.execute(
            "SELECT ticker FROM stocks WHERE market = 'india'"
        ).fetchall()
        tickers = [r[0] for r in rows]
        mcon.close()

    rate_limit = KITE_CFG["rate_limit_per_sec"]
    to_date = datetime.now()
    from_date = to_date - timedelta(days=lookback_days)

    total_inserted = 0
    skipped = 0

    for ticker in tqdm(tickers, desc=f"Kite {interval}"):
        token = inst_map.get(ticker)
        if token is None:
            # Try without .NS suffix
            bare = ticker.replace(".NS", "")
            token = inst_map.get(f"{bare}.NS") or inst_map.get(bare)
            if token is None:
                skipped += 1
                continue

        # Check last stored candle for this ticker+interval
        try:
            row = icon.execute(
                "SELECT MAX(ts) FROM candles "
                "WHERE ticker = ? AND interval = ?",
                [ticker, interval],
            ).fetchone()
            last_ts = row[0] if row else None
        except Exception:
            last_ts = None

        fetch_from = from_date
        if last_ts is not None:
            # Only fetch new data
            fetch_from = max(from_date, last_ts + timedelta(minutes=1))

        if fetch_from >= to_date:
            continue

        try:
            data = kite.historical_data(
                instrument_token=token,
                from_date=fetch_from.strftime("%Y-%m-%d"),
                to_date=to_date.strftime("%Y-%m-%d"),
                interval=interval,
            )
        except Exception as e:
            log.warning("Kite error for %s: %s", ticker, e)
            time.sleep(1.0 / rate_limit)
            continue

        if not data:
            time.sleep(1.0 / rate_limit)
            continue

        # Build DataFrame
        df = pd.DataFrame(data)
        df = df.rename(columns={"date": "ts"})
        df["ticker"] = ticker
        df["interval"] = interval
        df["ts"] = pd.to_datetime(df["ts"])
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).clip(upper=2**63 - 1).astype("int64")

        # Upsert into intraday DB
        n_before = icon.execute(
            "SELECT COUNT(*) FROM candles "
            "WHERE ticker = ? AND interval = ?",
            [ticker, interval],
        ).fetchone()[0]

        if use_postgres_database():
            upsert_candles_from_dataframe(icon, df)
        else:
            icon.execute(
                "INSERT OR REPLACE INTO candles "
                "(ts, ticker, interval, open, high, low, close, volume) "
                "SELECT ts, ticker, interval, open, high, low, close, volume "
                "FROM df"
            )

        n_after = icon.execute(
            "SELECT COUNT(*) FROM candles "
            "WHERE ticker = ? AND interval = ?",
            [ticker, interval],
        ).fetchone()[0]

        inserted = n_after - n_before
        total_inserted += inserted

        # Respect rate limit
        time.sleep(1.0 / rate_limit)

    log.info(
        "Kite %s: %d candles inserted, %d tickers skipped (no instrument token)",
        interval, total_inserted, skipped,
    )
    icon.close()
    return total_inserted


# Kite rate limit: 3 req/sec shared across all threads
_rate_lock = Lock()
_last_request_times: list[float] = []


def _rate_limited_fetch(kite, token, from_date, to_date, interval):
    """Fetch from Kite with global rate limiting (3 req/sec)."""
    with _rate_lock:
        now = time.time()
        # Keep track of last 3 requests
        _last_request_times[:] = [t for t in _last_request_times if now - t < 1.0]
        if len(_last_request_times) >= 3:
            sleep_for = 1.0 - (now - _last_request_times[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
        _last_request_times.append(time.time())

    return kite.historical_data(
        instrument_token=token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
    )


# Max days per single Kite API request, by interval
MAX_CHUNK_DAYS = {
    "15minute": 200,
    "30minute": 200,
    "60minute": 400,
}


def fetch_intraday_chunked(
    tickers: list[str] | None = None,
    interval: str = "15minute",
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    use_interval_db: bool = False,
) -> int:
    """Fetch intraday candles using chunked date windows for long history.

    Automatically splits the date range into windows that fit Kite's
    per-request limit and loops through them oldest-first.  Skips
    date ranges that already have data in DB.

    Args:
        tickers: List of tickers. None = all universe.
        interval: Kite interval string.
        from_date: Start of history. None = default lookback.
        to_date: End of history. None = now.

    Returns:
        Total candles inserted.
    """
    kite = _get_kite()
    inst_map = _load_instrument_map(kite)

    if use_interval_db:
        from faralpha.kite.intraday_db import get_interval_conn, init_interval_schema
        init_interval_schema(interval)
        icon = get_interval_conn(interval)
    else:
        icon = get_intraday_conn()
        init_intraday_schema(icon)

    if tickers is None:
        # Load ticker list — try cached file first (avoids DuckDB lock conflict
        # when multiple interval fetchers run in parallel).
        import json as _json
        _cache = config.PROJECT_ROOT / "db" / ".ticker_cache.json"
        if _cache.exists():
            tickers = _json.loads(_cache.read_text())
            log.info("Loaded %d tickers from cache", len(tickers))
        else:
            mcon = get_market_conn(read_only=True)
            rows = mcon.execute(
                "SELECT ticker FROM stocks WHERE market = 'india'"
            ).fetchall()
            tickers = [r[0] for r in rows]
            mcon.close()
            _cache.write_text(_json.dumps(tickers))
            log.info("Cached %d tickers to %s", len(tickers), _cache)

    if to_date is None:
        to_date = datetime.now()
    if from_date is None:
        chunk = MAX_CHUNK_DAYS.get(interval, 200)
        from_date = to_date - timedelta(days=chunk)

    chunk_days = MAX_CHUNK_DAYS.get(interval, 200)
    total_days = (to_date - from_date).days
    n_chunks = math.ceil(total_days / chunk_days)

    # Build date windows oldest-first
    windows = []
    cursor = from_date
    while cursor < to_date:
        win_end = min(cursor + timedelta(days=chunk_days), to_date)
        windows.append((cursor, win_end))
        cursor = win_end

    log.info(
        "Chunked fetch %s: %s → %s (%d days, %d chunks × %d tickers = %d API calls)",
        interval,
        from_date.strftime("%Y-%m-%d"),
        to_date.strftime("%Y-%m-%d"),
        total_days,
        len(windows),
        len(tickers),
        len(windows) * len(tickers),
    )

    rate_limit = KITE_CFG["rate_limit_per_sec"]
    total_inserted = 0
    skipped = 0

    for ticker in tqdm(tickers, desc=f"Kite {interval} (chunked)"):
        token = inst_map.get(ticker)
        if token is None:
            bare = ticker.replace(".NS", "")
            token = inst_map.get(f"{bare}.NS") or inst_map.get(bare)
            if token is None:
                skipped += 1
                continue

        # Check what we already have for this ticker+interval
        try:
            row = icon.execute(
                "SELECT MIN(ts), MAX(ts) FROM candles "
                "WHERE ticker = ? AND interval = ?",
                [ticker, interval],
            ).fetchone()
            existing_min = row[0] if row and row[0] else None
            existing_max = row[1] if row and row[1] else None
        except Exception:
            existing_min = existing_max = None

        for win_start, win_end in windows:
            # Skip if we already have data covering this window
            if existing_min is not None and existing_max is not None:
                if win_start >= existing_min and win_end <= existing_max + timedelta(days=1):
                    continue

            try:
                data = _rate_limited_fetch(
                    kite, token,
                    win_start.strftime("%Y-%m-%d"),
                    win_end.strftime("%Y-%m-%d"),
                    interval,
                )
            except Exception as e:
                log.warning("Kite error %s %s [%s→%s]: %s",
                            ticker, interval, win_start.date(), win_end.date(), e)
                continue

            if not data:
                continue

            df = pd.DataFrame(data)
            df = df.rename(columns={"date": "ts"})
            df["ticker"] = ticker
            df["interval"] = interval
            df["ts"] = pd.to_datetime(df["ts"])
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).clip(upper=2**63 - 1).astype("int64")

            if use_postgres_database():
                upsert_candles_from_dataframe(icon, df)
            else:
                icon.execute(
                    "INSERT OR REPLACE INTO candles "
                    "(ts, ticker, interval, open, high, low, close, volume) "
                    "SELECT ts, ticker, interval, open, high, low, close, volume "
                    "FROM df"
                )
            total_inserted += len(df)

    log.info(
        "Kite %s (chunked): %d candles inserted, %d tickers skipped",
        interval, total_inserted, skipped,
    )
    if use_interval_db:
        from faralpha.kite.intraday_db import close_interval
        close_interval(interval)
    else:
        icon.close()
    return total_inserted


def fetch_all_intervals(
    tickers: list[str] | None = None,
    intervals: list[tuple[str, int]] | None = None,
    max_workers: int = 3,
) -> dict[str, int]:
    """Fetch multiple intervals in parallel using threads.

    Uses ONE DuckDB connection (writer) but parallel Kite API calls
    via ThreadPoolExecutor. Each ticker fetches all intervals before
    moving to the next — this maximizes throughput while respecting
    Kite's 3 req/sec rate limit.

    Args:
        tickers: List of tickers. None = all universe.
        intervals: List of (interval, lookback_days). None = all 3 defaults.
        max_workers: Number of threads for API calls.

    Returns:
        Dict of interval -> candles inserted.
    """
    if intervals is None:
        intervals = [
            ("15minute", 200),
            ("30minute", 200),
            ("60minute", 400),
        ]

    kite = _get_kite()
    inst_map = _load_instrument_map(kite)

    icon = get_intraday_conn()
    init_intraday_schema(icon)

    if tickers is None:
        mcon = get_market_conn(read_only=True)
        rows = mcon.execute(
            "SELECT ticker FROM stocks WHERE market = 'india'"
        ).fetchall()
        tickers = [r[0] for r in rows]
        mcon.close()

    to_date = datetime.now()
    totals = {ivl: 0 for ivl, _ in intervals}
    skipped = 0

    def _fetch_one(ticker, interval, lookback_days):
        """Thread worker: fetch one ticker+interval from Kite API."""
        token = inst_map.get(ticker)
        if token is None:
            bare = ticker.replace(".NS", "")
            token = inst_map.get(f"{bare}.NS") or inst_map.get(bare)
        if token is None:
            return None

        from_date = to_date - timedelta(days=lookback_days)
        try:
            data = _rate_limited_fetch(
                kite, token,
                from_date.strftime("%Y-%m-%d"),
                to_date.strftime("%Y-%m-%d"),
                interval,
            )
        except Exception as e:
            log.warning("Kite error %s/%s: %s", ticker, interval, e)
            return None

        if not data:
            return None

        df = pd.DataFrame(data)
        df = df.rename(columns={"date": "ts"})
        df["ticker"] = ticker
        df["interval"] = interval
        df["ts"] = pd.to_datetime(df["ts"])
        return df

    # Process tickers — submit all intervals per ticker in parallel
    total_tasks = len(tickers) * len(intervals)
    done = 0

    for i, ticker in enumerate(tickers):
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for ivl, days in intervals:
                fut = pool.submit(_fetch_one, ticker, ivl, days)
                futures[fut] = ivl

            for fut in as_completed(futures):
                ivl = futures[fut]
                df = fut.result()
                done += 1
                if df is not None and len(df) > 0:
                    # Single-threaded DB write (safe)
                    df["volume"] = (
                        pd.to_numeric(df["volume"], errors="coerce")
                        .fillna(0)
                        .clip(upper=2**63 - 1)
                        .astype("int64")
                    )
                    if use_postgres_database():
                        upsert_candles_from_dataframe(icon, df)
                    else:
                        icon.execute(
                            "INSERT OR REPLACE INTO candles "
                            "(ts, ticker, interval, open, high, low, close, volume) "
                            "SELECT ts, ticker, interval, open, high, low, close, volume "
                            "FROM df"
                        )
                    totals[ivl] += len(df)

        if (i + 1) % 50 == 0 or i == len(tickers) - 1:
            pct = (i + 1) / len(tickers) * 100
            candles = sum(totals.values())
            log.info(
                "[%d/%d] %.0f%% — %s candles total (%s)",
                i + 1, len(tickers), pct, f"{candles:,}",
                ", ".join(f"{k}: {v:,}" for k, v in totals.items()),
            )

    for ivl, count in totals.items():
        log.info("Kite %s: %d candles", ivl, count)

    icon.close()
    return totals


def get_intraday(
    ticker: str,
    interval: str = "15minute",
    days: int = 5,
) -> pd.DataFrame:
    """Read intraday candles from DuckDB for a single ticker.

    Returns DataFrame with columns: ts, open, high, low, close, volume.
    """
    con = get_intraday_conn()
    cutoff = datetime.now() - timedelta(days=days)
    df = con.execute(
        "SELECT ts, open, high, low, close, volume FROM candles "
        "WHERE ticker = ? AND interval = ? AND ts >= ? "
        "ORDER BY ts",
        [ticker, interval, cutoff],
    ).df()
    con.close()
    return df


def get_first_hour_volume(ticker: str, date_str: str) -> dict:
    """Get volume profile for the first hour of a trading day.

    Returns dict with:
        - vol_15m: first 15min volume
        - vol_30m: first 30min volume
        - vol_60m: first 60min volume
        - avg_daily_vol: average full-day volume (last 20 days from daily)
        - rvol_15m: relative volume (15min vs avg_15min over last 20 days)
        - rvol_30m: relative volume (30min vs avg_30min over last 20 days)
        - rvol_60m: relative volume (60min vs avg_60min over last 20 days)
    """
    con = get_intraday_conn()
    target_date = pd.Timestamp(date_str)

    result = {}
    for minutes, label in [(15, "15m"), (30, "30m"), (60, "60m")]:
        interval = f"{minutes}minute"
        # Market open 09:15 IST
        open_time = target_date.replace(hour=9, minute=15)
        cutoff = open_time + timedelta(minutes=minutes)

        # Today's first N minutes volume
        row = con.execute(
            "SELECT SUM(volume) as vol FROM candles "
            "WHERE ticker = ? AND interval = ? "
            "AND ts >= ? AND ts < ?",
            [ticker, interval, open_time, cutoff],
        ).fetchone()
        today_vol = row[0] if row and row[0] else 0
        result[f"vol_{label}"] = today_vol

        # Average first-N-minutes volume over last 20 trading days
        lookback = target_date - timedelta(days=30)
        avg_row = con.execute(
            "SELECT AVG(day_vol) FROM ("
            "  SELECT DATE_TRUNC('day', ts) as day, SUM(volume) as day_vol "
            "  FROM candles "
            "  WHERE ticker = ? AND interval = ? "
            "  AND ts >= ? AND ts < ? "
            "  AND EXTRACT(HOUR FROM ts) = 9 "
            "  AND EXTRACT(MINUTE FROM ts) < (15 + ?) "
            "  GROUP BY DATE_TRUNC('day', ts) "
            "  ORDER BY day DESC LIMIT 20"
            ")",
            [ticker, interval, lookback, target_date, minutes],
        ).fetchone()
        avg_vol = avg_row[0] if avg_row and avg_row[0] else 0
        result[f"rvol_{label}"] = (today_vol / avg_vol) if avg_vol > 0 else 0.0

    con.close()
    return result


def purge_old_candles(keep_days: int | None = None) -> int:
    """Remove intraday candles older than keep_days to keep DB size manageable."""
    if keep_days is None:
        keep_days = KITE_CFG["intraday_lookback_days"]

    cutoff = datetime.now() - timedelta(days=keep_days)
    con = get_intraday_conn()
    result = con.execute(
        "DELETE FROM candles WHERE ts < ?", [cutoff]
    ).fetchone()
    deleted = result[0] if result else 0
    log.info("Purged %d candles older than %d days", deleted, keep_days)
    con.close()
    return deleted


def fetch_parallel_intervals(
    from_year: int = 2016,
    intervals: list[str] | None = None,
    tickers: list[str] | None = None,
) -> dict[str, int]:
    """Fetch multiple intervals in parallel using separate processes.

    Each interval writes to its own DuckDB file → no lock contention.
    Spawns one subprocess per interval, all sharing the same Kite rate
    limit (the OS-level rate limiter handles it naturally since each
    process has its own _rate_limited_fetch).

    NOTE: Total Kite rate = 3 req/sec SHARED. With 3 parallel processes,
    each effectively gets ~1 req/sec. Still faster than sequential because
    of I/O overlap and Kite's per-IP (not per-connection) limit.
    """
    import multiprocessing as mp

    if intervals is None:
        intervals = KITE_CFG["intervals"]

    start = datetime(from_year, 1, 1)
    end = datetime.now()

    def _worker(interval: str) -> tuple[str, int]:
        """Run in a subprocess — each gets its own DuckDB connection."""
        try:
            n = fetch_intraday_chunked(
                tickers=tickers,
                interval=interval,
                from_date=start,
                to_date=end,
                use_interval_db=True,
            )
            return interval, n
        except Exception as e:
            log.error("Worker %s failed: %s", interval, e)
            return interval, 0

    log.info("Launching %d parallel interval fetchers: %s", len(intervals), intervals)

    # Use spawn to avoid fork issues with DuckDB
    ctx = mp.get_context("spawn")
    results = {}
    processes = {}

    for ivl in intervals:
        p = ctx.Process(target=_worker, args=(ivl,), name=f"fetch_{ivl}")
        p.start()
        processes[ivl] = p
        log.info("Started worker PID %d for %s", p.pid, ivl)

    for ivl, p in processes.items():
        p.join()
        log.info("Worker %s finished (exit code %d)", ivl, p.exitcode or 0)
        results[ivl] = p.exitcode or 0

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Kite intraday candles")
    parser.add_argument("--interval", default="15minute",
                        choices=["15minute", "30minute", "60minute"])
    parser.add_argument("--tickers", nargs="*", help="Specific tickers")
    parser.add_argument("--all-intervals", action="store_true",
                        help="Fetch all configured intervals")
    parser.add_argument("--days", type=int, default=None,
                        help="Lookback days (default: config)")
    parser.add_argument("--from-year", type=int, default=None,
                        help="Fetch history from this year (loops through chunks)")
    parser.add_argument("--purge", action="store_true",
                        help="Purge old candles before fetching")
    parser.add_argument("--parallel", action="store_true",
                        help="Fetch all intervals in parallel (threaded, single DB)")
    parser.add_argument("--parallel-intervals", action="store_true",
                        help="Fetch all intervals in parallel (separate DB per interval)")
    parser.add_argument("--use-interval-db", action="store_true",
                        help="Use per-interval DB file instead of single intraday.duckdb")
    parser.add_argument("--workers", type=int, default=3,
                        help="Thread workers for parallel mode (default: 3)")

    args = parser.parse_args()

    if args.purge:
        purge_old_candles()

    if args.parallel_intervals:
        # True parallel: separate DB per interval, separate processes
        fetch_parallel_intervals(
            from_year=args.from_year or 2016,
            tickers=args.tickers,
        )
    elif args.from_year:
        # Long historical fetch using chunked date windows
        start = datetime(args.from_year, 1, 1)
        end = datetime.now()
        if args.all_intervals:
            for ivl in KITE_CFG["intervals"]:
                fetch_intraday_chunked(
                    tickers=args.tickers,
                    interval=ivl,
                    from_date=start,
                    to_date=end,
                    use_interval_db=args.use_interval_db,
                )
        else:
            fetch_intraday_chunked(
                tickers=args.tickers,
                interval=args.interval,
                from_date=start,
                to_date=end,
                use_interval_db=args.use_interval_db,
            )
    elif args.parallel:
        fetch_all_intervals(
            tickers=args.tickers,
            max_workers=args.workers,
        )
    elif args.all_intervals:
        MAX_LOOKBACK = {"15minute": 200, "30minute": 200, "60minute": 400}
        for ivl in KITE_CFG["intervals"]:
            days = args.days or MAX_LOOKBACK.get(ivl, 200)
            fetch_intraday_candles(
                tickers=args.tickers,
                interval=ivl,
                lookback_days=days,
            )
    else:
        fetch_intraday_candles(
            tickers=args.tickers,
            interval=args.interval,
            lookback_days=args.days,
        )
