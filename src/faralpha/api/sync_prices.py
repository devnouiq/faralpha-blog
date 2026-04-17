#!/usr/bin/env python3
"""
Daily Price Sync API
====================
Incremental daily price sync with automatic delisting detection.

Features:
  - **Incremental**: only fetches data since last stored date per ticker
  - **Bulk last-date lookup**: single SQL query instead of per-ticker
  - **Auto-delist**: after 3 consecutive download failures a ticker is
    marked delisted (``delisting_date`` set) and never synced again
  - **Multi-threaded**: 8 parallel Yahoo Finance downloads
  - **Market-hours aware**: skips sync while market is still open

Usage from CLI:
    uv run python -m faralpha.api.sync_prices --market india
    uv run python -m faralpha.api.sync_prices --status

Usage from Python (dashboard API):
    from faralpha.api.sync_prices import sync_prices
    result = sync_prices(market="india")
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from faralpha import config
from faralpha.pipeline.s02_prices import (
    download_ticker, _download_benchmark, YFinanceAPIError,
)
from faralpha.utils.db import get_conn, init_schema
from faralpha.utils.logger import get_logger

log = get_logger("sync_prices")


def _insert_prices_checkpoint(con, df: pd.DataFrame) -> None:
    """Per-ticker price upsert: DuckDB ``INSERT … FROM df`` or PostgreSQL ``ON CONFLICT``."""
    from faralpha.config import use_postgres_database

    if use_postgres_database():
        from psycopg2.extras import execute_values

        raw = getattr(con, "_raw", con)
        cols = ["date", "ticker", "open", "high", "low", "close", "volume", "market"]
        sub = df[cols]
        tuples = [tuple(x) for x in sub.to_numpy()]
        sql = """
            INSERT INTO prices (date, ticker, open, high, low, close, volume, market)
            VALUES %s
            ON CONFLICT (date, ticker, market) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """
        execute_values(raw, sql, tuples, page_size=5000)
        return
    try:
        con.connection.register("df", df)
    except Exception:
        pass
    con.execute("""
        INSERT OR REPLACE INTO prices
        SELECT date, ticker, open, high, low, close, volume, market FROM df
    """)


# After this many consecutive download failures a ticker is auto-delisted.
# Set high enough to avoid false delistings from transient Yahoo API issues.
_DELIST_AFTER_N_FAILS = 5

# Kite daily candles: max 2000 days per request, 3 req/sec
_KITE_DAY_MAX_WINDOW = 2000
_KITE_CHUNK_DAYS = 1900  # safety margin below 2000
_KITE_RATE_LIMIT = 3


def _kite_historical_chunked(kite, token: int, from_date: str, to_date: str,
                              interval: str = "day", delay: float = 0.34) -> list[dict]:
    """Fetch Kite historical data in chunks to stay within the 2000-day limit.

    Returns concatenated list of candle dicts.
    """
    from_dt = pd.Timestamp(from_date)
    to_dt = pd.Timestamp(to_date)
    all_data: list[dict] = []
    chunk_delta = pd.Timedelta(days=_KITE_CHUNK_DAYS)

    cursor = from_dt
    while cursor < to_dt:
        chunk_end = min(cursor + chunk_delta, to_dt)
        data = kite.historical_data(
            instrument_token=token,
            from_date=cursor.strftime("%Y-%m-%d"),
            to_date=chunk_end.strftime("%Y-%m-%d"),
            interval=interval,
        )
        if data:
            all_data.extend(data)
        cursor = chunk_end + pd.Timedelta(days=1)
        if cursor < to_dt:
            time.sleep(delay)

    return all_data


def sync_prices_kite(
    market: str = "india",
    force: bool = False,
    purge: bool = False,
) -> list[dict]:
    """Sync daily OHLCV from Kite historical API (replaces yfinance).

    Checkpoint-safe: each ticker is inserted immediately via INSERT OR REPLACE,
    so a server restart mid-sync simply resumes from where it left off on the
    next call (stale_cutoff check skips already-synced tickers).

    Args:
        purge: If True, delete ALL existing prices for the market and
               re-download full history from Kite.  Use this once to
               migrate from yfinance adjusted data to Kite raw data.

    Falls back to yfinance only for benchmark index data.
    """
    from faralpha.kite.fetch_intraday import _get_kite, _load_instrument_map

    con = get_conn()
    init_schema(con)

    markets = config.MARKETS if market == "both" else [market]
    results = []

    for mkt in markets:
        market_closed = _is_market_closed(mkt)
        if not market_closed and not force:
            log.warning("%s market still open — skipping sync", mkt.upper())
            results.append({"market": mkt, "tickers": 0, "synced": 0, "failed": 0,
                            "new_rows": 0, "newly_delisted": [], "skipped_up_to_date": 0,
                            "market_open": True, "message": f"{mkt.upper()} market still open"})
            continue

        tickers = _get_active_tickers(con, mkt)
        if not tickers:
            log.error("No active tickers for %s", mkt)
            results.append({"market": mkt, "tickers": 0, "synced": 0, "failed": 0,
                            "new_rows": 0, "newly_delisted": [], "skipped_up_to_date": 0,
                            "message": "No active tickers"})
            continue

        # ── Kite setup (one-time per run) ──
        try:
            kite = _get_kite()
            inst_map = _load_instrument_map(kite)
        except Exception as e:
            log.error("Kite init failed: %s — falling back to yfinance", e)
            results.extend(sync_prices(market=mkt, force=force))
            continue

        # ── Purge mode: wipe existing data for clean migration ──
        if purge:
            deleted = con.execute(
                "DELETE FROM prices WHERE market = ?", [mkt]
            ).fetchone()
            log.warning("%s: PURGE — deleted all existing prices", mkt.upper())

        # ── Checkpoint: bulk-query last stored date per ticker ──
        # This is the key to restart safety — any ticker already at
        # stale_cutoff is immediately skipped.
        last_dates = _bulk_last_dates(con, mkt)
        today = pd.Timestamp.now().normalize()
        stale_cutoff = today if market_closed else today - pd.Timedelta(days=1)

        work: list[tuple[str, int, str]] = []
        up_to_date = 0
        no_token = 0

        for t in tickers:
            last = last_dates.get(t)
            if last is not None and last >= stale_cutoff:
                up_to_date += 1
                continue

            token = inst_map.get(t)
            if token is None:
                bare = t.replace(".NS", "")
                token = inst_map.get(f"{bare}.NS")
            if token is None:
                no_token += 1
                continue

            if last is not None:
                start = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                # New ticker — fetch full history from Kite
                start = "2015-01-01"

            work.append((t, token, start))

        log.info(
            "%s: %d active, %d up-to-date, %d to sync, %d no token",
            mkt.upper(), len(tickers), up_to_date, len(work), no_token,
        )

        if not work:
            _safe_benchmark(con, mkt)
            _patch_today_close(con, kite, tickers, inst_map, mkt, today)
            results.append({"market": mkt, "tickers": len(tickers), "synced": 0,
                            "failed": 0, "new_rows": 0, "newly_delisted": [],
                            "skipped_up_to_date": up_to_date,
                            "market_open": not market_closed,
                            "message": "All tickers up to date", "source": "kite"})
            continue

        ok, fail, total_rows = 0, 0, 0
        newly_delisted: list[str] = []
        to_date_str = today.strftime("%Y-%m-%d")
        delay = 1.0 / _KITE_RATE_LIMIT
        batch_start = time.perf_counter()

        for idx, (ticker, token, start) in enumerate(work):
            # ── Progress broadcast (every 50 tickers) ──
            if idx % 50 == 0:
                elapsed = time.perf_counter() - batch_start
                log.info(
                    "  [%d/%d] synced=%d failed=%d rows=%d (%.0fs)",
                    idx, len(work), ok, fail, total_rows, elapsed,
                )

            # ── Fetch from Kite ──
            try:
                days_range = (pd.Timestamp(to_date_str) - pd.Timestamp(start)).days
                if days_range > _KITE_DAY_MAX_WINDOW:
                    data = _kite_historical_chunked(
                        kite, token, start, to_date_str, delay=delay,
                    )
                else:
                    data = kite.historical_data(
                        instrument_token=token,
                        from_date=start,
                        to_date=to_date_str,
                        interval="day",
                    )
            except Exception as e:
                msg = str(e)
                # Token expired — abort early, no point continuing
                if "Token" in msg and ("expired" in msg or "invalid" in msg.lower()):
                    log.error("Kite access token expired — aborting sync. Re-login needed.")
                    results.append({
                        "market": mkt, "tickers": len(tickers), "synced": ok,
                        "failed": fail + (len(work) - idx), "new_rows": total_rows,
                        "newly_delisted": newly_delisted,
                        "skipped_up_to_date": up_to_date,
                        "market_open": not market_closed,
                        "message": f"Kite token expired after {ok} synced",
                        "source": "kite", "error": "token_expired",
                    })
                    break
                log.debug("Kite error for %s: %s", ticker, msg)
                fail += 1
                fail_count = _increment_fail(con, ticker, mkt)
                if fail_count >= _DELIST_AFTER_N_FAILS:
                    _mark_delisted(con, ticker, mkt)
                    newly_delisted.append(ticker)
                time.sleep(delay)
                continue

            if not data:
                time.sleep(delay)
                continue

            df = pd.DataFrame(data)
            df = df.rename(columns={"date": "date_raw"})
            df["date"] = pd.to_datetime(df["date_raw"]).dt.date
            df["ticker"] = ticker
            df["market"] = mkt
            df = df[["date", "ticker", "open", "high", "low", "close", "volume", "market"]]

            # ── Split detection ──
            last_stored = last_dates.get(ticker)
            if last_stored is not None and not df.empty:
                first_new_close = float(df.iloc[0]["close"])
                old_row = con.execute(
                    "SELECT close FROM prices WHERE ticker = ? AND market = ? AND date = ? LIMIT 1",
                    [ticker, mkt, last_stored.date()],
                ).fetchone()
                if old_row and old_row[0] > 0 and first_new_close > 0:
                    ratio = old_row[0] / first_new_close
                    if ratio > 2.0 or ratio < 0.5:
                        log.warning(
                            "%s: price mismatch (stored=%.2f → new=%.2f, ratio=%.2f) — re-downloading from Kite",
                            ticker, old_row[0], first_new_close, ratio,
                        )
                        try:
                            full_data = _kite_historical_chunked(
                                kite, token,
                                from_date="2015-01-01",
                                to_date=to_date_str,
                                interval="day",
                                delay=delay,
                            )
                            if full_data:
                                full_df = pd.DataFrame(full_data)
                                full_df = full_df.rename(columns={"date": "date_raw"})
                                full_df["date"] = pd.to_datetime(full_df["date_raw"]).dt.date
                                full_df["ticker"] = ticker
                                full_df["market"] = mkt
                                full_df = full_df[["date", "ticker", "open", "high", "low", "close", "volume", "market"]]
                                con.execute("DELETE FROM prices WHERE ticker = ? AND market = ?", [ticker, mkt])
                                df = full_df
                                log.info("%s: replaced with %d rows of Kite data", ticker, len(full_df))
                        except Exception as e:
                            log.warning("%s: split re-download failed: %s", ticker, e)
                        time.sleep(delay)

            # ── INSERT immediately (checkpoint per ticker) ──
            try:
                _insert_prices_checkpoint(con, df)
                ok += 1
                total_rows += len(df)
                _reset_fail(con, ticker, mkt)
            except Exception as e:
                log.debug("Insert failed for %s: %s", ticker, e)
                fail += 1

            time.sleep(delay)
        else:
            # Only runs if loop completed without break (no token expiry)
            _safe_benchmark(con, mkt)

            elapsed = time.perf_counter() - batch_start
            msg = f"{ok} synced, {fail} failed, {total_rows} new rows (Kite, {elapsed:.0f}s)"
            if newly_delisted:
                msg += f", {len(newly_delisted)} newly delisted"
            log.info("✓ %s: %s", mkt.upper(), msg)

            # ── Patch today's close with live quote (closing auction price) ──
            _patch_today_close(con, kite, tickers, inst_map, mkt, today)

            results.append({
                "market": mkt, "tickers": len(tickers), "synced": ok,
                "failed": fail, "new_rows": total_rows,
                "newly_delisted": newly_delisted,
                "skipped_up_to_date": up_to_date,
                "market_open": not market_closed,
                "message": msg, "source": "kite",
            })

    return results


def _safe_benchmark(con, market: str) -> None:
    """Download benchmark index via yfinance (Kite doesn't serve index data)."""
    try:
        _download_benchmark(con, market)
    except Exception as e:
        log.warning("Benchmark sync failed (yfinance): %s", e)


def _patch_today_close(con, kite, tickers: list[str], inst_map: dict,
                       market: str, today: pd.Timestamp) -> None:
    """Update today's close/high/low/open with Kite live quotes.

    historical_data() returns the 3:30 PM close, but NSE's actual closing
    price is set during the closing auction (3:30-3:40 PM).  kite.quote()
    returns the real final price, so we patch today's row with it.
    """
    today_date = today.date()
    # Build reverse map: "NSE:SYMBOL" -> ticker
    symbols = []
    sym_to_ticker = {}
    for t in tickers:
        bare = t.replace(".NS", "")
        token = inst_map.get(t) or inst_map.get(f"{bare}.NS")
        if token:
            nse_sym = f"NSE:{bare}"
            symbols.append(nse_sym)
            sym_to_ticker[nse_sym] = t

    if not symbols:
        return

    # Kite quote() accepts max ~500 per call
    patched = 0
    batch_size = 450
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            quotes = kite.quote(batch)
        except Exception as e:
            log.warning("Quote patch failed: %s", e)
            return
        for nse_sym, data in quotes.items():
            ticker = sym_to_ticker.get(nse_sym)
            if not ticker:
                continue
            ohlc = data.get("ohlc", {})
            last = data.get("last_price", 0)
            if last <= 0:
                continue
            try:
                con.execute("""
                    UPDATE prices
                    SET close = ?, high = GREATEST(high, ?),
                        low = LEAST(low, ?)
                    WHERE ticker = ? AND market = ? AND date = ?
                """, [last, ohlc.get("high", last), ohlc.get("low", last),
                      ticker, market, today_date])
                patched += 1
            except Exception:
                pass
        time.sleep(0.34)

    log.info("Patched %d/%d tickers with closing auction prices", patched, len(symbols))


def _is_market_closed(market: str) -> bool:
    """Check if the market is currently closed (safe to sync end-of-day data)."""
    now = datetime.now(timezone.utc)

    if market == "india":
        india_hour = now.hour
        weekday = now.weekday()
        if weekday >= 5:
            return True
        return india_hour >= 11  # after 4:30 PM IST (buffer)

    return True


def _get_active_tickers(con, market: str) -> list[str]:
    """Get tickers that are still active (not delisted)."""
    df = con.execute("""
        SELECT ticker FROM stocks
        WHERE market = ?
          AND (delisting_date IS NULL OR delisting_date > CURRENT_DATE)
        ORDER BY ticker
    """, [market]).df()
    return df["ticker"].tolist()


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


def _mark_delisted(con, ticker: str, market: str) -> None:
    """Set delisting_date for a ticker that can't be downloaded."""
    con.execute("""
        UPDATE stocks
        SET delisting_date = CURRENT_DATE
        WHERE ticker = ? AND market = ?
    """, [ticker, market])
    log.warning(f"  ⛔ {ticker} marked DELISTED (no data after {_DELIST_AFTER_N_FAILS} attempts)")


def _increment_fail(con, ticker: str, market: str) -> int:
    """Increment sync_fail_count; return the new value."""
    con.execute("""
        UPDATE stocks
        SET sync_fail_count = COALESCE(sync_fail_count, 0) + 1
        WHERE ticker = ? AND market = ?
    """, [ticker, market])
    row = con.execute(
        "SELECT sync_fail_count FROM stocks WHERE ticker = ? AND market = ?",
        [ticker, market],
    ).fetchone()
    return row[0] if row else 1


def _reset_fail(con, ticker: str, market: str) -> None:
    """Reset failure counter on successful download."""
    con.execute("""
        UPDATE stocks
        SET sync_fail_count = 0
        WHERE ticker = ? AND market = ? AND COALESCE(sync_fail_count, 0) > 0
    """, [ticker, market])


def _refresh_eod_prices(con, market: str, date_str: str) -> int:
    """
    Batch-refresh today's prices with final EOD data from Yahoo Finance.

    Uses multi-ticker yf.download (much faster than individual downloads)
    to replace any intraday snapshots with final closing data.
    Also updates the watchlist table's close column.

    Returns number of tickers successfully updated.
    """
    suffix = ".NS" if market == "india" else ""
    tomorrow = (pd.Timestamp(date_str) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    # Get tickers that have data for today
    tickers_df = con.execute(
        "SELECT DISTINCT ticker FROM prices WHERE market = ? AND date = ?",
        [market, date_str],
    ).df()
    tickers = tickers_df["ticker"].tolist()
    if not tickers:
        return 0

    log.info(f"EOD refresh: updating {len(tickers)} tickers for {date_str}")
    updated = 0
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        symbols = [f"{t}{suffix}" for t in batch]

        try:
            raw = yf.download(
                symbols,
                start=date_str,
                end=tomorrow,
                progress=False,
                auto_adjust=True,
                threads=True,
                timeout=30,
            )
            if raw.empty:
                continue

            is_multi = isinstance(raw.columns, pd.MultiIndex)

            for ticker, symbol in zip(batch, symbols):
                try:
                    if is_multi:
                        close = float(raw["Close"][symbol].iloc[-1])
                        opn = float(raw["Open"][symbol].iloc[-1])
                        high = float(raw["High"][symbol].iloc[-1])
                        low = float(raw["Low"][symbol].iloc[-1])
                        vol = int(raw["Volume"][symbol].iloc[-1])
                    else:
                        # Single ticker in batch (shouldn't happen with batch_size > 1)
                        close = float(raw["Close"].iloc[-1])
                        opn = float(raw["Open"].iloc[-1])
                        high = float(raw["High"].iloc[-1])
                        low = float(raw["Low"].iloc[-1])
                        vol = int(raw["Volume"].iloc[-1])

                    if pd.isna(close) or close <= 0:
                        continue

                    con.execute(
                        "UPDATE prices SET open=?, high=?, low=?, close=?, volume=? "
                        "WHERE ticker=? AND market=? AND date=?",
                        [opn, high, low, close, vol, ticker, market, date_str],
                    )
                    updated += 1
                except Exception:
                    pass  # skip individual ticker failures
        except Exception as e:
            log.debug(f"  EOD batch {i}-{i + batch_size} failed: {e}")

    # Also update watchlist close from the refreshed prices
    if updated > 0:
        try:
            con.execute(
                "UPDATE watchlist SET close = p.close "
                "FROM (SELECT ticker, close FROM prices WHERE market = ? AND date = ?) p "
                "WHERE watchlist.ticker = p.ticker "
                "AND watchlist.market = ? AND watchlist.date = ?",
                [market, date_str, market, date_str],
            )
        except Exception:
            pass  # watchlist may not have today's rows

    log.info(f"  EOD refresh: {updated}/{len(tickers)} tickers updated")
    return updated


def sync_prices(
    market: str = "both",
    max_workers: int = 8,
    force: bool = False,
) -> list[dict]:
    """
    Incrementally sync latest prices for the given market(s).

    Only fetches data since each ticker's last stored date.
    Tickers that fail repeatedly are auto-marked as delisted.

    Returns:
        List of result dicts, one per market:
        [{"market": "india", "tickers": 500, "synced": 480, "failed": 20,
          "new_rows": 960, "newly_delisted": ["FOOO", "BARR"], ...}]
    """
    con = get_conn()
    init_schema(con)

    markets = config.MARKETS if market == "both" else [market]
    results = []

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Price sync: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        market_closed = _is_market_closed(mkt)
        if not market_closed and not force:
            log.warning(
                f"{mkt.upper()} market is still open — end-of-day data not yet available. "
                f"Use force=True to sync anyway (intraday data)."
            )
            results.append({
                "market": mkt,
                "tickers": 0,
                "synced": 0,
                "failed": 0,
                "new_rows": 0,
                "newly_delisted": [],
                "skipped_up_to_date": 0,
                "market_open": True,
                "message": f"{mkt.upper()} market still open, skipped",
            })
            continue

        tickers = _get_active_tickers(con, mkt)
        if not tickers:
            log.error(f"No active tickers for {mkt}. Run universe builder first.")
            results.append({
                "market": mkt, "tickers": 0, "synced": 0, "failed": 0,
                "new_rows": 0, "newly_delisted": [], "skipped_up_to_date": 0,
                "market_open": not market_closed,
                "message": "No active tickers",
            })
            continue

        # ── Bulk-query last dates (single SQL, not N queries) ──
        last_dates = _bulk_last_dates(con, mkt)
        ticker_starts: dict[str, str] = {}
        up_to_date = 0
        today = pd.Timestamp.now().normalize()

        # If market has already closed today, today's EOD data should be
        # available — require last_date >= today.  Otherwise (market still
        # open or we're running before close) yesterday's data is fine.
        stale_cutoff = today if market_closed else today - pd.Timedelta(days=1)

        for t in tickers:
            last = last_dates.get(t)
            if last is not None:
                if last >= stale_cutoff:
                    up_to_date += 1
                    continue  # already has fresh data
                ticker_starts[t] = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                # New ticker with no price history — fetch last 30 days
                ticker_starts[t] = (today - pd.Timedelta(days=30)).strftime("%Y-%m-%d")

        log.info(
            f"{len(tickers)} active tickers, {up_to_date} up-to-date, "
            f"{len(ticker_starts)} need sync ({max_workers} threads)"
        )

        if not ticker_starts:
            # Still sync benchmark even when all tickers are up-to-date
            _download_benchmark(con, mkt)
            results.append({
                "market": mkt, "tickers": len(tickers), "synced": 0, "failed": 0,
                "new_rows": 0, "newly_delisted": [], "skipped_up_to_date": up_to_date,
                "market_open": not market_closed,
                "message": "All tickers up to date",
            })
            continue

        # ── Parallel download ──
        ok, fail, api_errors, total_rows, splits_fixed = 0, 0, 0, 0, 0
        download_results: list[tuple[str, pd.DataFrame | None, bool]] = []

        def _download_one(ticker: str):
            start = ticker_starts[ticker]
            time.sleep(0.05)
            try:
                return ticker, download_ticker(ticker, mkt, start=start), False
            except YFinanceAPIError:
                return ticker, None, True  # API error — don't penalise

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_download_one, t): t for t in ticker_starts}
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    _, df, is_api_err = future.result()
                    download_results.append((ticker, df, is_api_err))
                except Exception:
                    download_results.append((ticker, pd.DataFrame(), False))

        # ── Sequential insert + delisting detection ──
        newly_delisted: list[str] = []

        for ticker, df, is_api_error in download_results:
            if is_api_error:
                # Transient Yahoo API issue — skip, don't penalise
                api_errors += 1
                continue
            if df is None or df.empty:
                fail += 1
                # Only penalise if the ticker is significantly behind (>3 days).
                # When today's data simply isn't published by Yahoo yet,
                # tickers with yesterday's data will return empty — this is
                # normal and should NOT increment the failure counter.
                start_date = pd.Timestamp(ticker_starts[ticker])
                days_behind = (today - start_date).days
                if days_behind > 3:
                    fail_count = _increment_fail(con, ticker, mkt)
                    if fail_count >= _DELIST_AFTER_N_FAILS:
                        _mark_delisted(con, ticker, mkt)
                        newly_delisted.append(ticker)
                    else:
                        log.debug(
                            f"  {ticker}: download failed ({fail_count}/{_DELIST_AFTER_N_FAILS})"
                        )
            else:
                try:
                    # ── Split detection ──
                    # If the first downloaded close differs from the last stored
                    # close by >2×, Yahoo has likely applied a retroactive split
                    # adjustment.  Delete stale history and re-download.
                    # Skip split detection when:
                    #   - same-day refresh (re-fetching today's data)
                    #   - market is still open (force sync) — live data is
                    #     unreliable and Yahoo sometimes returns wrong/stale
                    #     prices during market hours, especially after holidays.
                    first_new_close = float(df.iloc[0]["close"])
                    last_stored = last_dates.get(ticker)
                    did_redownload = False
                    is_same_day_refresh = (last_stored is not None and last_stored >= today)
                    skip_split_check = is_same_day_refresh or not market_closed
                    if last_stored is not None and first_new_close > 0 and not skip_split_check:
                        old_row = con.execute(
                            "SELECT close FROM prices "
                            "WHERE ticker = ? AND market = ? AND date = ? LIMIT 1",
                            [ticker, mkt, last_stored.date()],
                        ).fetchone()
                        if old_row and old_row[0] > 0:
                            ratio = old_row[0] / first_new_close
                            if ratio > 2.0 or ratio < 0.5:
                                log.warning(
                                    f"  {ticker}: split detected "
                                    f"(stored={old_row[0]:.2f} → new={first_new_close:.2f}, "
                                    f"ratio={ratio:.1f}×) — re-downloading full history"
                                )
                                full_df = download_ticker(ticker, mkt, start=config.DATA_START)
                                if full_df.empty:
                                    # Re-download failed — keep existing data, just
                                    # append the incremental update.
                                    log.warning(f"  {ticker}: split re-download failed, keeping existing data")
                                else:
                                    # Download succeeded — NOW safe to delete and replace
                                    con.execute(
                                        "DELETE FROM prices WHERE ticker = ? AND market = ?",
                                        [ticker, mkt],
                                    )
                                    df = full_df
                                    did_redownload = True

                    _insert_prices_checkpoint(con, df)
                    ok += 1
                    total_rows += len(df)
                    if did_redownload:
                        splits_fixed += 1
                    # Reset failure counter on success
                    _reset_fail(con, ticker, mkt)
                except Exception:
                    fail += 1

        # Also sync benchmark
        _download_benchmark(con, mkt)

        # ── EOD refresh ──
        # When market is closed, batch-refresh today's prices with final EOD
        # data.  This corrects any intraday snapshots that may have been
        # stored by earlier force-syncs during market hours.
        eod_refreshed = 0
        if market_closed:
            today_str = today.strftime("%Y-%m-%d")
            eod_refreshed = _refresh_eod_prices(con, mkt, today_str)

        msg = f"{ok} synced, {fail} failed, {total_rows} new rows"
        if eod_refreshed:
            msg += f", {eod_refreshed} EOD-refreshed"
        if splits_fixed:
            msg += f", {splits_fixed} splits fixed"
        if api_errors:
            msg += f", {api_errors} Yahoo API errors (skipped)"
        if newly_delisted:
            msg += f", {len(newly_delisted)} newly delisted"
        log.info(f"✓ {mkt.upper()}: {msg}")

        results.append({
            "market": mkt,
            "tickers": len(tickers),
            "synced": ok,
            "failed": fail,
            "new_rows": total_rows,
            "newly_delisted": newly_delisted,
            "skipped_up_to_date": up_to_date,
            "market_open": not market_closed,
            "message": msg,
        })

    con.close()
    return results


def purge_delisted(market: str = "both") -> dict:
    """Physically remove delisted stocks and all their price data from the DB."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]
    results = {}

    for mkt in markets:
        # Count delisted tickers
        row = con.execute("""
            SELECT COUNT(*) FROM stocks
            WHERE market = ? AND delisting_date IS NOT NULL
              AND delisting_date <= CURRENT_DATE
        """, [mkt]).fetchone()
        n_delisted = row[0] if row else 0

        if n_delisted == 0:
            results[mkt] = {"delisted_removed": 0, "price_rows_removed": 0}
            log.info(f"[{mkt.upper()}] No delisted stocks to purge")
            continue

        # Count price rows to be purged
        price_row = con.execute("""
            SELECT COUNT(*) FROM prices
            WHERE market = ? AND ticker IN (
                SELECT ticker FROM stocks
                WHERE market = ? AND delisting_date IS NOT NULL
                  AND delisting_date <= CURRENT_DATE
            )
        """, [mkt, mkt]).fetchone()
        n_prices = price_row[0] if price_row else 0

        # Delete from derived tables first
        for table in ["prices", "features", "ranked", "signals", "candidates", "watchlist"]:
            try:
                con.execute(f"""
                    DELETE FROM {table}
                    WHERE market = ? AND ticker IN (
                        SELECT ticker FROM stocks
                        WHERE market = ? AND delisting_date IS NOT NULL
                          AND delisting_date <= CURRENT_DATE
                    )
                """, [mkt, mkt])
            except Exception:
                pass  # table may not exist

        # Delete the stock rows
        con.execute("""
            DELETE FROM stocks
            WHERE market = ? AND delisting_date IS NOT NULL
              AND delisting_date <= CURRENT_DATE
        """, [mkt])

        results[mkt] = {"delisted_removed": n_delisted, "price_rows_removed": n_prices}
        log.info(
            f"✓ {mkt.upper()}: purged {n_delisted} delisted stocks, "
            f"{n_prices:,} price rows"
        )

    con.close()
    return results


def repair_splits(market: str = "both", max_workers: int = 8) -> dict:
    """Detect and repair tickers with stale pre-split prices.

    Downloads recent data from Yahoo for every active ticker (threaded),
    compares with the latest stored close.  When the ratio exceeds 2×,
    deletes all stored history and re-downloads from DATA_START.

    Returns:
        {"india": {"checked": N, "fixed": N, "tickers": [...]}, ...}
    """
    con = get_conn()
    init_schema(con)
    markets = config.MARKETS if market == "both" else [market]
    results = {}

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Split repair: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        active = _get_active_tickers(con, mkt)
        if not active:
            results[mkt] = {"checked": 0, "fixed": 0, "tickers": []}
            continue

        # Get latest stored close for every ticker
        stored: dict[str, float] = {}
        rows = con.execute("""
            SELECT p.ticker, p.close
            FROM prices p
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM prices WHERE market = ?
                GROUP BY ticker
            ) m ON p.ticker = m.ticker AND p.date = m.max_date
            WHERE p.market = ?
        """, [mkt, mkt]).fetchall()
        for t, c in rows:
            stored[t] = c

        # Threaded download of recent data per ticker
        yahoo_prices: dict[str, float] = {}
        start_10d = (pd.Timestamp.now() - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

        def _fetch_latest(ticker: str):
            time.sleep(0.05)
            try:
                df = download_ticker(ticker, mkt, start=start_10d)
                if not df.empty:
                    return ticker, float(df.iloc[-1]["close"])
            except Exception:
                pass
            return ticker, None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_latest, t): t for t in active}
            done = 0
            for future in as_completed(futures):
                ticker, yf_close = future.result()
                if yf_close is not None:
                    yahoo_prices[ticker] = yf_close
                done += 1
                if done % 200 == 0:
                    log.info(f"  Checked {done}/{len(active)} tickers")

        log.info(f"  Yahoo returned prices for {len(yahoo_prices)}/{len(active)} tickers")

        # Find tickers with split discrepancies
        split_tickers: list[str] = []
        for t in active:
            db_close = stored.get(t, 0)
            yf_close = yahoo_prices.get(t, 0)
            if db_close > 0 and yf_close > 0:
                ratio = db_close / yf_close
                if ratio > 2.0 or ratio < 0.5:
                    split_tickers.append(t)
                    log.info(
                        f"  {t}: split detected "
                        f"(DB={db_close:.2f} vs Yahoo={yf_close:.2f}, "
                        f"ratio={ratio:.1f}×)"
                    )

        log.info(f"Found {len(split_tickers)} tickers with split discrepancies")

        # Re-download full history for affected tickers
        fixed = 0
        for t in split_tickers:
            try:
                df = download_ticker(t, mkt, start=config.DATA_START)
                if not df.empty:
                    # Download succeeded — now safe to delete and replace
                    con.execute(
                        "DELETE FROM prices WHERE ticker = ? AND market = ?", [t, mkt]
                    )
                    _insert_prices_checkpoint(con, df)
                    fixed += 1
                    log.info(f"  ✓ {t}: re-downloaded {len(df)} rows")
                else:
                    log.warning(f"  ✗ {t}: re-download returned empty")
                time.sleep(0.1)
            except Exception as e:
                log.warning(f"  ✗ {t}: repair failed — {e}")

        results[mkt] = {
            "checked": len(active),
            "yahoo_responded": len(yahoo_prices),
            "fixed": fixed,
            "tickers": split_tickers,
        }
        log.info(
            f"✓ {mkt.upper()}: checked {len(active)}, "
            f"fixed {fixed}/{len(split_tickers)} split tickers"
        )

    con.close()
    return results


def get_sync_status() -> dict:
    """Get the current data freshness status for both markets."""
    con = get_conn(read_only=True)
    status = {}

    for mkt in config.MARKETS:
        try:
            row = con.execute("""
                SELECT
                    MAX(date) as last_date,
                    COUNT(DISTINCT ticker) as n_tickers,
                    COUNT(*) as n_rows
                FROM prices
                WHERE market = ?
            """, [mkt]).fetchone()

            counts = con.execute("""
                SELECT
                    SUM(CASE WHEN delisting_date IS NULL OR delisting_date > CURRENT_DATE
                             THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN delisting_date IS NOT NULL AND delisting_date <= CURRENT_DATE
                             THEN 1 ELSE 0 END) AS delisted
                FROM stocks
                WHERE market = ?
            """, [mkt]).fetchone()

            status[mkt] = {
                "last_date": str(row[0]) if row[0] else None,
                "n_tickers_with_data": row[1],
                "n_active_tickers": int(counts[0] or 0),
                "n_delisted_tickers": int(counts[1] or 0),
                "total_rows": row[2],
                "market_closed": _is_market_closed(mkt),
            }
        except Exception:
            status[mkt] = {"error": "No data"}

    con.close()
    return status


if __name__ == "__main__":
    import argparse
    import json

    p = argparse.ArgumentParser(description="Sync daily prices")
    p.add_argument("--market", default="india", choices=["india"])
    p.add_argument("--force", action="store_true", help="Sync even if market is open")
    p.add_argument("--status", action="store_true", help="Just show sync status")
    p.add_argument("--repair-splits", action="store_true",
                   help="Detect and fix tickers with stale pre-split prices")
    args = p.parse_args()

    if args.status:
        status = get_sync_status()
        print(json.dumps(status, indent=2, default=str))
    elif args.repair_splits:
        results = repair_splits(market=args.market)
        print(json.dumps(results, indent=2, default=str))
    else:
        results = sync_prices(market=args.market, force=args.force)
        print(json.dumps(results, indent=2, default=str))
