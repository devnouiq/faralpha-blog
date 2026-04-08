"""Routes: /api/intraday/* — live ticker, watchlist, signals, config, regime-guide"""

from __future__ import annotations

import asyncio
from datetime import datetime

from fastapi import APIRouter

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.api import state
from faralpha.api.state import broadcast_from_thread, table_exists
from faralpha.api.pipeline import run_pipeline

log = get_logger("dashboard_api")
router = APIRouter()


@router.get("/api/intraday/config")
async def intraday_config():
    """Return intraday reversal strategy configuration."""
    return config.INTRADAY_REVERSAL


@router.get("/api/intraday/regime-guide")
async def intraday_regime_guide():
    """Return strategy recommendation based on current market regime."""
    con = get_conn(read_only=True)
    try:
        if not table_exists(con, "regime"):
            return {"error": "No regime data", "recommendation": "unknown"}

        r = con.execute(
            "SELECT date, is_bull, is_recovery, is_weak_market, "
            "regime_strength, breadth_pct, bench_close, bench_ma200 "
            "FROM regime WHERE market = 'india' ORDER BY date DESC LIMIT 1",
        ).fetchone()
        if not r:
            return {"error": "No regime data", "recommendation": "unknown"}

        is_bull, is_recovery, is_weak = r[1], r[2], r[3]
        strength = r[4]
        breadth = r[5]
        bench, bench_ma200 = r[6], r[7]

        # If latest row has no breadth data, fetch from most recent row that does
        if breadth is None:
            br = con.execute(
                "SELECT breadth_pct FROM regime "
                "WHERE market = 'india' AND breadth_pct IS NOT NULL "
                "ORDER BY date DESC LIMIT 1",
            ).fetchone()
            if br:
                breadth = br[0]
        regime = "bull" if is_bull else ("recovery" if is_recovery else "bear")

        nifty_below_200 = bench < bench_ma200 if bench and bench_ma200 else False
        breadth_weak = (breadth or 0) < 0.3

        if regime == "bear" or (nifty_below_200 and breadth_weak):
            recommendation = "reversal"
            primary = "VWAP Reversal"
            secondary = "Momentum paused"
            message = (
                "Market is in bear regime. Momentum strategies get stopped out frequently. "
                "This is when VWAP reversal shines — oversold stocks with institutional volume "
                "confirmation bounce harder. Run the live ticker and watch for signals."
            )
            urgency = "high"
        elif regime == "recovery":
            recommendation = "both"
            primary = "Both Strategies"
            secondary = "Transition period"
            message = (
                "Market is recovering. Momentum is re-entering positions while reversals "
                "still fire on laggards. Run both strategies — momentum for leaders, "
                "reversal for beaten-down names catching up."
            )
            urgency = "medium"
        else:
            recommendation = "momentum"
            primary = "Momentum (Minervini)"
            secondary = "Reversals rare"
            message = (
                "Strong bull market. Momentum strategy is in its sweet spot with trend-following. "
                "Reversal signals are rare in bull markets (fewer 7+ down-day stocks). "
                "Keep reversal on standby for when regime weakens."
            )
            urgency = "low"

        return {
            "regime": regime,
            "date": str(r[0])[:10],
            "recommendation": recommendation,
            "primary_strategy": primary,
            "secondary_strategy": secondary,
            "message": message,
            "urgency": urgency,
            "metrics": {
                "strength": round(strength, 2) if strength else None,
                "breadth_pct": round((breadth or 0) * 100, 1),
                "nifty_below_200ma": nifty_below_200,
                "benchmark": round(bench, 2) if bench else None,
                "benchmark_ma200": round(bench_ma200, 2) if bench_ma200 else None,
            },
        }
    except Exception as e:
        return {"error": str(e), "recommendation": "unknown"}
    finally:
        con.close()


@router.get("/api/intraday/signals/history")
async def intraday_signal_history():
    """Return signals fired today by live ticker + persisted history from DB."""
    signals = []

    if state.live_engine is not None:
        st = state.live_engine.get_status()
        signals.extend(st.get("signal_history", []))

    try:
        con = get_conn()
        rows = con.execute("""
            SELECT ticker, signal_date, strategy, price, vwap, rvol,
                   down_days, depth_pct, prev_close, day_open,
                   day_change_pct, max_hold_days, trailing_stop_pct, signal_time
            FROM intraday_signals
            ORDER BY signal_time DESC
            LIMIT 100
        """).fetchall()
        con.close()
        for r in rows:
            signals.append({
                "type": "intraday_reversal",
                "strategy": r[2],
                "ticker": r[0],
                "price": r[3],
                "vwap": r[4],
                "rvol": r[5],
                "down_days": r[6],
                "depth_pct": r[7],
                "prev_close": r[8],
                "day_open": r[9],
                "day_change_pct": r[10],
                "max_hold_days": r[11],
                "trailing_stop_pct": r[12],
                "time": str(r[13]),
            })
    except Exception:
        pass

    # Dedupe by ticker per day
    seen = set()
    unique = []
    for s in sorted(signals, key=lambda x: x.get("time", ""), reverse=True):
        ticker = s.get("ticker", "")
        day = s.get("time", "")[:10]
        key = f"{ticker}_{day}"
        if key not in seen:
            seen.add(key)
            unique.append(s)

    return {"signals": unique[:100], "count": len(unique)}


@router.get("/api/intraday/watchlist")
async def intraday_watchlist():
    """Generate pre-market watchlist of next-day reversal candidates."""
    try:
        from faralpha.kite.watchlist import generate_watchlist
        wl = generate_watchlist()
        if wl.empty:
            return {"watchlist": [], "count": 0}
        return {
            "watchlist": wl.to_dict(orient="records"),
            "count": len(wl),
            "config": config.INTRADAY_REVERSAL["watchlist"],
        }
    except Exception as e:
        return {"watchlist": [], "count": 0, "error": str(e)}


@router.post("/api/intraday/sync")
async def intraday_sync():
    """Fetch latest data: daily sync + pipeline + watchlist + intraday candles."""
    try:
        broadcast_from_thread("scan_progress", {"step": "intraday_sync", "message": "Syncing daily prices via Kite…"})
        from faralpha.api.sync_prices import sync_prices_kite
        sync_result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: sync_prices_kite(market="india", force=False)
        )
        log.info("Daily sync done: %s", sync_result)

        broadcast_from_thread("scan_progress", {"step": "pipeline", "message": "Running pipeline…"})
        pipeline_result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: run_pipeline("india")
        )
        log.info("Pipeline done")

        from faralpha.kite.watchlist import generate_watchlist
        wl = generate_watchlist()
        wl_tickers = wl["ticker"].tolist() if not wl.empty else []
        log.info("Watchlist: %d tickers", len(wl_tickers))

        candles_result = {}
        if wl_tickers:
            broadcast_from_thread("scan_progress", {
                "step": "intraday_fetch",
                "message": f"Fetching 15m/30m candles for {len(wl_tickers)} stocks…",
            })
            from faralpha.kite.fetch_intraday import fetch_all_intervals
            candles_result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: fetch_all_intervals(
                    tickers=wl_tickers,
                    intervals=[("15minute", 30), ("30minute", 30)],
                    max_workers=3,
                )
            )
            log.info("Intraday fetch done: %s", candles_result)

        broadcast_from_thread("scan_progress", {"step": "done", "message": "Sync complete"})
        return {
            "status": "ok",
            "daily_sync": sync_result,
            "watchlist_count": len(wl_tickers),
            "candles": candles_result,
        }
    except Exception as e:
        log.error("Intraday sync failed: %s", e)
        return {"status": "error", "error": str(e)}


@router.get("/api/intraday/status")
async def intraday_status():
    """Return live ticker status and scrape progress."""
    status = {
        "live_ticker_active": state.live_engine is not None,
        "strategy_config": config.INTRADAY_REVERSAL["vwap_reclaim"],
    }
    if state.live_engine is not None:
        status["live"] = state.live_engine.get_status()

    scrape = {}
    for interval, db_path in config.INTRADAY_DB_PATHS.items():
        try:
            import duckdb as _ddb
            c = _ddb.connect(str(db_path), read_only=True)
            row = c.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(ts), MAX(ts) FROM candles").fetchone()
            c.close()
            scrape[interval] = {
                "candles": row[0],
                "tickers": row[1],
                "from": str(row[2]) if row[2] else None,
                "to": str(row[3]) if row[3] else None,
            }
        except Exception:
            scrape[interval] = {"candles": 0, "tickers": 0, "from": None, "to": None}

    try:
        import duckdb as _ddb
        c = _ddb.connect(str(config.INTRADAY_DB_PATH), read_only=True)
        row = c.execute(
            "SELECT interval, COUNT(*), COUNT(DISTINCT ticker) FROM candles GROUP BY interval"
        ).fetchall()
        c.close()
        for ivl, cnt, tickers in row:
            if ivl not in scrape or scrape[ivl]["candles"] == 0:
                scrape[ivl] = {"candles": cnt, "tickers": tickers, "source": "legacy"}
    except Exception:
        pass

    status["scrape"] = scrape
    return status


def _persist_signal(signal: dict) -> None:
    """Insert signal into DuckDB intraday_signals table.

    Uses INSERT … ON CONFLICT DO NOTHING so the FIRST signal price is
    permanently locked.  Subsequent re-fires (e.g. after server restart)
    are silently ignored — the original price/time are preserved.
    """
    try:
        con = get_conn()
        signal_time = signal.get("time", datetime.now().isoformat())
        signal_date = signal_time[:10]
        con.execute("""
            INSERT INTO intraday_signals (
                ticker, signal_date, strategy, price, vwap, rvol,
                down_days, depth_pct, prev_close, day_open,
                day_change_pct, max_hold_days, trailing_stop_pct, signal_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ticker, signal_date) DO NOTHING
        """, [
            signal.get("ticker", ""),
            signal_date,
            signal.get("strategy", "vwap_reclaim"),
            signal.get("price", 0),
            signal.get("vwap"),
            signal.get("rvol"),
            signal.get("down_days"),
            signal.get("depth_pct"),
            signal.get("prev_close"),
            signal.get("day_open"),
            signal.get("day_change_pct"),
            signal.get("max_hold_days"),
            signal.get("trailing_stop_pct"),
            signal_time,
        ])
        con.close()
    except Exception as e:
        log.warning("Failed to persist signal: %s", e)
