"""
Kite WebSocket Live Ticker — Real-time intraday signal detection.

Subscribes to Kite Ticker (WebSocket) for watchlist stocks during market hours.
Computes real-time VWAP and RVOL, fires signals when conditions are met.

Architecture:
  1. Pre-market (8:30 AM): Load watchlist (stocks at 4+ down days)
  2. Market open (9:15 AM): Subscribe to Kite Ticker for all watchlist tickers
  3. First hour (9:15-10:15): Accumulate volume + compute rolling VWAP
  4. Signal window (10:15-14:30): Check VWAP reclaim + RVOL conditions
  5. Market close (15:30): Disconnect, log results

Usage:
    uv run python -m faralpha.kite.live_ticker
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from threading import Thread

import numpy as np

from faralpha import config
from faralpha.utils.logger import get_logger

log = get_logger("kite_live")

KITE_CFG = config.KITE
REVERSAL_CFG = config.INTRADAY_REVERSAL


class LiveSignalEngine:
    """Real-time signal detection from Kite WebSocket ticks."""

    def __init__(self, watchlist: list[dict], on_signal=None):
        """
        Args:
            watchlist: List of dicts with keys: ticker, instrument_token, down_days, avg_first_hour_vol
            on_signal: Callback(signal_dict) when a signal fires
        """
        self.watchlist = {w["instrument_token"]: w for w in watchlist}
        self.on_signal = on_signal

        # Per-token accumulators for today
        self._volume: dict[int, float] = defaultdict(float)
        self._tp_volume: dict[int, float] = defaultdict(float)  # typical_price * volume
        self._first_hour_vol: dict[int, float] = defaultdict(float)
        self._first_hour_tp_vol: dict[int, float] = defaultdict(float)
        self._last_price: dict[int, float] = {}
        self._day_open: dict[int, float] = {}   # today's open price per token
        self._prev_close: dict[int, float] = {}  # previous day's close per token
        self._fired: set[int] = set()  # tokens that already fired today
        self._first_hour_done = False
        self._market_closed = False  # set True after 15:35
        self._today = date.today()
        self._signal_history: list[dict] = []  # all signals fired today
        self._last_broadcast: float = 0.0
        self.on_status_update = None  # callback(status_dict) for WS push
        self._load_fired_from_db()  # survive ticker restarts

    @property
    def vwap(self) -> dict[int, float]:
        """Current VWAP per token."""
        result = {}
        for token in self.watchlist:
            vol = self._first_hour_vol.get(token, 0)
            if vol > 0:
                result[token] = self._first_hour_tp_vol.get(token, 0) / vol
        return result

    @property
    def rvol(self) -> dict[int, float]:
        """Current relative volume per token (vs historical avg)."""
        result = {}
        for token, w in self.watchlist.items():
            avg = w.get("avg_first_hour_vol", 0)
            current = self._first_hour_vol.get(token, 0)
            if avg > 0:
                result[token] = current / avg
        return result

    def on_tick(self, ticks: list[dict]) -> None:
        """Called by KiteTicker on_ticks. Process each tick."""
        now = datetime.now()

        # Reset if new day
        if now.date() != self._today:
            self._reset()
            self._today = now.date()

        # Auto-close after 15:35 — stop processing ticks, broadcast final status
        if now.hour > 15 or (now.hour == 15 and now.minute >= 35):
            if not self._market_closed:
                self._market_closed = True
                log.info("Market closed — ticker idle (%d signals fired today)", len(self._fired))
                if self.on_status_update:
                    try:
                        self.on_status_update(self.get_status())
                    except Exception:
                        pass
            return  # skip all tick processing after market close

        is_first_hour = (now.hour == 9 and now.minute >= 15) or (now.hour == 10 and now.minute < 15)
        if now.hour == 10 and now.minute >= 15:
            self._first_hour_done = True

        for tick in ticks:
            token = tick.get("instrument_token")
            if token not in self.watchlist:
                continue

            ltp = tick.get("last_price", 0)
            vol = tick.get("volume_traded", 0)  # cumulative day volume
            ohlc = tick.get("ohlc", {})
            high = ohlc.get("high", ltp)
            low = ohlc.get("low", ltp)

            # Capture day open and previous close from Kite OHLC
            if token not in self._day_open and ohlc.get("open", 0) > 0:
                self._day_open[token] = ohlc["open"]
            if token not in self._prev_close and ohlc.get("close", 0) > 0:
                self._prev_close[token] = ohlc["close"]  # Kite: ohlc.close = prev day close

            # Typical price
            tp = (high + low + ltp) / 3

            prev_vol = self._volume.get(token, 0)
            tick_vol = max(0, vol - prev_vol) if prev_vol > 0 else 0

            self._volume[token] = vol
            self._last_price[token] = ltp
            self._tp_volume[token] += tp * tick_vol

            if is_first_hour:
                self._first_hour_vol[token] += tick_vol
                self._first_hour_tp_vol[token] += tp * tick_vol

            # Check signal only inside signal window (10:15–14:30)
            in_signal_window = (
                self._first_hour_done
                and (now.hour < 14 or (now.hour == 14 and now.minute <= 30))
            )
            if in_signal_window and token not in self._fired:
                self._check_signal(token, ltp, now)

        # Throttled status broadcast via WebSocket (every 10s)
        now_ts = time.time()
        if self.on_status_update and now_ts - self._last_broadcast >= 10.0:
            self._last_broadcast = now_ts
            try:
                self.on_status_update(self.get_status())
            except Exception:
                pass

    def _check_signal(self, token: int, price: float, now: datetime) -> None:
        """Check if VWAP reclaim signal fires for this token."""
        if token in self._fired:
            return
        cfg = REVERSAL_CFG["vwap_reclaim"]
        w = self.watchlist[token]

        # VWAP
        fh_vol = self._first_hour_vol.get(token, 0)
        if fh_vol <= 0:
            return
        vwap = self._first_hour_tp_vol.get(token, 0) / fh_vol

        # RVOL
        avg_vol = w.get("avg_first_hour_vol", 0)
        rvol = fh_vol / avg_vol if avg_vol > 0 else 0

        # Depth from 20d high (passed from watchlist)
        depth_pct = w.get("depth_pct", 0)
        depth_max = cfg.get("depth_max", None)  # e.g. -0.10

        # Signal conditions
        if (
            w.get("down_days", 0) >= cfg["min_down_days"]
            and rvol >= cfg["min_rvol"]
            and price > vwap
            and (depth_max is None or depth_pct <= depth_max)
        ):
            self._fired.add(token)
            prev_cl = self._prev_close.get(token, 0)
            d_open = self._day_open.get(token, 0)
            day_chg = (price / prev_cl - 1) if prev_cl > 0 else 0
            signal = {
                "type": "intraday_reversal",
                "strategy": "vwap_reclaim",
                "ticker": w["ticker"],
                "price": round(price, 2),
                "vwap": round(vwap, 2),
                "rvol": round(rvol, 2),
                "down_days": w["down_days"],
                "depth_pct": round(depth_pct * 100, 1),
                "prev_close": round(prev_cl, 2),
                "day_open": round(d_open, 2),
                "day_change_pct": round(day_chg * 100, 2),
                "max_hold_days": cfg.get("max_hold_days", 7),
                "trailing_stop_pct": cfg.get("trailing_stop_pct", 0.02),
                "stop_loss_pct": cfg.get("stop_loss_pct"),  # None = no fixed SL
                "time": now.isoformat(),
            }
            self._signal_history.append(signal)
            log.info("SIGNAL: %s @ %.2f (VWAP=%.2f, RVOL=%.1fx, down=%dd, depth=%.1f%%)",
                     w["ticker"], price, vwap, rvol, w["down_days"], depth_pct * 100)
            if self.on_signal:
                self.on_signal(signal)

    def _load_fired_from_db(self) -> None:
        """Pre-populate _fired set AND _signal_history from today's DB signals.

        This ensures:
        1. The signal won't re-fire after server restart
        2. The original signal price/time are preserved in memory
        """
        try:
            from faralpha.utils.db import get_conn
            con = get_conn()
            rows = con.execute(
                "SELECT ticker, strategy, price, vwap, rvol, down_days, "
                "depth_pct, prev_close, day_open, day_change_pct, "
                "max_hold_days, trailing_stop_pct, signal_time "
                "FROM intraday_signals WHERE signal_date = ?",
                [str(self._today)],
            ).fetchall()
            con.close()
            # Map ticker names back to instrument tokens
            ticker_to_token = {w["ticker"]: t for t, w in self.watchlist.items()}
            for row in rows:
                ticker = row[0]
                token = ticker_to_token.get(ticker)
                if token is not None:
                    self._fired.add(token)
                # Restore signal history so the API returns original data
                self._signal_history.append({
                    "type": "intraday_reversal",
                    "strategy": row[1],
                    "ticker": ticker,
                    "price": row[2],
                    "vwap": row[3],
                    "rvol": row[4],
                    "down_days": row[5],
                    "depth_pct": row[6],
                    "prev_close": row[7],
                    "day_open": row[8],
                    "day_change_pct": row[9],
                    "max_hold_days": row[10],
                    "trailing_stop_pct": row[11],
                    "time": str(row[12]),
                })
            if self._fired:
                log.info("Restored %d already-fired signals from DB (prices locked)", len(self._fired))
        except Exception as e:
            log.warning("Could not load fired signals from DB: %s", e)

    def _reset(self) -> None:
        """Reset daily accumulators."""
        self._volume.clear()
        self._tp_volume.clear()
        self._first_hour_vol.clear()
        self._first_hour_tp_vol.clear()
        self._last_price.clear()
        self._day_open.clear()
        self._prev_close.clear()
        self._fired.clear()
        self._first_hour_done = False
        self._market_closed = False
        self._signal_history.clear()

    def backfill_first_hour(self, kite) -> None:
        """Fetch today's 5-minute candles from Kite historical API to reconstruct
        first-hour VWAP and volume.  Called when starting the ticker after 9:15.

        This makes late starts (e.g. 11 AM) work correctly — the engine
        computes VWAP/RVOL from real 9:15–10:15 data instead of zeros.

        On restart, tries to load cached aggregates from DuckDB first
        to avoid re-fetching ~300 stocks from Kite API every time.
        """
        now = datetime.now()
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        first_hour_end = now.replace(hour=10, minute=15, second=0, microsecond=0)

        if now < market_open:
            log.info("Backfill: market not open yet, nothing to backfill")
            return

        # Try loading cached first-hour data from DB first
        cached = self._load_first_hour_cache()
        if cached > 0:
            if now >= first_hour_end:
                self._first_hour_done = True
            log.info("Loaded %d cached first-hour entries from DB — skipping Kite backfill",
                     cached)
            return

        # Fetch up to 10:15, or up to now if still in first hour
        fetch_to = min(first_hour_end, now)

        filled = 0
        for token, w in self.watchlist.items():
            ticker = w["ticker"]
            try:
                candles = kite.historical_data(
                    instrument_token=token,
                    from_date=market_open,
                    to_date=fetch_to,
                    interval="5minute",
                )
            except Exception as e:
                log.warning("Backfill failed for %s: %s", ticker, e)
                continue

            if not candles:
                continue

            fh_vol = 0.0
            fh_tp_vol = 0.0
            last_close = 0.0
            for c in candles:
                vol = c.get("volume", 0)
                tp = (c["high"] + c["low"] + c["close"]) / 3
                fh_vol += vol
                fh_tp_vol += tp * vol
                last_close = c["close"]

            if fh_vol > 0:
                self._first_hour_vol[token] = fh_vol
                self._first_hour_tp_vol[token] = fh_tp_vol
                self._last_price[token] = last_close
                self._volume[token] = fh_vol  # approximate cumulative
                filled += 1

            time.sleep(0.35)  # Kite rate limit: 3 req/sec

        if now >= first_hour_end:
            self._first_hour_done = True

        log.info("Backfill complete: %d/%d stocks filled, first_hour_done=%s",
                 filled, len(self.watchlist), self._first_hour_done)

        # Cache to DB so restarts don't re-fetch from Kite
        if filled > 0:
            self._save_first_hour_cache()

    def _save_first_hour_cache(self) -> None:
        """Persist first-hour vol/tp_vol aggregates to DuckDB for restart reuse."""
        try:
            from faralpha.utils.db import get_conn
            con = get_conn()
            con.execute("""
                CREATE TABLE IF NOT EXISTS first_hour_cache (
                    cache_date  DATE    NOT NULL,
                    token       INTEGER NOT NULL,
                    fh_vol      DOUBLE  NOT NULL,
                    fh_tp_vol   DOUBLE  NOT NULL,
                    last_price  DOUBLE  NOT NULL,
                    PRIMARY KEY (cache_date, token)
                )
            """)
            today = str(self._today)
            rows = []
            for token in self._first_hour_vol:
                rows.append((
                    today,
                    token,
                    self._first_hour_vol[token],
                    self._first_hour_tp_vol.get(token, 0),
                    self._last_price.get(token, 0),
                ))
            if rows:
                con.executemany("""
                    INSERT INTO first_hour_cache (cache_date, token, fh_vol, fh_tp_vol, last_price)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (cache_date, token) DO UPDATE SET
                        fh_vol = excluded.fh_vol,
                        fh_tp_vol = excluded.fh_tp_vol,
                        last_price = excluded.last_price
                """, rows)
                log.info("Cached %d first-hour entries to DB", len(rows))
            con.close()
        except Exception as e:
            log.warning("Failed to save first-hour cache: %s", e)

    def _load_first_hour_cache(self) -> int:
        """Load cached first-hour aggregates from DuckDB. Returns count loaded."""
        try:
            from faralpha.utils.db import get_conn
            con = get_conn()
            # Check table exists
            tables = [r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'first_hour_cache'"
            ).fetchall()]
            if "first_hour_cache" not in tables:
                con.close()
                return 0
            rows = con.execute(
                "SELECT token, fh_vol, fh_tp_vol, last_price "
                "FROM first_hour_cache WHERE cache_date = ?",
                [str(self._today)],
            ).fetchall()
            con.close()
            if not rows:
                return 0
            loaded = 0
            for token, fh_vol, fh_tp_vol, last_price in rows:
                if token in self.watchlist and fh_vol > 0:
                    self._first_hour_vol[token] = fh_vol
                    self._first_hour_tp_vol[token] = fh_tp_vol
                    self._last_price[token] = last_price
                    self._volume[token] = fh_vol
                    loaded += 1
            return loaded
        except Exception as e:
            log.warning("Failed to load first-hour cache: %s", e)
            return 0

    def get_status(self) -> dict:
        """Current status for API/UI."""
        # Fallback: set market_closed if time > 15:35 even if no ticks arrived
        now = datetime.now()
        if not self._market_closed and (now.hour > 15 or (now.hour == 15 and now.minute >= 35)):
            self._market_closed = True

        # Per-ticker detail for live monitor
        ticker_detail = []
        for token, w in self.watchlist.items():
            fh_vol = self._first_hour_vol.get(token, 0)
            avg_vol = w.get("avg_first_hour_vol", 0)
            vwap_val = (self._first_hour_tp_vol.get(token, 0) / fh_vol) if fh_vol > 0 else 0
            rvol_val = (fh_vol / avg_vol) if avg_vol > 0 else 0
            price = self._last_price.get(token, 0)
            dist_to_vwap = ((price - vwap_val) / vwap_val * 100) if vwap_val > 0 else 0
            prev_cl = self._prev_close.get(token, 0)
            d_open = self._day_open.get(token, 0)
            day_chg = ((price / prev_cl - 1) * 100) if prev_cl > 0 else 0
            ticker_detail.append({
                "ticker": w["ticker"],
                "price": round(price, 2),
                "vwap": round(vwap_val, 2),
                "rvol": round(rvol_val, 2),
                "down_days": w.get("down_days", 0),
                "depth_pct": round(w.get("depth_pct", 0) * 100, 1),
                "dist_to_vwap_pct": round(dist_to_vwap, 2),
                "day_change_pct": round(day_chg, 2),
                "prev_close": round(prev_cl, 2),
                "fired": token in self._fired,
                "has_volume": fh_vol > 0,
            })
        # Sort: fired first, then by RVOL descending
        ticker_detail.sort(key=lambda x: (-x["fired"], -x["rvol"]))

        return {
            "date": str(self._today),
            "watchlist_size": len(self.watchlist),
            "first_hour_done": self._first_hour_done,
            "market_closed": self._market_closed,
            "signals_fired": len(self._fired),
            "tokens_with_volume": sum(1 for v in self._first_hour_vol.values() if v > 0),
            "vwap": {self.watchlist[t]["ticker"]: round(v, 2) for t, v in self.vwap.items()},
            "rvol": {self.watchlist[t]["ticker"]: round(v, 2) for t, v in self.rvol.items()},
            "tickers": ticker_detail,
            "signal_history": list(self._signal_history),
        }


def start_kite_ticker(watchlist: list[dict], on_signal=None) -> tuple:
    """Start Kite WebSocket ticker in a background thread.

    If started after 9:15, automatically backfills the first-hour data
    from Kite historical API so VWAP/RVOL are computed correctly.

    Returns (KiteTicker instance, LiveSignalEngine instance).
    """
    from kiteconnect import KiteTicker, KiteConnect

    engine = LiveSignalEngine(watchlist, on_signal=on_signal)
    tokens = list(engine.watchlist.keys())

    kite = KiteConnect(api_key=KITE_CFG["api_key"])
    kite.set_access_token(KITE_CFG["access_token"])

    # Backfill first-hour data if starting late (but skip after market close)
    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=35, second=0, microsecond=0)
    if market_open < now < market_close:
        log.info("Late start detected (%s) — backfilling first-hour data…", now.strftime("%H:%M"))
        engine.backfill_first_hour(kite)
    elif now >= market_close:
        log.info("After market hours (%s) — skipping backfill", now.strftime("%H:%M"))
        engine._market_closed = True

    kws = KiteTicker(KITE_CFG["api_key"], KITE_CFG["access_token"])

    def on_ticks(ws, ticks):
        engine.on_tick(ticks)

    def on_connect(ws, response):
        log.info("Kite Ticker connected, subscribing to %d tokens", len(tokens))
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
        log.warning("Kite Ticker closed: %s %s", code, reason)

    def on_error(ws, code, reason):
        log.error("Kite Ticker error: %s %s", code, reason)

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    thread = Thread(target=kws.connect, kwargs={"threaded": True}, daemon=True)
    thread.start()
    log.info("Kite Ticker started in background thread")

    return kws, engine
