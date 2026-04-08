"""
Pre-Market Watchlist Generator — Next-day reversal targets.

Scans the universe each evening to find stocks approaching reversal conditions:
  - 4+ consecutive down days (1 more = hits the 5-day threshold)
  - Adequate average volume (liquid enough to trade)
  - Ranked by depth from recent high (deeper = more potential bounce)

These become tomorrow's "watch list" — during market hours, the live ticker
monitors them for VWAP reclaim + high RVOL to fire actual buy signals.

Usage:
    uv run python -m faralpha.kite.watchlist
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("watchlist")

REVERSAL_CFG = config.INTRADAY_REVERSAL
WATCHLIST_CFG = REVERSAL_CFG["watchlist"]


def generate_watchlist() -> pd.DataFrame:
    """Generate pre-market watchlist of next-day reversal candidates.

    Returns DataFrame with columns:
        ticker, down_days, depth_pct, avg_volume, close, sector
    """
    con = get_conn()

    # Latest prices per ticker (last 30 trading days needed for context)
    daily = con.execute("""
        SELECT p.date, p.ticker, p.close, p.low, p.volume, s.sector
        FROM prices p
        JOIN stocks s ON p.ticker = s.ticker
        WHERE s.market = 'india'
          AND p.date >= CURRENT_DATE - INTERVAL '60 days'
        ORDER BY p.ticker, p.date
    """).df()
    con.close()

    if daily.empty:
        log.warning("No daily price data found")
        return pd.DataFrame()

    candidates = []
    for ticker, grp in daily.groupby("ticker"):
        grp = grp.sort_values("date")
        if len(grp) < 10:
            continue

        close = grp["close"].values
        low = grp["low"].values
        volume = grp["volume"].values

        # Consecutive down days (from most recent)
        down_days = 0
        for i in range(len(close) - 1, 0, -1):
            if close[i] < close[i - 1]:
                down_days += 1
            else:
                break

        # Need at least min_near_down_days to be "on the radar"
        if down_days < WATCHLIST_CFG["min_near_down_days"]:
            continue

        # ── Lower circuit filter ──
        # If close == low on any of the recent down days, the stock is
        # circuit-locked (no liquidity, can't trade a reversal).
        circuit_locked = False
        for i in range(len(close) - 1, max(len(close) - 1 - down_days, 0), -1):
            if low[i] > 0 and abs(close[i] - low[i]) < 0.01:
                circuit_locked = True
                break
        if circuit_locked:
            continue

        # Average daily volume (20-day)
        avg_vol = float(np.mean(volume[-20:])) if len(volume) >= 20 else float(np.mean(volume))
        if avg_vol < WATCHLIST_CFG["min_avg_volume"]:
            continue

        # Depth from 20-day high
        recent_high = float(np.max(close[-20:]))
        current = float(close[-1])
        depth_pct = (current - recent_high) / recent_high

        sector = grp["sector"].iloc[-1] if "sector" in grp.columns else ""

        candidates.append({
            "ticker": ticker,
            "down_days": down_days,
            "depth_pct": round(depth_pct, 4),
            "avg_volume": int(avg_vol),
            "close": round(current, 2),
            "sector": sector,
        })

    if not candidates:
        log.info("No watchlist candidates found")
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values("depth_pct").head(WATCHLIST_CFG["max_watchlist_size"])
    log.info("Watchlist: %d candidates (down_days >= %d)", len(df), WATCHLIST_CFG["min_near_down_days"])
    return df


def get_watchlist_with_tokens() -> list[dict]:
    """Generate watchlist with Kite instrument tokens for live ticker subscription.

    Returns list of dicts ready for LiveSignalEngine.
    """
    from faralpha.kite.fetch_intraday import _get_kite, _load_instrument_map

    wl = generate_watchlist()
    if wl.empty:
        return []

    kite = _get_kite()
    inst_map = _load_instrument_map(kite)

    # Get average first-hour volume from intraday DB
    try:
        from faralpha.kite.intraday_db import get_conn
        icon = get_conn(read_only=True)
        avg_fh = icon.execute("""
            SELECT ticker, AVG(first_hour_vol) as avg_fh_vol
            FROM (
                SELECT ticker, DATE_TRUNC('day', ts) as day,
                       SUM(CASE WHEN EXTRACT(HOUR FROM ts) = 9
                                  OR (EXTRACT(HOUR FROM ts) = 10 AND EXTRACT(MINUTE FROM ts) < 15)
                            THEN volume ELSE 0 END) as first_hour_vol
                FROM candles
                WHERE interval = '15minute'
                  AND ts >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                GROUP BY ticker, DATE_TRUNC('day', ts)
            ) sub
            GROUP BY ticker
        """).df()
        icon.close()
        fh_map = dict(zip(avg_fh["ticker"], avg_fh["avg_fh_vol"]))
    except Exception as e:
        log.warning("Could not load avg first-hour volume: %s", e)
        fh_map = {}

    result = []
    for _, row in wl.iterrows():
        ticker = row["ticker"]
        # inst_map keys may have .NS suffix; try both formats
        token = inst_map.get(ticker) or inst_map.get(f"{ticker}.NS") or inst_map.get(ticker.replace(".NS", ""))
        if token is None:
            log.warning("No instrument token for %s — skipping", ticker)
            continue
        result.append({
            "ticker": ticker,
            "instrument_token": token,
            "down_days": row["down_days"],
            "depth_pct": row["depth_pct"],
            "close": row["close"],
            "avg_first_hour_vol": fh_map.get(ticker, 0) or fh_map.get(f"{ticker}.NS", 0) or fh_map.get(ticker.replace(".NS", ""), 0),
            "avg_volume": row["avg_volume"],
            "sector": row.get("sector", ""),
        })

    log.info("Watchlist with tokens: %d stocks ready for live monitoring", len(result))
    return result


if __name__ == "__main__":
    wl = generate_watchlist()
    if wl.empty:
        print("No watchlist candidates found.")
    else:
        print(f"\n{'='*70}")
        print(f"  PRE-MARKET WATCHLIST — {datetime.now().strftime('%Y-%m-%d')}")
        print(f"  Criteria: down_days >= {WATCHLIST_CFG['min_near_down_days']}, "
              f"avg_vol >= {WATCHLIST_CFG['min_avg_volume']:,}")
        print(f"{'='*70}\n")
        print(wl.to_string(index=False))
        print(f"\n  {len(wl)} stocks on tomorrow's radar")
