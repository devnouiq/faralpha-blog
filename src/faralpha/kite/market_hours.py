"""NSE market hours gate + tick-size utilities.

Every buy/sell/SL operation MUST call is_market_open() before touching Kite.
Server can start/stop at any time — off-hours calls are silently skipped.
"""

from __future__ import annotations

import math
from datetime import datetime, time

from faralpha.utils.logger import get_logger

log = get_logger("market_hours")

# NSE equity session (continuous trading)
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

# Pre-open session — orders can be placed but not filled
PRE_OPEN_START = time(9, 0)

# Default NSE tick size
TICK_SIZE = 0.05


def is_market_open() -> bool:
    """True if current IST time is within NSE continuous trading hours (Mon-Fri 09:15-15:30)."""
    now = datetime.now()
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


def is_pre_open() -> bool:
    """True if in NSE pre-open session (09:00-09:15)."""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return PRE_OPEN_START <= now.time() < MARKET_OPEN


def market_status() -> str:
    """Human-readable market status."""
    now = datetime.now()
    if now.weekday() >= 5:
        return "weekend"
    t = now.time()
    if t < PRE_OPEN_START:
        return "pre_market"
    if t < MARKET_OPEN:
        return "pre_open"
    if t <= MARKET_CLOSE:
        return "open"
    return "closed"


def round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price DOWN to nearest NSE tick size."""
    return round(math.floor(price / tick) * tick, 2)


def round_up_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price UP to nearest NSE tick size."""
    return round(math.ceil(price / tick) * tick, 2)
