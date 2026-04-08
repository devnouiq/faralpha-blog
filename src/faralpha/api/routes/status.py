"""Routes: /api/status"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from faralpha import config
from faralpha.api.state import scanner_state, op_lock, load_positions
from faralpha.api.helpers import get_market_status
from faralpha.api.scheduler import get_schedule_info

router = APIRouter()
TZ_UTC = timezone.utc


@router.get("/api/status")
async def get_status():
    markets_status = {m: get_market_status(m) for m in config.MARKETS}

    today = datetime.now(TZ_UTC).date()
    for m, ms in markets_status.items():
        if ms.get("last_price_date"):
            try:
                last = datetime.strptime(ms["last_price_date"], "%Y-%m-%d").date()
                days_old = (today - last).days
                ms["days_stale"] = days_old
                ms["freshness"] = (
                    "fresh" if days_old <= 1 else
                    "stale" if days_old <= 3 else
                    "very_stale"
                )
            except Exception:
                ms["days_stale"] = None
                ms["freshness"] = "unknown"

    return {
        "markets": markets_status,
        "scanner": scanner_state,
        "schedule": get_schedule_info(),
        "busy": op_lock.locked(),
        "positions": len(load_positions()),
        "data_type": "daily",
    }
