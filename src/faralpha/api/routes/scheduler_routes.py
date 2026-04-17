"""Routes: /api/scheduler/*, /api/schedule"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter
from pydantic import BaseModel

from faralpha.utils.logger import get_logger
from faralpha.api import state
from faralpha.api.state import scanner_state, set_scanner_task
from faralpha.api.scheduler import (
    get_schedule_info,
    daily_scheduler_loop,
    interval_scanner_loop,
)

log = get_logger("dashboard_api")
router = APIRouter()


class SchedulerRequest(BaseModel):
    mode: str = "daily"
    interval_minutes: int = 60
    market: str = "india"


@router.post("/api/scheduler/start")
async def start_scheduler(req: SchedulerRequest):
    if scanner_state["running"]:
        return {"status": "already_running", **scanner_state}

    scanner_state["running"] = True
    scanner_state["mode"] = req.mode
    scanner_state["interval_minutes"] = req.interval_minutes
    scanner_state["market"] = req.market

    if req.mode == "daily":
        set_scanner_task(asyncio.create_task(daily_scheduler_loop()))
        log.info("Daily scheduler started: India @ 4:30 PM IST")
    else:
        set_scanner_task(asyncio.create_task(interval_scanner_loop()))
        log.info(f"Interval scanner started: every {req.interval_minutes}m for {req.market}")

    return {"status": "started", **scanner_state}


@router.post("/api/scheduler/stop")
async def stop_scheduler():
    scanner_state["running"] = False
    scanner_state["next_run"] = None
    scanner_state["next_run_market"] = None
    if state.scanner_task and not state.scanner_task.done():
        state.scanner_task.cancel()
    set_scanner_task(None)
    log.info("Scanner stopped")
    return {"status": "stopped"}


@router.get("/api/scheduler")
async def scheduler_status():
    return scanner_state


@router.get("/api/schedule")
async def get_schedule():
    """Return the daily scan schedule with countdowns."""
    schedule = get_schedule_info()
    return {
        "schedule": schedule,
        "scheduler": scanner_state,
        "data_type": "daily",
        "explanation": (
            "All data is DAILY (end-of-day candles). "
            "Scans run once after each market closes: "
            "India at 4:30 PM IST, US at 5:00 PM ET. "
            "When a scan finds buy candidates, you get a real-time alert. "
            "Buy at next day's market open. "
            "Stop-loss breaches are also alerted — sell at next open."
        ),
    }
