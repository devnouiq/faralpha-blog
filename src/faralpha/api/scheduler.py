"""Scheduler loops and schedule-time helpers."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from faralpha.utils.logger import get_logger
from faralpha.api import state
from faralpha.api.state import broadcast, load_positions, save_positions
from faralpha.api.helpers import check_stops
from faralpha.api.pipeline import run_full_scan

log = get_logger("dashboard_api")

TZ_IST = ZoneInfo("Asia/Kolkata")
TZ_UTC = timezone.utc
INDIA_SCAN_HOUR, INDIA_SCAN_MIN = 16, 30  # IST


def next_scan_time_india() -> datetime:
    """Next India scan time: 4:30 PM IST, skip weekends."""
    now = datetime.now(TZ_IST)
    target = now.replace(hour=INDIA_SCAN_HOUR, minute=INDIA_SCAN_MIN, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return target.astimezone(TZ_UTC)


def get_next_scan() -> tuple[str, datetime]:
    return ("india", next_scan_time_india())


def get_schedule_info() -> dict:
    now_utc = datetime.now(TZ_UTC)
    india_next = next_scan_time_india()

    def _fmt(dt: datetime) -> dict:
        local_ist = dt.astimezone(TZ_IST)
        secs = max(0, (dt - now_utc).total_seconds())
        hours = int(secs // 3600)
        mins = int((secs % 3600) // 60)
        return {
            "utc": dt.isoformat(),
            "ist": local_ist.strftime("%I:%M %p IST"),
            "date": dt.strftime("%a %d %b"),
            "countdown": f"{hours}h {mins}m" if hours > 0 else f"{mins}m",
            "seconds_away": int(secs),
        }

    return {
        "india": _fmt(india_next),
        "next_market": "india",
    }


async def run_scan_for_market(market: str) -> None:
    """Execute a scan for one market and broadcast results."""
    log.info(f"Auto-scan starting for {market.upper()}")
    await broadcast("scan_progress", {
        "step": "start",
        "message": f"Daily scan starting for {market.upper()}…",
        "market": market,
    })

    try:
        async with state.op_lock:
            result = await asyncio.get_event_loop().run_in_executor(
                None, run_full_scan, market, True
            )
            state.scanner_state["last_run"] = datetime.now(TZ_UTC).isoformat()
            state.scanner_state["last_run_market"] = market
            state.scanner_state["last_result"] = result
            state.scanner_state["scans_today"][market] = datetime.now(TZ_UTC).isoformat()

            positions = load_positions()
            stop_alerts = check_stops(positions) if positions else []
            if positions:
                save_positions(positions)

            n_signals = len(result.get("signals", []))
            n_alerts = len(stop_alerts)
            log.info(f"Scan complete for {market}: {n_signals} signals, {n_alerts} stop alerts")

            await broadcast("scan_complete", {
                "market": market,
                "signals": result.get("signals", []),
                "stop_alerts": stop_alerts,
                "errors": result.get("errors", []),
            })
            for a in stop_alerts:
                await broadcast("sell_signal", a)
            for s in result.get("signals", []):
                await broadcast("buy_signal", s)

    except Exception as e:
        log.error(f"Scanner error ({market}): {e}")
        await broadcast("error", {"message": f"Scan failed for {market}: {e}"})


async def daily_scheduler_loop() -> None:
    """Smart daily scheduler: runs scan after India market closes."""
    log.info("Daily scheduler started — India @ 4:30 PM IST")
    await broadcast("scheduler_started", {
        "mode": "daily",
        "schedule": get_schedule_info(),
    })

    while state.scanner_state["running"]:
        next_market, next_time = get_next_scan()
        now_utc = datetime.now(TZ_UTC)
        wait_seconds = max(0, (next_time - now_utc).total_seconds())

        state.scanner_state["next_run"] = next_time.isoformat()
        state.scanner_state["next_run_market"] = next_market

        log.info(f"Next scan: {next_market.upper()} in {int(wait_seconds // 60)}m")

        while wait_seconds > 0 and state.scanner_state["running"]:
            sleep_time = min(30, wait_seconds)
            await asyncio.sleep(sleep_time)
            now_utc = datetime.now(TZ_UTC)
            wait_seconds = max(0, (next_time - now_utc).total_seconds())

        if not state.scanner_state["running"]:
            break

        await run_scan_for_market(next_market)
        await asyncio.sleep(5)


async def interval_scanner_loop() -> None:
    """Legacy interval-based scanner."""
    while state.scanner_state["running"]:
        market = state.scanner_state["market"]
        await run_scan_for_market(market)

        interval_s = state.scanner_state["interval_minutes"] * 60
        next_time = datetime.now(TZ_UTC) + timedelta(seconds=interval_s)
        state.scanner_state["next_run"] = next_time.isoformat()
        state.scanner_state["next_run_market"] = market

        for _ in range(interval_s):
            if not state.scanner_state["running"]:
                break
            await asyncio.sleep(1)
