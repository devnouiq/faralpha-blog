"""Shared global state, broadcast helpers, and position I/O."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path

from fastapi import WebSocket

from faralpha import config
from faralpha.utils.logger import get_logger

log = get_logger("dashboard_api")

POSITIONS_FILE = config.DATA_DIR / "positions.json"

scanner_state: dict = {
    "running": False,
    "mode": "daily",
    "interval_minutes": 60,
    "market": "india",
    "last_run": None,
    "last_run_market": None,
    "next_run": None,
    "next_run_market": None,
    "last_result": None,
    "scans_today": {"india": None},
}

scanner_task: asyncio.Task | None = None
ws_clients: list[WebSocket] = []
op_lock = asyncio.Lock()
event_loop: asyncio.AbstractEventLoop | None = None
live_engine = None  # LiveSignalEngine instance


def set_scanner_task(task: asyncio.Task | None) -> None:
    global scanner_task
    scanner_task = task


def set_event_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global event_loop
    event_loop = loop


def set_live_engine(engine) -> None:
    global live_engine
    live_engine = engine


# ── Position file I/O ──

def load_positions() -> list[dict]:
    if POSITIONS_FILE.exists():
        with open(POSITIONS_FILE) as f:
            return json.load(f).get("positions", [])
    return []


def save_positions(positions: list[dict]) -> None:
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(POSITIONS_FILE, "w") as f:
        json.dump({"positions": positions, "updated": datetime.now().isoformat()}, f, indent=2)


# ── WebSocket broadcast ──

async def broadcast(event_type: str, data: dict) -> None:
    msg = json.dumps({"type": event_type, "data": data, "ts": datetime.now().isoformat()})
    dead: list[WebSocket] = []
    for ws in ws_clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.remove(ws)


def broadcast_from_thread(event_type: str, data: dict) -> None:
    """Bridge: call broadcast from a sync thread."""
    if event_loop and event_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(event_type, data), event_loop)


# ── DB helper ──

def table_exists(con, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 0")
        return True
    except Exception:
        return False
