"""Routes: /api/sync, /api/pipeline, /api/scan, /api/universe, /api/cleanup"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.api.state import (
    op_lock, broadcast, broadcast_from_thread,
    load_positions, save_positions,
)
from faralpha.api.helpers import check_stops
from faralpha.api.pipeline import run_pipeline, run_full_scan

log = get_logger("dashboard_api")
router = APIRouter()


class SyncRequest(BaseModel):
    market: str = "india"
    force: bool = False
    purge: bool = False


class PipelineRequest(BaseModel):
    market: str = "india"


class ScanRequest(BaseModel):
    market: str = "india"
    force: bool = False


class UniverseRequest(BaseModel):
    market: str = "india"
    purge_delisted: bool = True


@router.post("/api/sync")
async def sync(req: SyncRequest):
    if op_lock.locked():
        raise HTTPException(409, "Another operation is in progress")
    log.info("POST /api/sync  market=%s  force=%s  purge=%s", req.market, req.force, req.purge)
    async with op_lock:
        from faralpha.api.sync_prices import sync_prices_kite
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: sync_prices_kite(market=req.market, force=req.force, purge=req.purge)
        )
        log.info("POST /api/sync complete for %s", req.market)
        return {"status": "complete", "result": result}


@router.post("/api/pipeline")
async def pipeline(req: PipelineRequest):
    if op_lock.locked():
        raise HTTPException(409, "Another operation is in progress")
    async with op_lock:
        result = await asyncio.get_event_loop().run_in_executor(
            None, run_pipeline, req.market
        )
        return {"status": "complete", "result": result}


@router.post("/api/scan")
async def full_scan(req: ScanRequest):
    """Full scan: sync + pipeline + stop checks."""
    if op_lock.locked():
        log.warning("Scan rejected — another operation is in progress")
        raise HTTPException(409, "Another operation is in progress")
    log.info("POST /api/scan  market=%s  force=%s", req.market, req.force)
    async with op_lock:
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: run_full_scan(req.market, req.force)
        )
        positions = load_positions()
        stop_alerts = check_stops(positions) if positions else []
        if positions:
            save_positions(positions)

        for a in stop_alerts:
            await broadcast("sell_signal", a)
        for s in result.get("signals", []):
            await broadcast("buy_signal", s)

        log.info("POST /api/scan complete — signals=%d  stop_alerts=%d  errors=%d",
                 len(result.get("signals", [])), len(stop_alerts), len(result.get("errors", [])))
        return {"status": "complete", "result": result, "stop_alerts": stop_alerts}


@router.post("/api/universe")
async def refresh_universe(req: UniverseRequest):
    """Re-download exchange listings and optionally purge delisted stocks."""
    if op_lock.locked():
        raise HTTPException(409, "Another operation is in progress")
    log.info("POST /api/universe  market=%s  purge=%s", req.market, req.purge_delisted)
    async with op_lock:
        def _do_universe():
            from faralpha.pipeline import s01_universe
            from faralpha.api.sync_prices import purge_delisted

            result: dict = {}

            if req.purge_delisted:
                broadcast_from_thread("scan_progress",
                    {"step": "cleanup", "message": "Purging delisted stocks…"})
                result["purged"] = purge_delisted(market=req.market)

            broadcast_from_thread("scan_progress",
                {"step": "universe", "message": "Downloading exchange listings…"})
            s01_universe.run(market=req.market, enrich=False)

            con = get_conn(read_only=True)
            for mkt in (config.MARKETS if req.market == "both" else [req.market]):
                row = con.execute(
                    "SELECT COUNT(*) FROM stocks "
                    "WHERE market = ? AND (delisting_date IS NULL OR delisting_date > CURRENT_DATE)",
                    [mkt],
                ).fetchone()
                result[f"{mkt}_active"] = row[0] if row else 0
            con.close()
            return result

        result = await asyncio.get_event_loop().run_in_executor(None, _do_universe)
        broadcast_from_thread("scan_progress",
            {"step": "done", "message": "Universe refresh complete"})
        log.info("POST /api/universe complete: %s", result)
        return {"status": "complete", "result": result}


@router.post("/api/cleanup")
async def cleanup_delisted(req: SyncRequest):
    """Purge delisted stocks and their price data from the database."""
    if op_lock.locked():
        raise HTTPException(409, "Another operation is in progress")
    log.info("POST /api/cleanup  market=%s", req.market)
    async with op_lock:
        from faralpha.api.sync_prices import purge_delisted
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: purge_delisted(market=req.market)
        )
        log.info("POST /api/cleanup complete: %s", result)
        return {"status": "complete", "result": result}
