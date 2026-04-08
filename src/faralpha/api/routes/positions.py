"""Routes: /api/positions (CRUD)"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from faralpha.api.state import load_positions, save_positions
from faralpha.api.helpers import check_stops

router = APIRouter()


class PositionCreate(BaseModel):
    ticker: str
    market: str = "india"
    entry_date: str
    entry_price: float
    shares: float
    notes: str = ""


@router.get("/api/positions")
async def get_positions():
    positions = load_positions()
    stop_alerts = check_stops(positions) if positions else []
    if positions:
        save_positions(positions)
    return {"positions": positions, "stop_alerts": stop_alerts}


@router.post("/api/positions")
async def add_position(pos: PositionCreate):
    positions = load_positions()
    for p in positions:
        if p["ticker"] == pos.ticker and p["market"] == pos.market:
            raise HTTPException(400, f"{pos.ticker} already tracked in {pos.market}")
    new_pos = {
        "ticker": pos.ticker,
        "market": pos.market,
        "entry_date": pos.entry_date,
        "entry_price": pos.entry_price,
        "shares": pos.shares,
        "highest_price": pos.entry_price,
        "notes": pos.notes,
    }
    positions.append(new_pos)
    save_positions(positions)
    return {"status": "added", "position": new_pos}


@router.delete("/api/positions/{market}/{ticker}")
async def remove_position(market: str, ticker: str):
    positions = load_positions()
    before = len(positions)
    positions = [p for p in positions if not (p["ticker"] == ticker and p["market"] == market)]
    if len(positions) == before:
        raise HTTPException(404, f"{ticker} not found in {market}")
    save_positions(positions)
    return {"status": "removed", "ticker": ticker, "market": market}
