#!/usr/bin/env python3
"""
FarAlpha Trading Dashboard — API Server
=========================================
Slim entrypoint: lifespan, CORS, WebSocket, static files, router includes.

Run:
    uv run uvicorn faralpha.api.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from faralpha.utils.db import init_schema, close_shared
from faralpha.utils.logger import get_logger
from faralpha.api import state
from faralpha.api.state import (
    set_event_loop, set_live_engine, set_scanner_task,
    ws_clients, scanner_state, broadcast_from_thread,
)

# Route modules
from faralpha.api.routes.status import router as status_router
from faralpha.api.routes.pipeline_routes import router as pipeline_router
from faralpha.api.routes.signals import router as signals_router
from faralpha.api.routes.positions import router as positions_router
from faralpha.api.routes.scheduler_routes import router as scheduler_router
from faralpha.api.routes.intraday import router as intraday_router
from faralpha.api.routes.orders import router as orders_router
from faralpha.api.routes.kite_auth import router as kite_auth_router, zerodha_router
from faralpha.api.routes.logs_routes import router as logs_router

log = get_logger("dashboard_api")


# ══════════════════════════════════════════════
#  AUTO-START: live ticker + auto-trade
# ══════════════════════════════════════════════

async def _auto_start_ticker() -> None:
    """Start live ticker and enable auto-trade on server startup."""
    try:
        from faralpha.kite.watchlist import get_watchlist_with_tokens
        from faralpha.kite.live_ticker import start_kite_ticker

        watchlist = get_watchlist_with_tokens()

        # Also subscribe to open-position tickers for live CMP in logbook
        try:
            from faralpha.kite.fetch_intraday import _get_kite, _load_instrument_map
            from faralpha.utils.db import get_conn
            con = get_conn()
            open_tickers = con.execute("""
                SELECT DISTINCT ticker FROM orders
                WHERE status IN ('bought', 'protected', 'UNPROTECTED', 'buy_placed')
                  AND exit_order_id IS NULL
            """).fetchall()
            con.close()
            wl_tickers = {w["ticker"] for w in watchlist}
            missing = [r[0] for r in open_tickers if r[0] not in wl_tickers]
            if missing:
                kite = _get_kite()
                inst_map = _load_instrument_map(kite)
                for ticker in missing:
                    token = inst_map.get(ticker) or inst_map.get(f"{ticker}.NS")
                    if token:
                        watchlist.append({
                            "ticker": ticker, "instrument_token": token,
                            "down_days": 0, "depth_pct": 0, "close": 0,
                            "avg_first_hour_vol": 0, "avg_volume": 0, "sector": "",
                        })
                log.info("Auto-start: added %d open-position tickers to watchlist: %s",
                         len(missing), missing)
        except Exception as e:
            log.warning("Auto-start: could not add open-position tickers: %s", e)

        if not watchlist:
            log.info("Auto-start: no watchlist stocks — ticker not started")
            return

        def _on_order_event(evt, data):
            """Broadcast individual order event + full orders snapshot."""
            broadcast_from_thread(evt, data)
            try:
                from faralpha.kite.kite_orders import order_manager
                broadcast_from_thread("orders_update", order_manager.status())
            except Exception:
                pass

        def on_signal(signal):
            broadcast_from_thread("intraday_signal", signal)
            from faralpha.api.routes.intraday import _persist_signal
            _persist_signal(signal)
            from faralpha.kite.kite_orders import order_manager
            order_manager.on_signal(signal)

        _, engine = start_kite_ticker(watchlist, on_signal=on_signal)
        set_live_engine(engine)

        # Push live status to UI via WebSocket every 2s (replaces HTTP polling)
        def on_status_update(status):
            broadcast_from_thread("intraday_status", status)

        engine.on_status_update = on_status_update

        log.info("Auto-start: live ticker started with %d stocks", len(watchlist))

        # Enable auto-trade
        from faralpha.kite.kite_orders import order_manager
        order_manager.set_on_order_event(_on_order_event)
        result = order_manager.enable()
        if result.get("error"):
            log.critical("Auto-start: Kite health check FAILED — %s: %s",
                         result["error"], result.get("detail", ""))
            log.critical("Auto-start: auto-trade NOT enabled. Fix the issue and restart.")
            return
        log.info("Auto-start: auto-trade enabled")

        # Reconcile: restore positions from Kite that DB lost
        reconciled = order_manager.reconcile_from_kite()
        if reconciled:
            log.info("Auto-start: reconciled %d positions from Kite", len(reconciled))

        # Re-place SL for multi-day holds (DAY orders expire at 3:30 PM)
        results = order_manager.morning_sl_refresh()
        if results:
            log.info("Auto-start: morning SL refresh — %d positions", len(results))

    except Exception as e:
        log.warning("Auto-start ticker failed (non-fatal): %s", e)


# ══════════════════════════════════════════════
#  APP LIFECYCLE
# ══════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    set_event_loop(asyncio.get_event_loop())
    log.info("Dashboard API starting…")
    init_schema()

    # Auto-purge delisted stocks on every startup
    from faralpha.api.sync_prices import purge_delisted
    purged = purge_delisted(market="india")
    for mkt, info in purged.items():
        n = info.get("delisted_removed", 0)
        if n:
            log.info("Startup purge: %s — removed %d delisted stocks", mkt.upper(), n)

    # Auto-start live ticker + auto-trade
    await _auto_start_ticker()

    yield

    scanner_state["running"] = False
    if state.scanner_task and not state.scanner_task.done():
        state.scanner_task.cancel()
    close_shared()
    log.info("Dashboard API shut down.")


app = FastAPI(title="FarAlpha Trading Dashboard", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Include routers ──
app.include_router(status_router)
app.include_router(pipeline_router)
app.include_router(signals_router)
app.include_router(positions_router)
app.include_router(scheduler_router)
app.include_router(intraday_router)
app.include_router(orders_router)
app.include_router(kite_auth_router)
app.include_router(zerodha_router)
app.include_router(logs_router)


# ══════════════════════════════════════════════
#  WEBSOCKET
# ══════════════════════════════════════════════

@app.websocket("/ws/events")
async def ws_events(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    log.info(f"WS connected ({len(ws_clients)} clients)")
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)
        log.info(f"WS disconnected ({len(ws_clients)} clients)")


# ══════════════════════════════════════════════
#  STATIC FILES  (serve React build in production)
# ══════════════════════════════════════════════

_ui_dist = Path(__file__).resolve().parent.parent.parent.parent / "ui" / "dist"
if _ui_dist.exists():
    from starlette.staticfiles import StaticFiles

    _static_app = StaticFiles(directory=str(_ui_dist), html=True)

    @app.middleware("http")
    async def static_cache_headers(request, call_next):
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        elif path == "/" or path.endswith(".html"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response

    app.mount("/", _static_app, name="ui")
