"""
Kite Auth Routes — OAuth2 login flow via the UI.

Endpoints:
  GET  /api/kite/auth-status   → { logged_in, user_name }
  GET  /api/kite/login-url     → { url }
  POST /api/kite/callback      → exchanges request_token, hot-reloads config
a  GET  /api/zerodha/callback   → same exchange (browser redirect from Kite), then redirect to SPA
"""

from __future__ import annotations

import os
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from faralpha import config
from faralpha.utils.logger import get_logger

log = get_logger("kite_auth")
router = APIRouter(prefix="/api/kite", tags=["kite-auth"])
zerodha_router = APIRouter(prefix="/api/zerodha", tags=["kite-auth"])

KITE_CFG = config.KITE


# ── Models ──

class CallbackRequest(BaseModel):
    request_token: str


# ── Auth status ──

@router.get("/auth-status")
def auth_status():
    """Check if we have a valid Kite access token."""
    token = KITE_CFG.get("access_token", "")
    if not token:
        return {"logged_in": False, "user_name": None}

    # Try a lightweight API call to verify the token
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=KITE_CFG["api_key"])
        kite.set_access_token(token)
        profile = kite.profile()
        return {
            "logged_in": True,
            "user_name": profile.get("user_name", profile.get("user_id", "Connected")),
        }
    except Exception:
        return {"logged_in": False, "user_name": None}


# ── Login URL ──

@router.get("/login-url")
def login_url():
    """Generate the Kite OAuth login URL."""
    api_key = KITE_CFG.get("api_key", "")
    if not api_key:
        raise HTTPException(400, "KITE_API_KEY not configured")
    url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
    return {"url": url}


# ── Callback (exchange request_token → access_token) ──


def _redirect_origin(request: Request) -> str:
    """Public site origin behind nginx (X-Forwarded-*) or direct."""
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",")[0].strip()
    if host:
        return f"{proto}://{host}"
    return str(request.base_url).rstrip("/")


def exchange_request_token(request_token: str) -> str:
    """Exchange request_token for access_token, persist, hot-reload. Returns user_name."""
    from kiteconnect import KiteConnect
    from faralpha.kite.login import _update_env_file

    api_key = KITE_CFG.get("api_key", "")
    api_secret = KITE_CFG.get("api_secret", "")
    if not api_key or not api_secret:
        raise HTTPException(400, "KITE_API_KEY and KITE_API_SECRET not configured")

    try:
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
    except Exception as e:
        log.error("Token exchange failed: %s", e)
        raise HTTPException(400, f"Token exchange failed: {e}")

    _update_env_file("KITE_ACCESS_TOKEN", access_token)
    KITE_CFG["access_token"] = access_token
    os.environ["KITE_ACCESS_TOKEN"] = access_token
    _reset_kite_clients()

    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
        user_name = profile.get("user_name", profile.get("user_id", "Connected"))
    except Exception:
        user_name = "Connected"

    log.info("Kite login successful — user: %s, token saved to .env", user_name)
    return user_name


@router.post("/callback")
def callback(body: CallbackRequest):
    """Exchange the request_token for an access_token and hot-reload config."""
    user_name = exchange_request_token(body.request_token)
    return {"success": True, "user_name": user_name}


@zerodha_router.get("/callback")
def zerodha_callback_get(
    request: Request,
    request_token: str = Query(..., description="From Kite redirect"),
    status: str | None = Query(None),
):
    """Kite redirects the browser here with GET + query params; send user back to the SPA."""
    if status and status != "success":
        log.warning("Kite callback status=%s (expected success)", status)
    base = _redirect_origin(request)
    try:
        exchange_request_token(request_token)
    except HTTPException as e:
        err = str(e.detail) if isinstance(e.detail, str) else "Login failed"
        return RedirectResponse(url=f"{base}/?kite_login=error&msg={quote(err, safe='')}", status_code=302)
    return RedirectResponse(url=f"{base}/?kite_login=ok", status_code=302)


def _reset_kite_clients():
    """Reset cached KiteConnect instances so they use the new access_token."""
    try:
        from faralpha.kite.kite_orders import order_manager
        order_manager._kite = None  # will be re-created on next _get_kite() call
        log.info("Reset order_manager kite client")
    except Exception as e:
        log.debug("Could not reset order_manager: %s", e)
    # fetch_intraday._get_kite() creates fresh instances each call — no reset needed
