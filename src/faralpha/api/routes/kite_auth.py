"""
Kite Auth Routes — OAuth2 login flow via the UI.

Endpoints:
  GET  /api/kite/auth-status   → { logged_in, user_name }
  GET  /api/kite/login-url     → { url }
  POST /api/kite/callback      → exchanges request_token, hot-reloads config
"""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from faralpha import config
from faralpha.utils.logger import get_logger

log = get_logger("kite_auth")
router = APIRouter(prefix="/api/kite", tags=["kite-auth"])

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

@router.post("/callback")
def callback(body: CallbackRequest):
    """Exchange the request_token for an access_token and hot-reload config."""
    from kiteconnect import KiteConnect
    from faralpha.kite.login import _update_env_file

    api_key = KITE_CFG.get("api_key", "")
    api_secret = KITE_CFG.get("api_secret", "")
    if not api_key or not api_secret:
        raise HTTPException(400, "KITE_API_KEY and KITE_API_SECRET not configured")

    try:
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(body.request_token, api_secret=api_secret)
        access_token = data["access_token"]
    except Exception as e:
        log.error("Token exchange failed: %s", e)
        raise HTTPException(400, f"Token exchange failed: {e}")

    # 1. Update .env file (persists across restarts)
    _update_env_file("KITE_ACCESS_TOKEN", access_token)

    # 2. Hot-reload into running config (no restart needed)
    KITE_CFG["access_token"] = access_token
    os.environ["KITE_ACCESS_TOKEN"] = access_token

    # 3. Reset cached kite clients so they pick up new token
    _reset_kite_clients()

    # 4. Get user name for UI
    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
        user_name = profile.get("user_name", profile.get("user_id", "Connected"))
    except Exception:
        user_name = "Connected"

    log.info("Kite login successful — user: %s, token saved to .env", user_name)
    return {"success": True, "user_name": user_name}


def _reset_kite_clients():
    """Reset cached KiteConnect instances so they use the new access_token."""
    try:
        from faralpha.kite.kite_orders import order_manager
        order_manager._kite = None  # will be re-created on next _get_kite() call
        log.info("Reset order_manager kite client")
    except Exception as e:
        log.debug("Could not reset order_manager: %s", e)
    # fetch_intraday._get_kite() creates fresh instances each call — no reset needed
