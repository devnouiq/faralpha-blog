#!/usr/bin/env python3
"""
Kite Login Flow
================
Zerodha Kite uses OAuth — the access_token expires daily and must be refreshed.

Usage:
    # Step 1: Get login URL
    uv run python -m faralpha.kite.login

    # Step 2: Open the URL in browser, log in, and copy the `request_token` from
    #   the redirect URL (format: http://127.0.0.1/?request_token=XXXX&action=login&status=success)

    # Step 3: Exchange for access token
    uv run python -m faralpha.kite.login --request-token XXXX

    The access token will be saved to .env and loaded automatically.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

from faralpha import config

PROJECT_ROOT = config.PROJECT_ROOT
KITE_CFG = config.KITE


def get_login_url() -> str:
    """Generate the Kite login URL."""
    api_key = KITE_CFG["api_key"]
    if not api_key:
        print("ERROR: KITE_API_KEY not set in .env")
        sys.exit(1)
    return f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"


def exchange_request_token(request_token: str) -> str:
    """Exchange request_token for access_token using Kite API."""
    from kiteconnect import KiteConnect

    api_key = KITE_CFG["api_key"]
    api_secret = KITE_CFG["api_secret"]

    if not api_key or not api_secret:
        print("ERROR: KITE_API_KEY and KITE_API_SECRET must be set in .env")
        sys.exit(1)

    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]

    # Save to .env file
    _update_env_file("KITE_ACCESS_TOKEN", access_token)

    # Also set in current process
    os.environ["KITE_ACCESS_TOKEN"] = access_token

    print("Access token saved to .env (valid until ~6:00 AM IST tomorrow)")
    return access_token


def _update_env_file(key: str, value: str):
    """Update or add a key=value in the .env file."""
    env_path = PROJECT_ROOT / ".env"

    if env_path.exists():
        content = env_path.read_text()
        # Replace existing line or append
        pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
        if pattern.search(content):
            content = pattern.sub(f"{key}={value}", content)
        else:
            content = content.rstrip("\n") + f"\n{key}={value}\n"
    else:
        content = f"{key}={value}\n"

    env_path.write_text(content)


def main():
    parser = argparse.ArgumentParser(description="Kite login flow")
    parser.add_argument(
        "--request-token", "-r",
        help="Request token from the Kite redirect URL",
    )
    args = parser.parse_args()

    if args.request_token:
        exchange_request_token(args.request_token)
        print("\nYou can now fetch intraday data:")
        print("  uv run python -m faralpha.kite.fetch_intraday --interval 15minute")
    else:
        url = get_login_url()
        print("1. Open this URL in your browser:")
        print(f"   {url}")
        print()
        print("2. Log in with your Zerodha credentials")
        print()
        print("3. After redirect, copy the 'request_token' from the URL")
        print(f"   (it looks like: http://127.0.0.1/?request_token=XXXX&action=login)")
        print()
        print(f"4. Run:")
        print(f"   uv run python -m faralpha.kite.login --request-token XXXX")


if __name__ == "__main__":
    main()
