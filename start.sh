#!/bin/bash
# ─────────────────────────────────────────────
# FarAlpha Quant Trader — One-Command Start
# ─────────────────────────────────────────────
# Usage:  ./start.sh  →  https://faralpha.localhost
# Auto-installs portless + starts HTTPS proxy if needed.
# ─────────────────────────────────────────────

set -e
cd "$(dirname "$0")"

PORT="${PORT:-8000}"

# Kill any existing server / stale Python process holding the DuckDB lock
if lsof -ti:"$PORT" >/dev/null 2>&1; then
  echo "⚠️  Killing existing process on port $PORT…"
  lsof -ti:"$PORT" | xargs kill -9 2>/dev/null || true
  sleep 1
fi
# Also kill any lingering Python process locking the DuckDB file
DB_FILE="db/market.duckdb"
if [ -f "$DB_FILE" ]; then
  lsof "$DB_FILE" 2>/dev/null | awk 'NR>1{print $2}' | sort -u | xargs kill -9 2>/dev/null || true
fi

# Rebuild UI if source is newer than dist
if [ ! -d "ui/dist" ] || [ "$(find ui/src -newer ui/dist -print -quit 2>/dev/null)" ]; then
  echo "📦 Building dashboard UI..."
  (cd ui && npm install --silent && npm run build)
  echo "✅ UI built"
fi

# ── Kite login check ──────────────────────────
# Reads .env, checks if KITE_ACCESS_TOKEN works.
# If not, opens the login URL and prompts for the request_token.
_check_kite_token() {
  # Load existing .env values
  if [ -f .env ]; then
    set -a; source .env; set +a
  fi

  # Quick check: try a lightweight Kite API call
  if [ -n "$KITE_ACCESS_TOKEN" ] && [ -n "$KITE_API_KEY" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      -H "X-Kite-Version: 3" \
      -H "Authorization: token ${KITE_API_KEY}:${KITE_ACCESS_TOKEN}" \
      "https://api.kite.trade/user/profile" 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "200" ]; then
      echo "🔑 Kite token valid"
      return 0
    fi
  fi

  echo ""
  echo "🔑 Kite access token expired or missing — let's refresh it."
  echo ""

  # Get login URL from Python
  LOGIN_URL=$(uv run python -c "from faralpha.kite.login import get_login_url; print(get_login_url())" 2>/dev/null)
  if [ -z "$LOGIN_URL" ]; then
    echo "⚠️  Could not generate Kite login URL (check KITE_API_KEY in .env)"
    echo "   Skipping Kite auth — intraday features won't work."
    return 1
  fi

  echo "   Open this link → log in → you'll be redirected to a URL"
  echo "   containing request_token=XXXX"
  echo ""
  echo "   $LOGIN_URL"
  echo ""

  # Try to open in default browser
  open "$LOGIN_URL" 2>/dev/null || xdg-open "$LOGIN_URL" 2>/dev/null || true

  printf "   Paste the request_token (or full callback URL): "
  read -r REQUEST_TOKEN

  # Extract token from full URL if user pasted the whole thing
  if echo "$REQUEST_TOKEN" | grep -q "request_token="; then
    REQUEST_TOKEN=$(echo "$REQUEST_TOKEN" | sed -n 's/.*request_token=\([^&]*\).*/\1/p')
  fi

  if [ -z "$REQUEST_TOKEN" ]; then
    echo "   ⚠️  No token entered — skipping Kite auth."
    return 1
  fi

  # Exchange request_token for access_token via Python
  echo "   Exchanging token…"
  uv run python -c "from faralpha.kite.login import exchange_request_token; exchange_request_token('$REQUEST_TOKEN')" 2>&1

  # Reload .env so the server picks up the new token
  if [ -f .env ]; then
    set -a; source .env; set +a
  fi
  echo "   ✅ Kite token refreshed"
}

_check_kite_token || true
echo ""

# Ensure portless is available (npx avoids global install / sudo)
if ! command -v portless &>/dev/null; then
  echo "📦 Installing portless…"
  npm install -g portless 2>/dev/null || npx --yes portless --version >/dev/null 2>&1
fi

# Resolve the portless binary (global or npx)
if command -v portless &>/dev/null; then
  PL="portless"
else
  PL="npx --yes portless"
fi

# Ensure the HTTPS proxy is running
if ! $PL proxy status &>/dev/null; then
  echo "🔒 Starting portless HTTPS proxy…"
  $PL proxy start --https 2>/dev/null || true
  sleep 1
fi

URL="https://faralpha.localhost"
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  FarAlpha Quant Trader Dashboard             ║"
echo "║  $URL                        ║"
echo "║                                              ║"
echo "║  1. Click 'Full Scan' to sync + find signals ║"
echo "║  2. Enable 'Daily Scanner' for auto alerts   ║"
echo "║  3. Add positions to track stop-losses       ║"
echo "║                                              ║"
echo "║  Press Ctrl+C to stop                        ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
exec $PL faralpha --force sh -c 'exec uv run uvicorn faralpha.api.app:app --host 0.0.0.0 --port ${PORT:-8000}'
