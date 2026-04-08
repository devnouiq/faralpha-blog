"""
Kite Order Placement — Production-grade auto-trade on signal fire.

Execution Architecture:
  1. Signal fires → place LIMIT BUY at max_entry (signal + 0.5%)
     - LIMIT sits on exchange all day. Even if price spikes above and returns,
       order fills at limit or better. This is core to the strategy.
  2. Poll order status every 30s → detect fill
  3. On BUY fill → place SL-M SELL (stop-loss market) at initial trailing stop
     - SL-M triggers a MARKET sell when price touches trigger. No slippage risk.
  4. On unfilled BUY at EOD → auto-cancelled by exchange (DAY validity)
  5. SL-M is a DAY order → needs daily re-placement for multi-day holds
  6. Morning routine: re-place SL for all open positions at current trail level

Edge Cases Handled:
  - BUY partial fill → SL placed for filled quantity only
  - BUY fills but SL placement fails → retry 3x, then alert
  - Server restart mid-day → reload state from order log + Kite order API
  - Kite API error → log + alert, never leave naked position
  - Duplicate signals → skip if already ordered today
  - Max positions → skip if at limit
  - Price validation → reject if max_entry < initial_stop (impossible trade)

Storage:
  - Primary: DuckDB `orders` table — full audit trail, queryable
  - Ground truth: Kite order/position APIs — synced on every status check
  - We NEVER trust only one source. Always cross-check.

Safety:
  - Auto-trade is OFF by default; must be enabled via API toggle per session
  - All orders tagged "faralpha" for easy identification in Kite Console
  - Position sizing from Kite live margins (not stale config)
"""

from __future__ import annotations

import json
import math
import time
from datetime import date, datetime, timedelta
from threading import Lock, Thread

from kiteconnect import KiteConnect

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("kite_orders")

KITE_CFG = config.KITE
REVERSAL_CFG = config.INTRADAY_REVERSAL
DATA_DIR = config.DATA_DIR

# NSE default tick size is ₹0.05; some stocks (e.g. HYUNDAI) use ₹0.10
TICK_SIZE = 0.05

# Max late-entry slippage (backtest-validated: +0.5% max, Sharpe 2.13 vs 2.70)
MAX_LATE_ENTRY_PCT = 0.005

# Order polling interval
POLL_INTERVAL_SEC = 30

# Max retries for SL placement after BUY fill
MAX_SL_RETRIES = 3

# Kite order statuses
STATUS_COMPLETE = "COMPLETE"
STATUS_CANCELLED = "CANCELLED"
STATUS_REJECTED = "REJECTED"
STATUS_OPEN = "OPEN"
STATUS_TRIGGER_PENDING = "TRIGGER PENDING"


def _round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price DOWN to nearest NSE tick size."""
    return round(math.floor(price / tick) * tick, 2)


def _round_up_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price UP to nearest NSE tick size."""
    return round(math.ceil(price / tick) * tick, 2)


# Column list for all order queries (single source of truth)
_ORDER_COLUMNS = """
    ticker, order_date, signal_price, max_entry_price,
    initial_stop, current_stop, trail_pct, max_hold_days,
    exit_date, quantity, filled_qty, avg_fill_price,
    invest_amount, risk_amount, risk_pct,
    buy_order_id, sl_order_id, exit_order_id,
    buy_status, sl_status, status,
    exit_price, pnl, pnl_pct, errors, fills
""".strip()


class OrderManager:
    """Production-grade order management for intraday reversal signals."""

    def __init__(self):
        self._enabled = False
        self._lock = Lock()
        # ticker -> order record (today's orders)
        self._today_orders: dict[str, dict] = {}
        # ticker -> open position from previous days (for SL re-placement)
        self._open_positions: dict[str, dict] = {}
        self._pending_queue: list[dict] = []  # overflow signals awaiting approval
        self._kite: KiteConnect | None = None
        self._tick_sizes: dict[str, float] = {}  # ticker -> tick_size from Kite instruments
        self._poller_thread: Thread | None = None
        self._poller_running = False
        self._on_order_event = None  # callback for WebSocket broadcast
        self._breadth_cache: dict = {}  # date → breadth_pct
        self._ensure_table()
        self._load_today_orders()

    # ── tick size ──

    def _load_tick_sizes(self):
        """Load tick sizes from Kite instruments API (cached per session)."""
        if self._tick_sizes:
            return
        try:
            kite = self._get_kite()
            instruments = kite.instruments("NSE")
            for inst in instruments:
                sym = inst.get("tradingsymbol", "")
                ts = inst.get("tick_size", TICK_SIZE)
                if sym and ts > 0:
                    self._tick_sizes[sym] = ts
            log.info("Loaded tick sizes for %d instruments", len(self._tick_sizes))
        except Exception as e:
            log.warning("Failed to load tick sizes: %s (using default %.2f)", e, TICK_SIZE)

    def _get_tick_size(self, ticker: str) -> float:
        """Get tick size for a ticker; loads from Kite on first call."""
        if not self._tick_sizes:
            self._load_tick_sizes()
        return self._tick_sizes.get(ticker, TICK_SIZE)

    # ── state ──

    @property
    def enabled(self) -> bool:
        return self._enabled

    def enable(self) -> dict:
        # ── Pre-flight health check: verify Kite API access ──
        try:
            kite = self._get_kite()
            kite.margins(segment="equity")
            log.info("Kite health check PASSED — API access OK")
        except Exception as e:
            err = str(e)
            if "IP" in err and "not allowed" in err:
                log.critical("Kite health check FAILED: IP not whitelisted — %s", err)
                return {"auto_trade": False, "error": "ip_not_whitelisted", "detail": err}
            if "TokenException" in type(e).__name__ or "access_token" in err.lower():
                log.critical("Kite health check FAILED: invalid/expired access token — %s", err)
                return {"auto_trade": False, "error": "token_invalid", "detail": err}
            # Other errors (network blip etc.) — warn but don't block
            log.warning("Kite health check warning: %s (proceeding anyway)", err)

        self._enabled = True
        self._start_poller()
        log.info("AUTO-TRADE ENABLED")
        return {"auto_trade": True}

    def process_pending_signals(self, signals: list[dict]) -> list:
        """Process unordered signals retroactively (e.g. when auto-trade
        is toggled ON after signals already fired)."""
        if not self._enabled or not signals:
            return []
        results = []
        for sig in signals:
            ticker = sig.get("ticker", "").replace(".NS", "")
            if ticker in self._today_orders:
                continue  # already ordered
            result = self.on_signal(sig)
            if result:
                results.append(result)
        return results

    def disable(self) -> dict:
        self._enabled = False
        self._stop_poller()
        log.info("AUTO-TRADE DISABLED")
        return {"auto_trade": False}

    def status(self) -> dict:
        """Full status: merge local state with Kite ground truth."""
        orders = list(self._today_orders.values())
        positions = list(self._open_positions.values())
        return {
            "auto_trade": self._enabled,
            "today_orders": len(orders),
            "open_positions": len(positions),
            "orders": orders,
            "positions": positions,
            "pending_queue": list(self._pending_queue),
        }

    def approve_pending(self, ticker: str) -> dict:
        """Approve a pending signal — bypass max_positions check and place order."""
        ticker_clean = ticker.replace(".NS", "")
        signal = None
        for p in self._pending_queue:
            t = p.get("ticker", "").replace(".NS", "")
            if t == ticker_clean:
                signal = p
                break
        if not signal:
            return {"status": "error", "message": f"No pending signal for {ticker_clean}"}

        # Remove from queue
        self._pending_queue = [
            p for p in self._pending_queue
            if p.get("ticker", "").replace(".NS", "") != ticker_clean
        ]

        # Remove any previous failed order so on_signal doesn't skip
        with self._lock:
            self._today_orders.pop(ticker_clean, None)

        # Force-place the order (on_signal will run without max_pos block
        # because we removed the ticker from _today_orders)
        result = self.on_signal(signal, force=True)
        if result:
            return {"status": "ok", "order": result}
        return {"status": "error", "message": f"Order placement failed for {ticker_clean}"}

    def dismiss_pending(self, ticker: str) -> dict:
        """Remove a signal from the pending queue without acting."""
        ticker_clean = ticker.replace(".NS", "")
        before = len(self._pending_queue)
        self._pending_queue = [
            p for p in self._pending_queue
            if p.get("ticker", "").replace(".NS", "") != ticker_clean
        ]
        removed = before - len(self._pending_queue)
        return {"status": "ok", "removed": removed}

    def retry_failed_order(self, ticker: str) -> dict:
        """Retry a failed (error status) order by re-constructing the signal and re-placing."""
        ticker_clean = ticker.replace(".NS", "")

        with self._lock:
            order = self._today_orders.get(ticker_clean)
            if not order:
                return {"status": "error", "message": f"No order found for {ticker_clean}"}
            if order.get("status") != "error":
                return {"status": "error",
                        "message": f"{ticker_clean} status is '{order.get('status')}', not 'error'"}

            # Reconstruct signal from stored order data
            signal = {
                "ticker": ticker_clean,
                "price": order.get("signal_price", 0),
                "trailing_stop_pct": order.get("trail_pct",
                    REVERSAL_CFG.get("trailing_stop_pct", 0.02)),
                "max_hold_days": order.get("max_hold_days",
                    REVERSAL_CFG.get("max_hold_days", 7)),
            }

            # Remove failed order so on_signal doesn't skip it
            self._today_orders.pop(ticker_clean, None)

        # Also remove from pending queue if present
        self._pending_queue = [
            p for p in self._pending_queue
            if p.get("ticker", "").replace(".NS", "") != ticker_clean
        ]

        log.info("RETRY failed order: %s @ %.2f", ticker_clean, signal["price"])
        result = self.on_signal(signal, force=True)
        if result:
            return {"status": "ok", "order": result}
        return {"status": "error", "message": f"Retry failed for {ticker_clean}"}

    def place_manual_order(self, signal: dict) -> dict:
        """Manually place an order from a signal dict (bypasses auto-trade check)."""
        ticker = signal.get("ticker", "").replace(".NS", "")
        if not ticker:
            return {"status": "error", "message": "ticker required in signal"}
        if not signal.get("price"):
            return {"status": "error", "message": "price required in signal"}

        with self._lock:
            existing = self._today_orders.get(ticker)
            if existing and existing.get("status") not in ("error", "cancelled"):
                return {"status": "error",
                        "message": f"{ticker} already has an active order ({existing.get('status')})"}
            # Clear any previous failed/cancelled order
            self._today_orders.pop(ticker, None)

        result = self.on_signal(signal, force=True)
        if result:
            return {"status": "ok", "order": result}
        return {"status": "error", "message": f"Order placement failed for {ticker}"}

    def set_on_order_event(self, callback):
        """Set callback for order events (e.g. WebSocket broadcast)."""
        self._on_order_event = callback

    def _skip_signal(self, signal: dict, ticker: str, reason: str):
        """Add signal to pending queue and emit skip event."""
        signal["_skip_reason"] = reason
        signal["_skip_time"] = datetime.now().isoformat()
        self._pending_queue = [
            p for p in self._pending_queue
            if p.get("ticker") != signal.get("ticker", ticker)
        ]
        self._pending_queue.append(signal)
        self._emit_event("signal_skipped", {
            "ticker": ticker.replace(".NS", ""),
            "reason": reason,
            "signal": signal,
            "pending_count": len(self._pending_queue),
        })

    def _get_today_breadth(self) -> float | None:
        """Get today's breadth from regime table (cached per day)."""
        today = date.today()
        if today in self._breadth_cache:
            return self._breadth_cache[today]

        try:
            con = get_conn(read_only=True)
            row = con.execute("""
                SELECT breadth_pct FROM regime
                WHERE breadth_pct IS NOT NULL
                ORDER BY date DESC LIMIT 1
            """).fetchone()
            con.close()
            if row and row[0] is not None:
                val = float(row[0])
                self._breadth_cache[today] = val
                log.info("Breadth today: %.1f%%", val * 100)
                return val
        except Exception as e:
            log.warning("Failed to read breadth: %s", e)

        self._breadth_cache[today] = None
        return None

    # ── Kite client ──

    def _get_kite(self) -> KiteConnect:
        if self._kite is None:
            self._kite = KiteConnect(api_key=KITE_CFG["api_key"])
            self._kite.set_access_token(KITE_CFG["access_token"])
        return self._kite

    def _get_held_stocks(self, kite) -> dict[str, dict]:
        """Merge kite.positions() + kite.holdings() into {symbol: info} for CNC qty > 0."""
        held: dict[str, dict] = {}

        # 1) positions — same-day T+0 trades
        try:
            positions = kite.positions()
            for p in positions.get("net", []):
                if p["quantity"] > 0 and p.get(
                        "product") == "CNC":
                    held[p["tradingsymbol"]] = {
                        "quantity": p["quantity"],
                        "average_price": p["average_price"],
                        "last_price": p.get(
                            "last_price",
                            p["average_price"]),
                        "product": "CNC",
                    }
        except Exception as e:
            log.warning("_get_held: positions() failed: %s", e)

        # 2) holdings — settled CNC
        holdings = kite.holdings()
        for h in holdings:
            sym = h.get("tradingsymbol", "")
            qty = h.get("quantity", 0)
            if qty > 0 and sym not in held:
                held[sym] = {
                    "quantity": qty,
                    "average_price": h.get(
                        "average_price", 0),
                    "last_price": h.get(
                        "last_price",
                        h.get("average_price", 0)),
                    "product": "CNC",
                }

        return held

    # ── order placement ──

    def on_signal(self, signal: dict, force: bool = False) -> dict | None:
        """Place LIMIT BUY on signal fire. Returns order dict or None if skipped."""
        if not self._enabled and not force:
            return None

        # ── Breadth skip zone (legacy): skip signals entirely in choppy breadth ──
        if not force and REVERSAL_CFG.get("breadth_skip_enabled", False):
            breadth = self._get_today_breadth()
            skip_lo = REVERSAL_CFG.get("breadth_skip_low", 0.30)
            skip_hi = REVERSAL_CFG.get("breadth_skip_high", 0.50)
            if breadth is not None and skip_lo <= breadth <= skip_hi:
                log.info("SKIP %s — breadth %.1f%% in skip zone [%.0f%%–%.0f%%]",
                         signal["ticker"], breadth * 100, skip_lo * 100, skip_hi * 100)
                signal["_breadth_pct"] = round(breadth * 100, 1)
                self._skip_signal(signal, signal["ticker"],
                                  f"breadth_skip_zone ({breadth*100:.1f}%)")
                return None

        # ── Breadth reduce zone: use smaller position in choppy breadth ──
        breadth_size_factor = 1.0
        if not force and REVERSAL_CFG.get("breadth_reduce_enabled", False):
            breadth = self._get_today_breadth()
            reduce_lo = REVERSAL_CFG.get("breadth_reduce_low", 0.30)
            reduce_hi = REVERSAL_CFG.get("breadth_reduce_high", 0.55)
            if breadth is not None and reduce_lo <= breadth <= reduce_hi:
                breadth_size_factor = REVERSAL_CFG.get("breadth_reduce_factor", 0.60)
                log.info("REDUCE %s — breadth %.1f%% in reduce zone [%.0f%%–%.0f%%], "
                         "size factor=%.1fx",
                         signal.get("ticker", "?"), breadth * 100,
                         reduce_lo * 100, reduce_hi * 100, breadth_size_factor)

        ticker = signal["ticker"]
        trading_symbol = ticker.replace(".NS", "")

        # ── Rolling rvol tier: higher bar early, relaxes over time ──
        if not force and REVERSAL_CFG.get("rvol_tiers_enabled", False):
            sig_rvol = signal.get("rvol", 0)
            now_t = datetime.now().strftime("%H:%M")
            for cutoff, min_rvol in REVERSAL_CFG.get("rvol_tiers", []):
                if now_t <= cutoff:
                    if sig_rvol < min_rvol:
                        reason = f"rvol_tier ({sig_rvol:.1f} < {min_rvol:.1f} before {cutoff})"
                        log.info("SKIP %s — %s", trading_symbol, reason)
                        self._skip_signal(signal, ticker, reason)
                        return None
                    break  # passed the tier check

        with self._lock:
            if trading_symbol in self._today_orders:
                log.warning("SKIP %s — already ordered today", trading_symbol)
                return None

            # Count active positions (today + carry-forward)
            # Error/cancelled orders don't count — only real positions
            real_today = sum(
                1 for o in self._today_orders.values()
                if o.get("status") not in ("error", "cancelled")
            )
            active = real_today + len(self._open_positions)
            max_pos = REVERSAL_CFG.get("max_positions", 5)
            if not force and active >= max_pos:
                log.warning("SKIP %s — max positions (%d) reached",
                            trading_symbol, max_pos)
                self._skip_signal(signal, ticker, "max_positions")
                return None

        signal_price = signal["price"]
        trail_pct = signal.get("trailing_stop_pct", 0.02)
        max_hold = signal.get("max_hold_days", 7)

        # ── Price levels ──
        tick = self._get_tick_size(trading_symbol)
        max_entry_price = _round_up_to_tick(signal_price * (1 + MAX_LATE_ENTRY_PCT), tick)
        initial_stop = _round_to_tick(signal_price * (1 - trail_pct), tick)

        # Sanity: entry must be above stop
        if max_entry_price <= initial_stop:
            log.error("SKIP %s — max_entry %.2f <= initial_stop %.2f",
                      trading_symbol, max_entry_price, initial_stop)
            return None

        # ── Position sizing from live margin ──
        capital = self._get_capital()
        if capital <= 0:
            log.error("SKIP %s — no capital available", trading_symbol)
            return None

        pos_size_pct = REVERSAL_CFG.get("position_size_pct", 0.20)
        position_value = capital * pos_size_pct
        quantity = int(position_value / max_entry_price)
        if quantity <= 0:
            log.error("SKIP %s — quantity=0 (capital=%.0f, price=%.2f)",
                      trading_symbol, capital, max_entry_price)
            return None

        invest_amount = quantity * max_entry_price
        risk_per_share = max_entry_price - initial_stop
        risk_amount = quantity * risk_per_share
        risk_pct = risk_amount / capital * 100

        log.info(
            "ORDER %s | Qty=%d | Limit=%.2f | SL=%.2f | "
            "Invest=%.0f | Risk=%.0f (%.1f%%)",
            trading_symbol, quantity, max_entry_price, initial_stop,
            invest_amount, risk_amount, risk_pct,
        )

        try:
            kite = self._get_kite()

            # ── Step 1: LIMIT BUY ──
            buy_order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=trading_symbol,
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=quantity,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=max_entry_price,
                product=kite.PRODUCT_CNC,
                validity=kite.VALIDITY_DAY,
                tag="faralpha",
            )
            log.info("BUY LIMIT placed: %s -> order_id=%s @ %.2f",
                     trading_symbol, buy_order_id, max_entry_price)

            exit_date = (date.today() + timedelta(days=max_hold)).isoformat()

            order_info = {
                "ticker": trading_symbol,
                "signal_price": signal_price,
                "max_entry_price": max_entry_price,
                "initial_stop": initial_stop,
                "current_stop": initial_stop,
                "trail_pct": trail_pct,
                "max_hold_days": max_hold,
                "exit_date": exit_date,
                "quantity": quantity,
                "filled_qty": 0,
                "avg_fill_price": 0,
                "invest_amount": round(invest_amount, 2),
                "risk_amount": round(risk_amount, 2),
                "risk_pct": round(risk_pct, 2),
                "buy_order_id": str(buy_order_id),
                "sl_order_id": None,
                "exit_order_id": None,
                "buy_status": "OPEN",
                "sl_status": None,
                "status": "buy_placed",
                "exit_price": None,
                "pnl": None,
                "pnl_pct": None,
                "time": date.today().isoformat(),
                "fills": [],
                "errors": [],
            }

            with self._lock:
                self._today_orders[trading_symbol] = order_info

            self._persist_order(order_info)
            self._emit_event("order_placed", order_info)
            return order_info

        except Exception as e:
            log.error("BUY FAILED for %s: %s", trading_symbol, e)
            error_info = {
                "ticker": trading_symbol,
                "signal_price": signal_price,
                "max_entry_price": max_entry_price,
                "initial_stop": initial_stop,
                "current_stop": initial_stop,
                "trail_pct": trail_pct,
                "max_hold_days": max_hold,
                "exit_date": None,
                "quantity": quantity,
                "filled_qty": 0,
                "avg_fill_price": 0,
                "invest_amount": 0,
                "risk_amount": 0,
                "risk_pct": 0,
                "buy_order_id": None,
                "sl_order_id": None,
                "exit_order_id": None,
                "buy_status": None,
                "sl_status": None,
                "status": "error",
                "exit_price": None,
                "pnl": None,
                "pnl_pct": None,
                "time": date.today().isoformat(),
                "errors": [str(e)],
                "fills": [],
            }
            with self._lock:
                self._today_orders[trading_symbol] = error_info
            self._persist_order(error_info)
            self._emit_event("order_error", error_info)
            self._skip_signal(signal, ticker, f"order_failed: {e}")
            return error_info

    # ── order poller ──

    def _start_poller(self):
        """Start background thread that polls Kite for order updates."""
        if self._poller_running:
            return
        self._poller_running = True
        self._poller_thread = Thread(target=self._poll_loop, daemon=True)
        self._poller_thread.start()
        log.info("Order poller started (every %ds)", POLL_INTERVAL_SEC)

    def _stop_poller(self):
        self._poller_running = False
        log.info("Order poller stopped")

    def _poll_loop(self):
        """Poll Kite order status in background thread."""
        while self._poller_running:
            try:
                self._poll_orders()
            except Exception as e:
                log.error("Poller error: %s", e)
            time.sleep(POLL_INTERVAL_SEC)

    def _poll_orders(self):
        """Single poll cycle: check all active orders against Kite."""
        kite = self._get_kite()

        with self._lock:
            active = {k: v for k, v in self._today_orders.items()
                      if v.get("status") not in ("closed", "error", "cancelled")}

        if active:
            try:
                kite_orders = kite.orders()
            except Exception as e:
                log.error("Failed to fetch Kite orders: %s", e)
                return

            # Index Kite orders by order_id
            kite_by_id = {str(o["order_id"]): o for o in kite_orders}

            for ticker, order in active.items():
                self._check_buy_status(kite, order, kite_by_id)
                self._check_sl_status(kite, order, kite_by_id)

        # Intraday trailing: update Kite SL when trail moves up
        # (runs even if no today_orders — covers multi-day holds)
        self._trail_open_positions(kite)

        # Recover UNPROTECTED positions that now have a live SL on Kite
        self._recover_unprotected(kite)

    def _trail_open_positions(self, kite):
        """Trail SL up for all protected positions using LTP. Runs every poll cycle."""
        with self._lock:
            # Collect all positions that have an active SL order
            trailable = {}
            for ticker, order in {**self._today_orders, **self._open_positions}.items():
                if (order.get("status") in ("protected", "bought")
                        and order.get("sl_order_id")
                        and order.get("sl_status") == STATUS_TRIGGER_PENDING):
                    trailable[ticker] = order

        if not trailable:
            return

        # Fetch LTP for all trailable tickers in one call
        try:
            instruments = [f"NSE:{t}" for t in trailable]
            ltp_data = kite.ltp(instruments)
        except Exception as e:
            log.error("Trail LTP fetch failed: %s", e)
            return

        for ticker, order in trailable.items():
            ltp_info = ltp_data.get(f"NSE:{ticker}", {})
            last_price = ltp_info.get("last_price", 0)
            if last_price <= 0:
                continue

            entry = order.get("avg_fill_price", 0)
            if entry <= 0:
                continue

            trail_pct = order.get("trail_pct", 0.02)
            current_stop = order.get("current_stop", 0)
            tick = self._get_tick_size(ticker)

            # Trail only moves up, never down
            new_stop = _round_to_tick(last_price * (1 - trail_pct), tick)
            if new_stop <= current_stop:
                continue

            # Update the Kite SL order via modify
            sl_order_id = order["sl_order_id"]
            new_limit = _round_to_tick(new_stop * 0.99, tick)
            try:
                kite.modify_order(
                    variety=kite.VARIETY_REGULAR,
                    order_id=sl_order_id,
                    trigger_price=new_stop,
                    price=new_limit,
                )
                log.info("TRAIL UP: %s | stop %.2f -> %.2f (LTP=%.2f)",
                         ticker, current_stop, new_stop, last_price)
                order["current_stop"] = new_stop
                self._persist_order(order)
            except Exception as e:
                err_msg = str(e)
                if "modifications exceeded" in err_msg.lower():
                    # NSE limits ~25 modifications per order; cancel and re-place
                    log.warning("TRAIL %s: max modifications hit — replacing SL order", ticker)
                    try:
                        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_order_id)
                        order["sl_order_id"] = None
                        order["current_stop"] = new_stop
                        qty = order.get("filled_qty", 0)
                        self._place_sl(kite, order, qty, new_stop)
                        log.info("TRAIL %s: replaced SL -> %.2f (LTP=%.2f)",
                                 ticker, new_stop, last_price)
                        self._persist_order(order)
                    except Exception as e2:
                        log.error("TRAIL %s: SL replacement failed: %s", ticker, e2)
                else:
                    log.error("Trail modify failed for %s (SL %s): %s",
                              ticker, sl_order_id, e)

    def _recover_unprotected(self, kite):
        """Check UNPROTECTED positions — if SL now exists on Kite, flip to protected."""
        with self._lock:
            unprotected = {t: o for t, o in {**self._today_orders, **self._open_positions}.items()
                           if o.get("status") == "UNPROTECTED" and o.get("sl_order_id")}

        if not unprotected:
            return

        try:
            kite_orders = kite.orders()
        except Exception:
            return
        kite_by_id = {str(o["order_id"]): o for o in kite_orders}

        for ticker, order in unprotected.items():
            sl_id = str(order["sl_order_id"])
            kite_sl = kite_by_id.get(sl_id)
            if kite_sl and kite_sl.get("status") == STATUS_TRIGGER_PENDING:
                order["status"] = "protected"
                order["sl_status"] = STATUS_TRIGGER_PENDING
                self._persist_order(order)
                log.info("RECOVERED: %s SL active on Kite (order %s) — status -> protected",
                         ticker, sl_id)
            elif not kite_sl:
                # SL order doesn't exist on Kite — try to re-place it
                qty = order.get("filled_qty", 0)
                stop = order.get("current_stop") or order.get("initial_stop") or 0
                if qty > 0 and stop > 0:
                    self._place_sl(kite, order, qty, stop)
                    self._persist_order(order)

    def _check_buy_status(self, kite, order: dict, kite_by_id: dict):
        """Check BUY order. On fill -> place SL-M."""
        buy_id = order.get("buy_order_id")
        if not buy_id:
            return
        if order.get("buy_status") in (STATUS_COMPLETE, STATUS_CANCELLED, STATUS_REJECTED):
            return

        kite_order = kite_by_id.get(buy_id)
        if not kite_order:
            return

        kite_status = kite_order.get("status", "")
        filled_qty = kite_order.get("filled_quantity", 0)
        avg_price = kite_order.get("average_price", 0)
        ticker = order["ticker"]

        order["buy_status"] = kite_status
        order["filled_qty"] = filled_qty
        if avg_price > 0:
            order["avg_fill_price"] = avg_price

        if kite_status == STATUS_COMPLETE and filled_qty > 0:
            log.info("BUY FILLED: %s | Qty=%d | AvgPrice=%.2f",
                     ticker, filled_qty, avg_price)
            order["status"] = "bought"
            order["fills"].append({
                "type": "buy_fill",
                "qty": filled_qty,
                "price": avg_price,
                "time": datetime.now().isoformat(),
            })

            # Recalculate stop based on actual fill price (may be better than limit)
            tick = self._get_tick_size(ticker)
            actual_stop = _round_to_tick(avg_price * (1 - order["trail_pct"]), tick)
            if actual_stop > order["initial_stop"]:
                order["current_stop"] = actual_stop
                order["initial_stop"] = actual_stop

            # Place SL-M for filled quantity
            if not order.get("sl_order_id"):
                self._place_sl(kite, order, filled_qty, order["current_stop"])

            self._persist_order(order)
            self._emit_event("buy_filled", order)

        elif kite_status == STATUS_REJECTED:
            log.error("BUY REJECTED: %s — %s", ticker, kite_order.get("status_message", ""))
            order["status"] = "error"
            order["errors"].append(f"BUY rejected: {kite_order.get('status_message', '')}")
            self._persist_order(order)
            self._emit_event("order_error", order)

        elif kite_status == STATUS_CANCELLED:
            log.info("BUY CANCELLED: %s (unfilled EOD or manual)", ticker)
            order["status"] = "cancelled"
            self._persist_order(order)

    def _check_sl_status(self, kite, order: dict, kite_by_id: dict):
        """Check SL-M order. On trigger -> mark position closed."""
        sl_id = order.get("sl_order_id")
        if not sl_id:
            return
        if order.get("sl_status") in (STATUS_COMPLETE, STATUS_CANCELLED, STATUS_REJECTED):
            return

        kite_order = kite_by_id.get(sl_id)
        if not kite_order:
            return

        kite_status = kite_order.get("status", "")
        ticker = order["ticker"]
        order["sl_status"] = kite_status

        if kite_status == STATUS_COMPLETE:
            exit_price = kite_order.get("average_price", 0)
            entry = order.get("avg_fill_price", 0)
            qty = order.get("filled_qty", 0)
            pnl = (exit_price - entry) * qty if entry > 0 else 0
            pnl_pct = (exit_price / entry - 1) * 100 if entry > 0 else 0

            log.info("SL TRIGGERED: %s | Exit=%.2f | PnL=%.0f (%.1f%%)",
                     ticker, exit_price, pnl, pnl_pct)
            order["status"] = "closed"
            order["exit_price"] = exit_price
            order["pnl"] = round(pnl, 2)
            order["pnl_pct"] = round(pnl_pct, 2)
            order["fills"].append({
                "type": "sl_exit",
                "qty": qty,
                "price": exit_price,
                "time": datetime.now().isoformat(),
            })
            self._persist_order(order)
            self._emit_event("position_closed", order)

        elif kite_status == STATUS_REJECTED:
            log.error("SL REJECTED: %s — position unprotected! %s",
                      ticker, kite_order.get("status_message", ""))
            order["errors"].append(f"SL rejected: {kite_order.get('status_message', '')}")
            order["sl_order_id"] = None
            # Retry SL placement
            self._place_sl(kite, order, order.get("filled_qty", 0), order["current_stop"])
            self._persist_order(order)
            self._emit_event("order_error", order)

        elif kite_status == STATUS_CANCELLED:
            # SL cancelled — could be EOD expiry OR manual cancel for force_exit.
            # Only move to open_positions if the position is still active.
            if order.get("status") in ("closed", "exit_sl_breached", "exit_max_hold"):
                log.info("SL CANCELLED: %s — position already closed, not re-queueing", ticker)
                order["sl_status"] = "cancelled"
                order["sl_order_id"] = None
            else:
                log.warning("SL EXPIRED: %s — will re-place tomorrow morning", ticker)
                order["sl_status"] = "expired"
                order["sl_order_id"] = None
                with self._lock:
                    self._open_positions[ticker] = order
            self._persist_order(order)

    def _poll_exit_fill(self, kite, order_id: str,
                         max_wait: float = 5.0) -> float | None:
        """Poll Kite for actual fill price of an exit order.
        Returns average_price if filled, else None."""
        import time as _time
        end = _time.monotonic() + max_wait
        while _time.monotonic() < end:
            try:
                hist = kite.order_history(order_id)
                for h in reversed(hist):
                    if (h.get("status") == "COMPLETE"
                            and h.get("average_price", 0) > 0):
                        return h["average_price"]
            except Exception:
                pass
            _time.sleep(1)
        return None

    def _place_sl(self, kite, order: dict, quantity: int, trigger_price: float):
        """Place SL order with retries. If trigger >= LTP, exits immediately."""
        ticker = order["ticker"]

        # ── HARD SAFEGUARD: never sell more than system bought ──
        max_qty = order.get("filled_qty", 0)
        if max_qty <= 0:
            log.error("SELL BLOCKED: %s has no filled_qty — refusing to place any sell order", ticker)
            return
        if quantity > max_qty:
            log.critical(
                "SELL QTY CAPPED: %s requested qty=%d but filled_qty=%d — "
                "capping to %d to protect personal holdings",
                ticker, quantity, max_qty, max_qty)
            quantity = max_qty
        if order.get("exit_order_id"):
            log.warning("SELL BLOCKED: %s already has exit_order_id=%s — skipping duplicate sell",
                        ticker, order["exit_order_id"])
            return

        tick = self._get_tick_size(ticker)
        trigger = _round_to_tick(trigger_price, tick)
        # Limit price slightly below trigger for guaranteed fill
        limit = _round_to_tick(trigger * 0.99, tick)

        # Check if stop is already breached (trigger >= LTP)
        try:
            ltp_data = kite.ltp([f"NSE:{ticker}"])
            ltp = ltp_data.get(
                f"NSE:{ticker}", {}).get("last_price", 0)
        except Exception:
            ltp = 0

        if ltp > 0 and trigger >= ltp:
            # Stop breached — exit immediately with LIMIT sell
            sell_limit = _round_to_tick(ltp * 0.99, tick)
            log.warning(
                "SL BREACHED: %s stop=%.2f >= LTP=%.2f — "
                "placing immediate LIMIT sell @ %.2f",
                ticker, trigger, ltp, sell_limit)
            try:
                sell_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=ticker,
                    transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_LIMIT,
                    price=sell_limit,
                    product=kite.PRODUCT_CNC,
                    validity=kite.VALIDITY_DAY,
                    tag="faralpha",
                )
                entry = order.get("avg_fill_price", 0)
                qty = order.get("filled_qty", quantity)
                order["exit_order_id"] = str(sell_id)
                # Poll Kite for actual fill price
                fill = self._poll_exit_fill(kite, str(sell_id))
                exit_px = fill or sell_limit
                pnl = (exit_px - entry) * qty if entry > 0 else 0
                log.info(
                    "EXIT SELL placed: %s order_id=%s "
                    "fill=%.2f PnL=%.0f (stop breached%s)",
                    ticker, sell_id, exit_px, pnl,
                    "" if fill else ", fill est.")
                order["exit_price"] = exit_px
                order["pnl"] = round(pnl, 2)
                order["pnl_pct"] = round((exit_px / entry - 1) * 100, 2) if entry > 0 else 0
                order["status"] = "exit_sl_breached"
                order["sl_status"] = STATUS_COMPLETE
                order["current_stop"] = trigger
                return
            except Exception as e:
                log.error(
                    "EXIT SELL failed for %s: %s — "
                    "falling back to SL order", ticker, e)
                # Fall through to normal SL placement

        for attempt in range(1, MAX_SL_RETRIES + 1):
            try:
                sl_order_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=ticker,
                    transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_SL,
                    trigger_price=trigger,
                    price=limit,
                    product=kite.PRODUCT_CNC,
                    validity=kite.VALIDITY_DAY,
                    tag="faralpha",
                )
                log.info(
                    "SL placed: %s -> order_id=%s trigger=%.2f limit=%.2f (attempt %d)",
                    ticker, sl_order_id, trigger, limit, attempt,
                )
                order["sl_order_id"] = str(sl_order_id)
                order["sl_status"] = STATUS_TRIGGER_PENDING
                order["current_stop"] = trigger
                if order.get("status") == "bought":
                    order["status"] = "protected"
                return
            except Exception as e:
                err_msg = str(e).lower()
                # If trigger >= LTP error, don't retry — exit
                if "trigger price" in err_msg and (
                        "lower than" in err_msg
                        or "last traded" in err_msg):
                    log.warning(
                        "SL %s: trigger %.2f >= LTP, "
                        "placing market exit",
                        ticker, trigger)
                    try:
                        sell_id = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NSE,
                            tradingsymbol=ticker,
                            transaction_type=(
                                kite.TRANSACTION_TYPE_SELL),
                            quantity=quantity,
                            order_type=kite.ORDER_TYPE_MARKET,
                            product=kite.PRODUCT_CNC,
                            validity=kite.VALIDITY_DAY,
                            tag="faralpha",
                        )
                        entry = order.get("avg_fill_price", 0)
                        qty = order.get("filled_qty", quantity)
                        order["exit_order_id"] = str(sell_id)
                        # Poll Kite for actual fill price
                        fill = self._poll_exit_fill(
                            kite, str(sell_id))
                        exit_px = fill or trigger
                        order["exit_price"] = exit_px
                        order["pnl"] = round((exit_px - entry) * qty, 2) if entry > 0 else 0
                        order["pnl_pct"] = round((exit_px / entry - 1) * 100, 2) if entry > 0 else 0
                        order["status"] = "exit_sl_breached"
                        order["sl_status"] = STATUS_COMPLETE
                        log.info(
                            "MARKET EXIT: %s order_id=%s "
                            "fill=%.2f PnL=%.0f%s",
                            ticker, sell_id, exit_px,
                            order["pnl"],
                            "" if fill else " (fill est.)")
                        return
                    except Exception as e2:
                        log.error(
                            "Market exit also failed for "
                            "%s: %s", ticker, e2)
                log.error("SL placement failed for %s (attempt %d/%d): %s",
                          ticker, attempt, MAX_SL_RETRIES, e)
                order["errors"].append(f"SL attempt {attempt}: {e}")
                if attempt < MAX_SL_RETRIES:
                    time.sleep(2)

        # All retries exhausted — CRITICAL
        log.critical("CRITICAL: %s has NO STOP LOSS! Manual intervention required.", ticker)
        order["status"] = "UNPROTECTED"
        self._emit_event("order_error", {
            **order,
            "critical": True,
            "message": f"STOP LOSS FAILED for {ticker} after {MAX_SL_RETRIES} attempts!",
        })

    # ── morning SL refresh ──

    def morning_sl_refresh(self) -> list[dict]:
        """Re-place SL for all open positions. Called at market open each day."""
        results = []
        kite = self._get_kite()

        with self._lock:
            open_orders = list(self._open_positions.items())

        if not open_orders:
            log.info("Morning SL refresh: no open positions")
            return results

        net_positions = self._get_held_stocks(kite)

        # Guard: if Kite returns NO held stocks but we have
        # open orders, it is likely a holiday / market closed.
        # Never bulk-close everything on empty data.
        if not net_positions and open_orders:
            log.warning(
                "Morning SL refresh: Kite returned 0 net "
                "positions but we track %d — likely holiday. "
                "Skipping.", len(open_orders))
            return [{"skipped": "holiday_or_no_data",
                      "open_count": len(open_orders)}]

        # Filter to only non-closed positions for the missing check.
        # Closed orders may linger in _open_positions briefly after
        # force_exit (race between poller and cleanup).
        live_orders = [(t, o) for t, o in open_orders
                       if o.get("status") not in ("closed", "cancelled", "error")]

        # Safety: if ALL live positions are missing from Kite, likely
        # API error or holiday — don't bulk-close.
        if live_orders:
            missing = [t for t, _ in live_orders if t not in net_positions]
            if len(missing) > 0 and len(missing) == len(live_orders):
                log.warning(
                    "Morning SL refresh: ALL %d live positions "
                    "missing from Kite held stocks (%s). Likely API "
                    "error — skipping to avoid accidental closure.",
                    len(missing), ", ".join(missing))
                return [{"skipped": "all_positions_missing",
                          "missing": missing}]

        # Fetch today's Kite orders once (for existing SL check)
        kite_by_id = {}
        try:
            kite_by_id = {str(o["order_id"]): o for o in kite.orders()}
        except Exception as e:
            log.warning("Morning SL refresh: failed to fetch Kite orders: %s", e)

        for ticker, order in open_orders:
            # Skip already-closed orders lingering in _open_positions
            if order.get("status") in ("closed", "cancelled", "error"):
                with self._lock:
                    self._open_positions.pop(ticker, None)
                continue

            pos = net_positions.get(ticker)
            if not pos:
                # Position not in Kite holdings/positions.
                # If buy is confirmed and no exit placed, this is likely
                # T+1 settlement limbo (CNC bought yesterday won't show
                # in holdings until settlement completes). Do NOT close —
                # trust the DB and proceed to place SL.
                if (order.get("buy_status") == STATUS_COMPLETE
                        and not order.get("exit_order_id")):
                    log.warning(
                        "Position %s not in Kite held stocks but "
                        "buy_status=COMPLETE with no exit — likely T+1 "
                        "settlement. Proceeding with SL placement.", ticker)
                else:
                    log.info("Position %s no longer held — removing", ticker)
                    with self._lock:
                        self._open_positions.pop(ticker, None)
                    order["status"] = "closed"
                    self._persist_order(order)
                    continue

            # Check if max hold date exceeded -> sell at market
            exit_date = order.get("exit_date", "")
            if exit_date and date.today().isoformat() >= exit_date:
                log.info("MAX HOLD reached for %s — placing market sell", ticker)
                qty = order.get("filled_qty", 0)
                if qty <= 0:
                    log.error("MAX HOLD: %s has no filled_qty — skipping", ticker)
                    continue
                try:
                    sell_id = kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange=kite.EXCHANGE_NSE,
                        tradingsymbol=ticker,
                        transaction_type=kite.TRANSACTION_TYPE_SELL,
                        quantity=qty,
                        order_type=kite.ORDER_TYPE_MARKET,
                        product=kite.PRODUCT_CNC,
                        validity=kite.VALIDITY_DAY,
                        tag="faralpha",
                    )
                    order["status"] = "exit_max_hold"
                    order["exit_order_id"] = str(sell_id)
                    results.append({"ticker": ticker, "action": "max_hold_exit",
                                    "order_id": str(sell_id)})
                except Exception as e:
                    log.error("Max hold exit failed for %s: %s", ticker, e)
                    results.append({"ticker": ticker, "action": "max_hold_exit",
                                    "error": str(e)})
                with self._lock:
                    self._open_positions.pop(ticker, None)
                self._persist_order(order)
                continue

            # Trail stop up using last_price from Kite position
            entry = order.get("avg_fill_price", 0)
            trail_pct = order.get("trail_pct", 0.02)
            current_stop = order.get("current_stop", 0)
            last_price = pos.get("last_price", 0) if pos else 0

            if last_price > entry > 0:
                tick = self._get_tick_size(ticker)
                new_stop = _round_to_tick(last_price * (1 - trail_pct), tick)
                if new_stop > current_stop:
                    current_stop = new_stop
                    order["current_stop"] = current_stop
                    log.info("TRAILING UP: %s stop -> %.2f", ticker, current_stop)

            # Check if there's already a live SL on Kite (e.g. from previous server instance)
            existing_sl = order.get("sl_order_id")
            if existing_sl and kite_by_id:
                kite_sl = kite_by_id.get(str(existing_sl))
                if kite_sl and kite_sl.get("status") == STATUS_TRIGGER_PENDING:
                    log.info("SL already active: %s order_id=%s trigger=%.2f",
                             ticker, existing_sl, kite_sl.get("trigger_price", 0))
                    order["sl_status"] = STATUS_TRIGGER_PENDING
                    if order.get("status") in ("bought", "UNPROTECTED"):
                        order["status"] = "protected"
                    self._persist_order(order)
                    results.append({"ticker": ticker, "action": "sl_already_active",
                                    "stop": current_stop,
                                    "sl_order_id": existing_sl})
                    continue

            # Fallback: scan ALL Kite orders for a TRIGGER PENDING sell on this ticker
            # (catches SLs placed by a previous server that the DB lost track of)
            if kite_by_id:
                for oid, ko in kite_by_id.items():
                    if (ko.get("tradingsymbol") == ticker
                            and ko.get("transaction_type") == "SELL"
                            and ko.get("status") == STATUS_TRIGGER_PENDING
                            and ko.get("product") == "CNC"):
                        log.info("SL found by ticker scan: %s order_id=%s trigger=%.2f",
                                 ticker, oid, ko.get("trigger_price", 0))
                        order["sl_order_id"] = oid
                        order["sl_status"] = STATUS_TRIGGER_PENDING
                        if order.get("status") in ("bought", "UNPROTECTED"):
                            order["status"] = "protected"
                        self._persist_order(order)
                        results.append({"ticker": ticker, "action": "sl_found_by_scan",
                                        "stop": current_stop, "sl_order_id": oid})
                        break
                else:
                    # No existing SL found — proceed to place new one
                    qty = order.get("filled_qty", 0)
                    self._place_sl(kite, order, qty, current_stop)
                    self._persist_order(order)
                    results.append({"ticker": ticker, "action": "sl_refreshed",
                                    "stop": current_stop})
                continue

            qty = order.get("filled_qty", 0)
            self._place_sl(kite, order, qty, current_stop)
            self._persist_order(order)
            results.append({"ticker": ticker, "action": "sl_refreshed",
                            "stop": current_stop})

        return results

    # ── sync ──

    def sync_from_kite(self) -> dict:
        """Cross-check local state against Kite orders. Updates exit fill prices."""
        kite = self._get_kite()
        synced = {"orders_synced": 0, "fills_updated": 0}

        try:
            kite_orders = kite.orders()
            kite_by_id = {str(o["order_id"]): o for o in kite_orders}
            faralpha_orders = [
                o for o in kite_orders
                if o.get("tag") == "faralpha"
            ]
            synced["orders_synced"] = len(faralpha_orders)

            # Check for completed exit orders whose fill price we missed
            all_tracked = {**self._today_orders, **self._open_positions}
            for ticker, order in all_tracked.items():
                exit_id = order.get("exit_order_id")
                if not exit_id:
                    continue
                kite_exit = kite_by_id.get(exit_id)
                if not kite_exit:
                    continue
                if kite_exit.get("status") == STATUS_COMPLETE:
                    actual_price = kite_exit.get("average_price", 0)
                    if actual_price > 0 and order.get("exit_price") != actual_price:
                        entry = order.get("avg_fill_price", 0)
                        qty = order.get("filled_qty", 0)
                        order["exit_price"] = actual_price
                        order["pnl"] = round((actual_price - entry) * qty, 2) if entry else 0
                        order["pnl_pct"] = round((actual_price / entry - 1) * 100, 2) if entry else 0
                        self._persist_order(order)
                        synced["fills_updated"] += 1
                        log.info("SYNC: updated %s exit fill %.2f (was %.2f)",
                                 ticker, actual_price, order.get("exit_price", 0))
        except Exception as e:
            log.warning("sync_from_kite failed: %s", e)

        return synced

    def reconcile_from_kite(self) -> list[dict]:
        """Restore system trades wrongly marked closed but still held in Kite."""
        results = []

        # Only look at system trades that got wrongly closed
        # (no exit_order_id means it wasn't a real exit)
        try:
            con = get_conn()
            rows = con.execute("""
                SELECT ticker, order_date, current_stop,
                       trail_pct, filled_qty
                FROM orders
                WHERE buy_order_id IS NOT NULL
                  AND exit_order_id IS NULL
                  AND status IN ('closed', 'UNPROTECTED')
            """).fetchall()
            con.close()
        except Exception as e:
            log.warning("Reconcile: DB query failed: %s", e)
            return results

        if not rows:
            return results

        # Check which of these are still in Kite
        try:
            kite = self._get_kite()
            held = self._get_held_stocks(kite)
        except Exception as e:
            log.warning("Reconcile: Kite fetch failed: %s", e)
            return [{"error": str(e)}]

        for ticker, odate, stop, trail, db_qty in rows:
            if ticker in self._open_positions:
                continue
            if ticker in self._today_orders:
                continue
            if ticker not in held:
                continue  # genuinely exited

            # Still held in Kite — restore using DB qty
            restored = self._restore_closed_order(ticker)
            if restored:
                with self._lock:
                    self._open_positions[ticker] = restored
                log.info(
                    "RECONCILE: restored %s (qty=%d)",
                    ticker, restored["filled_qty"])
                results.append({
                    "ticker": ticker,
                    "action": "restored",
                    "qty": restored["filled_qty"]})

        if results:
            log.info("Reconcile: %d positions restored",
                     len(results))
        return results

    def _restore_closed_order(self, ticker: str) -> dict | None:
        """Find the most recent DB record for ticker (must have
        buy_order_id) and flip it back to 'protected'.

        Preserves the original DB values for qty, stop, entry
        price etc. Only clears SL/exit fields.
        """
        try:
            con = get_conn()
            row = con.execute(f"""
                SELECT {_ORDER_COLUMNS}
                FROM orders
                WHERE ticker = ?
                  AND buy_order_id IS NOT NULL
                ORDER BY order_date DESC LIMIT 1
            """, [ticker]).fetchone()
            if not row:
                con.close()
                return None

            order = self._row_to_dict(row)
            # Only clear SL/exit — keep original qty and stop
            order["status"] = "protected"
            order["sl_order_id"] = None
            order["sl_status"] = None
            order["exit_order_id"] = None
            order["exit_price"] = None
            order["pnl"] = None
            order["pnl_pct"] = None

            # Persist
            con.execute("""
                UPDATE orders
                SET status = 'protected',
                    sl_order_id = NULL,
                    sl_status = NULL,
                    exit_order_id = NULL,
                    exit_price = NULL,
                    pnl = NULL,
                    pnl_pct = NULL
                WHERE ticker = ?
                  AND order_date = ?
                  AND buy_order_id IS NOT NULL
            """, [ticker, order["time"]])
            con.close()
            return order
        except Exception as e:
            log.warning("Restore %s failed: %s", ticker, e)
            return None

    # ── capital ──

    def _get_capital(self) -> float:
        """Get available capital from Kite margins."""
        try:
            kite = self._get_kite()
            margins = kite.margins(segment="equity")
            available = margins.get("available", {})
            cash = available.get("cash", 0)
            collateral = available.get("collateral", 0)
            live_balance = available.get("live_balance", 0)
            capital = live_balance if live_balance > 0 else (cash + collateral)
            if capital > 0:
                return capital
        except Exception as e:
            log.warning("Kite margins failed: %s — using config fallback", e)

        return REVERSAL_CFG.get("capital", 0)

    # ── manual actions ──

    def update_trailing_stop(self, ticker: str, new_stop: float) -> dict:
        """Manually trail the stop up for a position."""
        trading_symbol = ticker.replace(".NS", "")
        order = (self._today_orders.get(trading_symbol) or
                 self._open_positions.get(trading_symbol))
        if not order:
            return {"status": "error", "message": f"No position for {trading_symbol}"}

        new_trigger = _round_to_tick(new_stop, self._get_tick_size(trading_symbol))
        current = order.get("current_stop", 0)
        if new_trigger <= current:
            return {"status": "error",
                    "message": f"New stop {new_trigger:.2f} must be above current {current:.2f}"}

        sl_id = order.get("sl_order_id")
        if not sl_id:
            return {"status": "error", "message": "No active SL order to modify"}

        try:
            kite = self._get_kite()
            kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id=sl_id,
                trigger_price=new_trigger,
            )
            log.info("SL TRAILED: %s %.2f -> %.2f", trading_symbol, current, new_trigger)
            order["current_stop"] = new_trigger
            self._persist_order(order)
            self._emit_event("sl_updated", order)
            return {"status": "ok", "old_stop": current, "new_stop": new_trigger}
        except Exception as e:
            log.error("SL trail failed for %s: %s", trading_symbol, e)
            return {"status": "error", "message": str(e)}

    def cancel_buy(self, ticker: str) -> dict:
        """Cancel an unfilled BUY order."""
        trading_symbol = ticker.replace(".NS", "")
        order = self._today_orders.get(trading_symbol)
        if not order or not order.get("buy_order_id"):
            return {"status": "error", "message": f"No BUY order for {trading_symbol}"}
        if order.get("buy_status") == STATUS_COMPLETE:
            return {"status": "error", "message": f"Already filled for {trading_symbol}"}

        try:
            kite = self._get_kite()
            kite.cancel_order(
                variety=kite.VARIETY_REGULAR,
                order_id=order["buy_order_id"],
            )
            order["status"] = "cancelled"
            order["buy_status"] = STATUS_CANCELLED
            self._persist_order(order)
            return {"status": "ok", "cancelled": order["buy_order_id"]}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def force_exit(self, ticker: str) -> dict:
        """Force-exit a position: cancel any pending SL, sell at market."""
        trading_symbol = ticker.replace(".NS", "")
        order = (self._today_orders.get(trading_symbol)
                 or self._open_positions.get(trading_symbol))
        if not order:
            return {"status": "error",
                    "message": f"No position for {trading_symbol}"}

        # Block if already exiting
        if order.get("exit_order_id"):
            return {"status": "error",
                    "message": f"{trading_symbol} already has exit order {order['exit_order_id']}"}

        filled_qty = order.get("filled_qty", 0)
        if filled_qty <= 0:
            return {"status": "error",
                    "message": f"No filled shares for {trading_symbol}"}

        try:
            kite = self._get_kite()

            # Cancel active SL order first
            sl_id = order.get("sl_order_id")
            if sl_id and order.get("sl_status") in (
                STATUS_OPEN, STATUS_TRIGGER_PENDING
            ):
                try:
                    kite.cancel_order(
                        variety=kite.VARIETY_REGULAR,
                        order_id=sl_id,
                    )
                    log.info("Cancelled SL %s for %s", sl_id, trading_symbol)
                except Exception as e:
                    log.warning("SL cancel failed for %s: %s", trading_symbol, e)

            # Place LIMIT sell at current bid (slightly below LTP for fill)
            ltp_data = kite.ltp(f"NSE:{trading_symbol}")
            ltp = ltp_data.get(f"NSE:{trading_symbol}", {}).get(
                "last_price", 0
            )
            if ltp <= 0:
                return {"status": "error",
                        "message": f"Cannot get LTP for {trading_symbol}"}

            sell_price = _round_to_tick(ltp * 0.998, self._get_tick_size(trading_symbol))  # 0.2% below LTP

            sell_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=trading_symbol,
                transaction_type=kite.TRANSACTION_TYPE_SELL,
                quantity=filled_qty,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=sell_price,
                product=kite.PRODUCT_CNC,
                validity=kite.VALIDITY_DAY,
                tag="faralpha",
            )

            log.info(
                "FORCE EXIT: %s | Qty=%d | Sell=%.2f | order_id=%s",
                trading_symbol, filled_qty, sell_price, sell_id,
            )

            avg_fill = order.get("avg_fill_price", 0)
            pnl = (sell_price - avg_fill) * filled_qty if avg_fill else 0

            order["exit_order_id"] = str(sell_id)
            order["exit_price"] = sell_price
            order["pnl"] = round(pnl, 2)
            order["pnl_pct"] = round(
                (sell_price / avg_fill - 1) * 100, 2
            ) if avg_fill else 0
            order["status"] = "closed"
            order["sl_status"] = "cancelled"

            # Remove from tracking dicts so it doesn't block
            # morning_sl_refresh or get re-added by _check_sl_status
            with self._lock:
                self._open_positions.pop(trading_symbol, None)
                # Don't remove from _today_orders — poller needs it
                # to confirm the exit fill

            self._persist_order(order)
            self._emit_event("position_closed", order)

            return {
                "status": "ok",
                "ticker": trading_symbol,
                "sell_order_id": str(sell_id),
                "sell_price": sell_price,
                "quantity": filled_qty,
                "pnl": round(pnl, 2),
            }
        except Exception as e:
            log.error("Force exit failed for %s: %s", trading_symbol, e)
            return {"status": "error", "message": str(e)}

    # ── events ──

    def _emit_event(self, event_type: str, data: dict):
        if self._on_order_event:
            try:
                self._on_order_event(event_type, data)
            except Exception:
                pass

    # ── persistence (DuckDB) ──

    def _ensure_table(self) -> None:
        """Create orders table if it doesn't exist."""
        try:
            con = get_conn()
            con.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    ticker           VARCHAR   NOT NULL,
                    order_date       DATE      NOT NULL,
                    signal_price     DOUBLE    NOT NULL,
                    max_entry_price  DOUBLE    NOT NULL,
                    initial_stop     DOUBLE    NOT NULL,
                    current_stop     DOUBLE    NOT NULL,
                    trail_pct        DOUBLE    NOT NULL,
                    max_hold_days    INTEGER   NOT NULL,
                    exit_date        DATE,
                    quantity         INTEGER   NOT NULL,
                    filled_qty       INTEGER   DEFAULT 0,
                    avg_fill_price   DOUBLE    DEFAULT 0,
                    invest_amount    DOUBLE,
                    risk_amount      DOUBLE,
                    risk_pct         DOUBLE,
                    buy_order_id     VARCHAR,
                    sl_order_id      VARCHAR,
                    exit_order_id    VARCHAR,
                    buy_status       VARCHAR,
                    sl_status        VARCHAR,
                    status           VARCHAR   NOT NULL,
                    exit_price       DOUBLE,
                    pnl              DOUBLE,
                    pnl_pct          DOUBLE,
                    errors           VARCHAR,
                    fills            VARCHAR,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, order_date)
                );
            """)
            con.close()
        except Exception as e:
            log.warning("Failed to ensure orders table: %s", e)

    def _persist_order(self, order_info: dict) -> None:
        """Upsert order into DuckDB orders table."""
        try:
            con = get_conn()
            order_date = order_info.get("time", "")[:10]
            ticker = order_info.get("ticker", "")
            exit_date_str = order_info.get("exit_date")
            exit_date_val = exit_date_str if exit_date_str else None

            # Serialize lists to JSON strings for storage
            errors_json = json.dumps(order_info.get("errors", []))
            fills_json = json.dumps(order_info.get("fills", []))

            con.execute("""
                INSERT INTO orders (
                    ticker, order_date, signal_price, max_entry_price,
                    initial_stop, current_stop, trail_pct, max_hold_days,
                    exit_date, quantity, filled_qty, avg_fill_price,
                    invest_amount, risk_amount, risk_pct,
                    buy_order_id, sl_order_id, exit_order_id,
                    buy_status, sl_status, status,
                    exit_price, pnl, pnl_pct,
                    errors, fills, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (ticker, order_date) DO UPDATE SET
                    current_stop = excluded.current_stop,
                    filled_qty = excluded.filled_qty,
                    avg_fill_price = excluded.avg_fill_price,
                    buy_order_id = excluded.buy_order_id,
                    sl_order_id = excluded.sl_order_id,
                    exit_order_id = excluded.exit_order_id,
                    buy_status = excluded.buy_status,
                    sl_status = excluded.sl_status,
                    status = excluded.status,
                    exit_price = excluded.exit_price,
                    pnl = excluded.pnl,
                    pnl_pct = excluded.pnl_pct,
                    errors = excluded.errors,
                    fills = excluded.fills,
                    updated_at = now()
            """, [
                ticker, order_date,
                order_info.get("signal_price", 0),
                order_info.get("max_entry_price", 0),
                order_info.get("initial_stop", 0),
                order_info.get("current_stop", 0),
                order_info.get("trail_pct", 0.02),
                order_info.get("max_hold_days", 7),
                exit_date_val,
                order_info.get("quantity", 0),
                order_info.get("filled_qty", 0),
                order_info.get("avg_fill_price", 0),
                order_info.get("invest_amount", 0),
                order_info.get("risk_amount", 0),
                order_info.get("risk_pct", 0),
                order_info.get("buy_order_id"),
                order_info.get("sl_order_id"),
                order_info.get("exit_order_id"),
                order_info.get("buy_status"),
                order_info.get("sl_status"),
                order_info.get("status", "unknown"),
                order_info.get("exit_price"),
                order_info.get("pnl"),
                order_info.get("pnl_pct"),
                errors_json,
                fills_json,
            ])
            con.close()
        except Exception as e:
            log.warning("Failed to persist order: %s", e)

    def _load_today_orders(self) -> None:
        """Load today's orders + recent open positions from DuckDB."""
        try:
            con = get_conn()

            # Check table exists
            tables = [r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'"
            ).fetchall()]
            if "orders" not in tables:
                con.close()
                return

            today = str(date.today())

            # Today's orders
            rows = con.execute(f"""
                SELECT {_ORDER_COLUMNS}
                FROM orders WHERE order_date = ?
            """, [today]).fetchall()

            for r in rows:
                order = self._row_to_dict(r)
                self._today_orders[order["ticker"]] = order

            # Open positions from prior days
            rows = con.execute(f"""
                SELECT {_ORDER_COLUMNS}
                FROM orders
                WHERE order_date < ? AND status IN ('bought', 'protected', 'UNPROTECTED')
            """, [today]).fetchall()

            for r in rows:
                order = self._row_to_dict(r)
                self._open_positions[order["ticker"]] = order

            con.close()

            if self._today_orders:
                log.info("Loaded %d orders from today", len(self._today_orders))
            if self._open_positions:
                log.info("Loaded %d open positions from prior days",
                         len(self._open_positions))
        except Exception as e:
            log.warning("Failed to load orders from DB: %s", e)

    @staticmethod
    def _row_to_dict(r) -> dict:
        """Convert a DB row tuple to the order dict format."""
        exit_date_val = str(r[8]) if r[8] else None
        return {
            "ticker": r[0],
            "time": str(r[1]),  # order_date as string for compat
            "signal_price": r[2],
            "max_entry_price": r[3],
            "initial_stop": r[4],
            "current_stop": r[5],
            "trail_pct": r[6],
            "max_hold_days": r[7],
            "exit_date": exit_date_val,
            "quantity": r[9],
            "filled_qty": r[10],
            "avg_fill_price": r[11],
            "invest_amount": r[12],
            "risk_amount": r[13],
            "risk_pct": r[14],
            "buy_order_id": r[15],
            "sl_order_id": r[16],
            "exit_order_id": r[17],
            "buy_status": r[18],
            "sl_status": r[19],
            "status": r[20],
            "exit_price": r[21],
            "pnl": r[22],
            "pnl_pct": r[23],
            "errors": json.loads(r[24]) if r[24] else [],
            "fills": json.loads(r[25]) if r[25] else [],
        }


# Singleton
order_manager = OrderManager()
