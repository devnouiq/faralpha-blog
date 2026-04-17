"""OrderManager — thin orchestrator for signal→buy→SL→trail→exit lifecycle.

Delegates to:
  - market_hours: gate all operations to NSE trading hours
  - holdings: check actual Kite state before any sell
  - sl_manager: SL placement, trailing, recovery, morning refresh
  - db_store: DuckDB persistence
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from threading import Lock, Thread

from kiteconnect import KiteConnect

from faralpha import config
from faralpha.kite import db_store, sl_manager
from faralpha.kite.holdings import get_held_stocks
from faralpha.kite.market_hours import (
    TICK_SIZE,
    is_market_open,
    market_status,
    round_to_tick,
    round_up_to_tick,
)
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("kite_orders")

KITE_CFG = config.KITE
REVERSAL_CFG = config.INTRADAY_REVERSAL
DATA_DIR = config.DATA_DIR

# Max late-entry slippage
MAX_LATE_ENTRY_PCT = 0.005

# Order polling interval
POLL_INTERVAL_SEC = 30

# Kite order statuses
STATUS_COMPLETE = "COMPLETE"
STATUS_CANCELLED = "CANCELLED"
STATUS_REJECTED = "REJECTED"
STATUS_OPEN = "OPEN"
STATUS_TRIGGER_PENDING = "TRIGGER PENDING"


class OrderManager:
    """Production-grade order management for intraday reversal signals."""

    def __init__(self):
        self._enabled = False
        self._lock = Lock()
        self._today_orders: dict[str, dict] = {}
        self._open_positions: dict[str, dict] = {}
        self._pending_queue: list[dict] = []
        self._kite: KiteConnect | None = None
        self._tick_sizes: dict[str, float] = {}
        self._poller_thread: Thread | None = None
        self._poller_running = False
        self._on_order_event = None
        self._breadth_cache: dict = {}

        db_store.ensure_table()
        today, open_pos = db_store.load_today_orders()
        self._today_orders = today
        self._open_positions = open_pos

    # ── tick size ──

    def _load_tick_sizes(self):
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
        if not self._tick_sizes:
            self._load_tick_sizes()
        return self._tick_sizes.get(ticker, TICK_SIZE)

    # ── state ──

    @property
    def enabled(self) -> bool:
        return self._enabled

    def enable(self) -> dict:
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
            log.warning("Kite health check warning: %s (proceeding anyway)", err)

        self._enabled = True
        self._start_poller()
        log.info("AUTO-TRADE ENABLED")
        return {"auto_trade": True}

    def process_pending_signals(self, signals: list[dict]) -> list:
        if not self._enabled or not signals:
            return []
        results = []
        for sig in signals:
            ticker = sig.get("ticker", "").replace(".NS", "")
            if ticker in self._today_orders:
                continue
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
        ticker_clean = ticker.replace(".NS", "")
        signal = None
        for p in self._pending_queue:
            t = p.get("ticker", "").replace(".NS", "")
            if t == ticker_clean:
                signal = p
                break
        if not signal:
            return {"status": "error", "message": f"No pending signal for {ticker_clean}"}

        self._pending_queue = [
            p for p in self._pending_queue
            if p.get("ticker", "").replace(".NS", "") != ticker_clean
        ]

        with self._lock:
            self._today_orders.pop(ticker_clean, None)

        result = self.on_signal(signal, force=True)
        if result:
            return {"status": "ok", "order": result}
        return {"status": "error", "message": f"Order placement failed for {ticker_clean}"}

    def dismiss_pending(self, ticker: str) -> dict:
        ticker_clean = ticker.replace(".NS", "")
        before = len(self._pending_queue)
        self._pending_queue = [
            p for p in self._pending_queue
            if p.get("ticker", "").replace(".NS", "") != ticker_clean
        ]
        removed = before - len(self._pending_queue)
        return {"status": "ok", "removed": removed}

    def retry_failed_order(self, ticker: str) -> dict:
        ticker_clean = ticker.replace(".NS", "")

        with self._lock:
            order = self._today_orders.get(ticker_clean)
            if not order:
                return {"status": "error", "message": f"No order found for {ticker_clean}"}
            if order.get("status") != "error":
                return {"status": "error",
                        "message": f"{ticker_clean} status is '{order.get('status')}', not 'error'"}

            signal = {
                "ticker": ticker_clean,
                "price": order.get("signal_price", 0),
                "trailing_stop_pct": order.get("trail_pct",
                    REVERSAL_CFG.get("trailing_stop_pct", 0.02)),
                "max_hold_days": order.get("max_hold_days",
                    REVERSAL_CFG.get("max_hold_days", 7)),
            }
            self._today_orders.pop(ticker_clean, None)

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
            self._today_orders.pop(ticker, None)

        result = self.on_signal(signal, force=True)
        if result:
            return {"status": "ok", "order": result}
        return {"status": "error", "message": f"Order placement failed for {ticker}"}

    def set_on_order_event(self, callback):
        self._on_order_event = callback

    def _skip_signal(self, signal: dict, ticker: str, reason: str):
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

    # ── order placement ──

    def on_signal(self, signal: dict, force: bool = False) -> dict | None:
        """Place LIMIT BUY on signal fire. Returns order dict or None if skipped."""
        if not self._enabled and not force:
            return None

        # ── MARKET HOURS GATE ──
        if not is_market_open():
            log.warning("SKIP signal — market is %s", market_status())
            return None

        # ── Breadth skip zone ──
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

        # ── Breadth reduce zone ──
        breadth_size_factor = 1.0
        if not force and REVERSAL_CFG.get("breadth_reduce_enabled", False):
            breadth = self._get_today_breadth()
            reduce_lo = REVERSAL_CFG.get("breadth_reduce_low", 0.30)
            reduce_hi = REVERSAL_CFG.get("breadth_reduce_high", 0.55)
            if breadth is not None and reduce_lo <= breadth <= reduce_hi:
                breadth_size_factor = REVERSAL_CFG.get("breadth_reduce_factor", 0.60)
                log.info("REDUCE %s — breadth %.1f%% in reduce zone, factor=%.1fx",
                         signal.get("ticker", "?"), breadth * 100, breadth_size_factor)

        ticker = signal["ticker"]
        trading_symbol = ticker.replace(".NS", "")

        # ── Rolling rvol tier ──
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
                    break

        with self._lock:
            if trading_symbol in self._today_orders:
                log.warning("SKIP %s — already ordered today", trading_symbol)
                return None

            real_today = sum(
                1 for o in self._today_orders.values()
                if o.get("status") not in ("error", "cancelled")
            )
            active = real_today + len(self._open_positions)
            max_pos = REVERSAL_CFG.get("max_positions", 5)
            if not force and active >= max_pos:
                log.warning("SKIP %s — max positions (%d) reached", trading_symbol, max_pos)
                self._skip_signal(signal, ticker, "max_positions")
                return None

        signal_price = signal["price"]
        trail_pct = signal.get("trailing_stop_pct", 0.02)
        max_hold = signal.get("max_hold_days", 7)

        tick = self._get_tick_size(trading_symbol)
        max_entry_price = round_up_to_tick(signal_price * (1 + MAX_LATE_ENTRY_PCT), tick)
        initial_stop = round_to_tick(signal_price * (1 - trail_pct), tick)

        if max_entry_price <= initial_stop:
            log.error("SKIP %s — max_entry %.2f <= initial_stop %.2f",
                      trading_symbol, max_entry_price, initial_stop)
            return None

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
            "ORDER %s | Qty=%d | Limit=%.2f | SL=%.2f | Invest=%.0f | Risk=%.0f (%.1f%%)",
            trading_symbol, quantity, max_entry_price, initial_stop,
            invest_amount, risk_amount, risk_pct)

        try:
            kite = self._get_kite()

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

            # Trading days (skip weekends)
            exit_d = date.today()
            added = 0
            while added < max_hold:
                exit_d += timedelta(days=1)
                if exit_d.weekday() < 5:
                    added += 1
            exit_date = exit_d.isoformat()

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

            db_store.persist_order(order_info)
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
            db_store.persist_order(error_info)
            self._emit_event("order_error", error_info)
            self._skip_signal(signal, ticker, f"order_failed: {e}")
            return error_info

    # ── order poller ──

    def _start_poller(self):
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
        while self._poller_running:
            try:
                self._poll_orders()
            except Exception as e:
                log.error("Poller error: %s", e)
            time.sleep(POLL_INTERVAL_SEC)

    def _poll_orders(self):
        """Single poll cycle — only runs order checks during market hours."""
        kite = self._get_kite()

        # Status checks (buy fill, SL trigger) can run anytime to catch updates
        with self._lock:
            active = {k: v for k, v in self._today_orders.items()
                      if v.get("status") not in ("closed", "error", "cancelled")}

        if active:
            try:
                kite_orders = kite.orders()
            except Exception as e:
                log.error("Failed to fetch Kite orders: %s", e)
                return

            kite_by_id = {str(o["order_id"]): o for o in kite_orders}

            for ticker, order in active.items():
                self._check_buy_status(kite, order, kite_by_id)
                self._check_sl_status(kite, order, kite_by_id)

        # Trailing + UNPROTECTED recovery — only during market hours
        if is_market_open():
            sl_manager.trail_open_positions(
                kite, self._today_orders, self._open_positions,
                self._get_tick_size, self._lock, db_store.persist_order)
            sl_manager.recover_unprotected(
                kite, self._today_orders, self._open_positions,
                self._get_tick_size, self._lock, db_store.persist_order)

    def _check_buy_status(self, kite, order: dict, kite_by_id: dict):
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
            log.info("BUY FILLED: %s | Qty=%d | AvgPrice=%.2f", ticker, filled_qty, avg_price)
            order["status"] = "bought"
            order["fills"].append({
                "type": "buy_fill",
                "qty": filled_qty,
                "price": avg_price,
                "time": datetime.now().isoformat(),
            })

            tick = self._get_tick_size(ticker)
            actual_stop = round_to_tick(avg_price * (1 - order["trail_pct"]), tick)
            if actual_stop > order["initial_stop"]:
                order["current_stop"] = actual_stop
                order["initial_stop"] = actual_stop

            # Place SL only during market hours
            if not order.get("sl_order_id") and is_market_open():
                sl_manager.place_sl(
                    kite, order, filled_qty, order["current_stop"],
                    self._get_tick_size, self._emit_event)

            db_store.persist_order(order)
            self._emit_event("buy_filled", order)

        elif kite_status == STATUS_REJECTED:
            log.error("BUY REJECTED: %s — %s", ticker, kite_order.get("status_message", ""))
            order["status"] = "error"
            order["errors"].append(f"BUY rejected: {kite_order.get('status_message', '')}")
            db_store.persist_order(order)
            self._emit_event("order_error", order)

        elif kite_status == STATUS_CANCELLED:
            log.info("BUY CANCELLED: %s (unfilled EOD or manual)", ticker)
            order["status"] = "cancelled"
            db_store.persist_order(order)

    def _check_sl_status(self, kite, order: dict, kite_by_id: dict):
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
            db_store.persist_order(order)
            self._cancel_pending_sls(kite, ticker)
            self._emit_event("position_closed", order)

        elif kite_status == STATUS_REJECTED:
            log.error("SL REJECTED: %s — position unprotected! %s",
                      ticker, kite_order.get("status_message", ""))
            order["errors"].append(f"SL rejected: {kite_order.get('status_message', '')}")
            order["sl_order_id"] = None
            if is_market_open():
                sl_manager.place_sl(
                    kite, order, order.get("filled_qty", 0), order["current_stop"],
                    self._get_tick_size, self._emit_event)
            db_store.persist_order(order)
            self._emit_event("order_error", order)

        elif kite_status == STATUS_CANCELLED:
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
            db_store.persist_order(order)

    # ── morning SL refresh ──

    def morning_sl_refresh(self) -> list[dict]:
        """Delegate to sl_manager with access to our state."""
        kite = self._get_kite()
        return sl_manager.morning_sl_refresh(
            kite, self._open_positions, self._get_tick_size,
            self._lock, db_store.persist_order)

    # ── sync ──

    def sync_from_kite(self) -> dict:
        kite = self._get_kite()
        synced = {"orders_synced": 0, "fills_updated": 0}

        try:
            kite_orders = kite.orders()
            kite_by_id = {str(o["order_id"]): o for o in kite_orders}
            faralpha_orders = [o for o in kite_orders if o.get("tag") == "faralpha"]
            synced["orders_synced"] = len(faralpha_orders)

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
                        db_store.persist_order(order)
                        synced["fills_updated"] += 1
                        log.info("SYNC: updated %s exit fill %.2f", ticker, actual_price)
        except Exception as e:
            log.warning("sync_from_kite failed: %s", e)

        return synced

    def reconcile_from_kite(self) -> list[dict]:
        results = []
        try:
            con = get_conn()
            rows = con.execute("""
                SELECT ticker, order_date, current_stop, trail_pct, filled_qty
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

        try:
            kite = self._get_kite()
            held = get_held_stocks(kite)
        except Exception as e:
            log.warning("Reconcile: Kite fetch failed: %s", e)
            return [{"error": str(e)}]

        for ticker, odate, stop, trail, db_qty in rows:
            if ticker in self._open_positions or ticker in self._today_orders:
                continue
            if ticker not in held:
                continue

            restored = db_store.restore_closed_order(ticker)
            if restored:
                with self._lock:
                    self._open_positions[ticker] = restored
                log.info("RECONCILE: restored %s (qty=%d)", ticker, restored["filled_qty"])
                results.append({"ticker": ticker, "action": "restored", "qty": restored["filled_qty"]})

        if results:
            log.info("Reconcile: %d positions restored", len(results))
        return results

    # ── capital ──

    def _get_capital(self) -> float:
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
        trading_symbol = ticker.replace(".NS", "")
        order = (self._today_orders.get(trading_symbol) or
                 self._open_positions.get(trading_symbol))
        if not order:
            return {"status": "error", "message": f"No position for {trading_symbol}"}

        new_trigger = round_to_tick(new_stop, self._get_tick_size(trading_symbol))
        current = order.get("current_stop", 0)
        if new_trigger <= current:
            return {"status": "error",
                    "message": f"New stop {new_trigger:.2f} must be above current {current:.2f}"}

        sl_id = order.get("sl_order_id")
        if not sl_id:
            return {"status": "error", "message": "No active SL order to modify"}

        if not is_market_open():
            return {"status": "error", "message": "Market is closed — cannot modify SL"}

        try:
            kite = self._get_kite()
            kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id=sl_id,
                trigger_price=new_trigger,
            )
            log.info("SL TRAILED: %s %.2f -> %.2f", trading_symbol, current, new_trigger)
            order["current_stop"] = new_trigger
            db_store.persist_order(order)
            self._emit_event("sl_updated", order)
            return {"status": "ok", "old_stop": current, "new_stop": new_trigger}
        except Exception as e:
            log.error("SL trail failed for %s: %s", trading_symbol, e)
            return {"status": "error", "message": str(e)}

    def cancel_buy(self, ticker: str) -> dict:
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
            db_store.persist_order(order)
            return {"status": "ok", "cancelled": order["buy_order_id"]}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def force_exit(self, ticker: str) -> dict:
        """Force-exit a position. Checks Kite holdings to cap qty."""
        trading_symbol = ticker.replace(".NS", "")
        order = (self._today_orders.get(trading_symbol)
                 or self._open_positions.get(trading_symbol))
        if not order:
            return {"status": "error", "message": f"No position for {trading_symbol}"}

        if order.get("exit_order_id"):
            return {"status": "error",
                    "message": f"{trading_symbol} already has exit order {order['exit_order_id']}"}

        if not is_market_open():
            return {"status": "error", "message": "Market is closed — cannot force exit"}

        filled_qty = order.get("filled_qty", 0)
        if filled_qty <= 0:
            return {"status": "error", "message": f"No filled shares for {trading_symbol}"}

        try:
            kite = self._get_kite()

            # Check actual deliverable holdings before selling
            held = get_held_stocks(kite)
            pos = held.get(trading_symbol)
            deliverable = pos.get("deliverable_qty", pos.get("quantity", 0)) if pos else 0
            sell_qty = min(filled_qty, deliverable) if deliverable > 0 else filled_qty
            if sell_qty <= 0:
                t1 = pos.get("t1_quantity", 0) if pos else 0
                if t1 > 0:
                    return {"status": "error",
                            "message": f"{trading_symbol}: {t1} shares in T+1 settlement, cannot sell yet"}
                return {"status": "error",
                        "message": f"{trading_symbol}: 0 deliverable holdings in Kite"}

            # Cancel all pending SL orders for this ticker
            self._cancel_pending_sls(kite, trading_symbol)

            ltp_data = kite.ltp(f"NSE:{trading_symbol}")
            ltp = ltp_data.get(f"NSE:{trading_symbol}", {}).get("last_price", 0)
            if ltp <= 0:
                return {"status": "error", "message": f"Cannot get LTP for {trading_symbol}"}

            sell_price = round_to_tick(ltp * 0.998, self._get_tick_size(trading_symbol))

            sell_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=trading_symbol,
                transaction_type=kite.TRANSACTION_TYPE_SELL,
                quantity=sell_qty,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=sell_price,
                product=kite.PRODUCT_CNC,
                validity=kite.VALIDITY_DAY,
                tag="faralpha",
            )

            log.info("FORCE EXIT: %s | Qty=%d | Sell=%.2f | order_id=%s",
                     trading_symbol, sell_qty, sell_price, sell_id)

            avg_fill = order.get("avg_fill_price", 0)
            pnl = (sell_price - avg_fill) * sell_qty if avg_fill else 0

            order["exit_order_id"] = str(sell_id)
            order["exit_price"] = sell_price
            order["pnl"] = round(pnl, 2)
            order["pnl_pct"] = round((sell_price / avg_fill - 1) * 100, 2) if avg_fill else 0
            order["status"] = "closed"
            order["sl_status"] = "cancelled"

            with self._lock:
                self._open_positions.pop(trading_symbol, None)

            db_store.persist_order(order)
            self._emit_event("position_closed", order)

            return {
                "status": "ok",
                "ticker": trading_symbol,
                "sell_order_id": str(sell_id),
                "sell_price": sell_price,
                "quantity": sell_qty,
                "pnl": round(pnl, 2),
            }
        except Exception as e:
            log.error("Force exit failed for %s: %s", trading_symbol, e)
            return {"status": "error", "message": str(e)}

    # ── cleanup ──

    def _cancel_pending_sls(self, kite, ticker: str):
        """Cancel ALL pending SL/sell orders for *ticker* on Kite.

        Scans the full order book for faralpha-tagged SELL orders that are
        still OPEN or TRIGGER PENDING and cancels every one of them.
        """
        try:
            kite_orders_list = kite.orders()
        except Exception as e:
            log.warning("cancel_pending_sls: cannot fetch orders for %s: %s", ticker, e)
            return
        for ko in kite_orders_list:
            if (ko.get("tradingsymbol") == ticker
                    and ko.get("transaction_type") == "SELL"
                    and ko.get("tag") == "faralpha"
                    and ko.get("status") in (STATUS_OPEN, STATUS_TRIGGER_PENDING)):
                try:
                    kite.cancel_order(variety=kite.VARIETY_REGULAR,
                                      order_id=ko["order_id"])
                    log.info("Cancelled pending SL %s for %s", ko["order_id"], ticker)
                except Exception as e:
                    log.warning("Failed to cancel SL %s for %s: %s",
                                ko["order_id"], ticker, e)

    # ── events ──

    def _emit_event(self, event_type: str, data: dict):
        if self._on_order_event:
            try:
                self._on_order_event(event_type, data)
            except Exception:
                pass


# Singleton
order_manager = OrderManager()
