"""Stop-loss management — placement, trailing, morning refresh, recovery.

Every sell operation:
  1. Checks market hours (skip if off-hours)
  2. Checks actual Kite deliverable holdings (never trust DB alone)
  3. Deducts pending sell orders from available qty
  4. Caps qty to min(db_filled_qty, deliverable - pending_sells)
  5. Handles T+1 settlement (skip & defer)
  6. Never touches personal holdings (only system-tagged orders)
"""

from __future__ import annotations

import time as _time
from datetime import date, datetime

from faralpha import config
from faralpha.kite import db_store
from faralpha.kite.holdings import (
    compute_sellable_qty,
    get_held_stocks,
    get_pending_sells,
)
from faralpha.kite.market_hours import (
    is_market_open,
    round_to_tick,
)
from faralpha.utils.logger import get_logger

log = get_logger("sl_manager")

REVERSAL_CFG = config.INTRADAY_REVERSAL

# Kite order statuses
STATUS_COMPLETE = "COMPLETE"
STATUS_CANCELLED = "CANCELLED"
STATUS_REJECTED = "REJECTED"
STATUS_OPEN = "OPEN"
STATUS_TRIGGER_PENDING = "TRIGGER PENDING"

MAX_SL_RETRIES = 3


def place_sl(
    kite,
    order: dict,
    quantity: int,
    trigger_price: float,
    tick_fn,
    emit_fn=None,
) -> None:
    """Place SL order with retries. If trigger >= LTP, exits immediately.

    Safety checks:
      - Market hours: refuses to place if market is closed
      - Max qty capped to filled_qty (protects personal holdings)
      - Blocks duplicate exits (if exit_order_id already set)
    """
    ticker = order["ticker"]

    # ── MARKET HOURS GATE ──
    if not is_market_open():
        log.warning("SL BLOCKED: %s — market closed, deferring SL placement", ticker)
        return

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

    tick = tick_fn(ticker)
    trigger = round_to_tick(trigger_price, tick)
    limit = round_to_tick(trigger * 0.99, tick)

    # Check if stop is already breached (trigger >= LTP)
    try:
        ltp_data = kite.ltp([f"NSE:{ticker}"])
        ltp = ltp_data.get(f"NSE:{ticker}", {}).get("last_price", 0)
    except Exception:
        ltp = 0

    if ltp > 0 and trigger >= ltp:
        sell_limit = round_to_tick(ltp * 0.99, tick)
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
            fill = _poll_exit_fill(kite, str(sell_id))
            exit_px = fill or sell_limit
            pnl = (exit_px - entry) * qty if entry > 0 else 0
            log.info(
                "EXIT SELL placed: %s order_id=%s fill=%.2f PnL=%.0f (stop breached%s)",
                ticker, sell_id, exit_px, pnl, "" if fill else ", fill est.")
            order["exit_price"] = exit_px
            order["pnl"] = round(pnl, 2)
            order["pnl_pct"] = round((exit_px / entry - 1) * 100, 2) if entry > 0 else 0
            order["status"] = "exit_sl_breached"
            order["sl_status"] = STATUS_COMPLETE
            order["current_stop"] = trigger
            return
        except Exception as e:
            log.error("EXIT SELL failed for %s: %s — falling back to SL order", ticker, e)

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
                ticker, sl_order_id, trigger, limit, attempt)
            order["sl_order_id"] = str(sl_order_id)
            order["sl_status"] = STATUS_TRIGGER_PENDING
            order["current_stop"] = trigger
            if order.get("status") == "bought":
                order["status"] = "protected"
            return
        except Exception as e:
            err_msg = str(e).lower()
            if "trigger price" in err_msg and (
                    "lower than" in err_msg or "last traded" in err_msg):
                log.warning("SL %s: trigger %.2f >= LTP, placing market exit", ticker, trigger)
                try:
                    sell_id = kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange=kite.EXCHANGE_NSE,
                        tradingsymbol=ticker,
                        transaction_type=kite.TRANSACTION_TYPE_SELL,
                        quantity=quantity,
                        order_type=kite.ORDER_TYPE_MARKET,
                        product=kite.PRODUCT_CNC,
                        validity=kite.VALIDITY_DAY,
                        tag="faralpha",
                    )
                    entry = order.get("avg_fill_price", 0)
                    qty = order.get("filled_qty", quantity)
                    order["exit_order_id"] = str(sell_id)
                    fill = _poll_exit_fill(kite, str(sell_id))
                    exit_px = fill or trigger
                    order["exit_price"] = exit_px
                    order["pnl"] = round((exit_px - entry) * qty, 2) if entry > 0 else 0
                    order["pnl_pct"] = round((exit_px / entry - 1) * 100, 2) if entry > 0 else 0
                    order["status"] = "exit_sl_breached"
                    order["sl_status"] = STATUS_COMPLETE
                    log.info(
                        "MARKET EXIT: %s order_id=%s fill=%.2f PnL=%.0f%s",
                        ticker, sell_id, exit_px, order["pnl"],
                        "" if fill else " (fill est.)")
                    return
                except Exception as e2:
                    log.error("Market exit also failed for %s: %s", ticker, e2)
            log.error("SL placement failed for %s (attempt %d/%d): %s",
                      ticker, attempt, MAX_SL_RETRIES, e)
            order["errors"].append(f"SL attempt {attempt}: {e}")
            if attempt < MAX_SL_RETRIES:
                _time.sleep(2)

    # All retries exhausted
    log.critical("CRITICAL: %s has NO STOP LOSS! Manual intervention required.", ticker)
    order["status"] = "UNPROTECTED"
    if emit_fn:
        emit_fn("order_error", {
            **order,
            "critical": True,
            "message": f"STOP LOSS FAILED for {ticker} after {MAX_SL_RETRIES} attempts!",
        })


def trail_open_positions(kite, today_orders, open_positions, tick_fn, lock, persist_fn):
    """Trail SL up for all protected positions using LTP. Runs every poll cycle.

    Skips if market is closed — trailing is only meaningful during live trading.
    """
    if not is_market_open():
        return

    with lock:
        trailable = {}
        for ticker, order in {**today_orders, **open_positions}.items():
            if (order.get("status") in ("protected", "bought")
                    and order.get("sl_order_id")
                    and order.get("sl_status") == STATUS_TRIGGER_PENDING):
                trailable[ticker] = order

    if not trailable:
        return

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
        tick = tick_fn(ticker)

        new_stop = round_to_tick(last_price * (1 - trail_pct), tick)
        if new_stop <= current_stop:
            continue

        sl_order_id = order["sl_order_id"]
        new_limit = round_to_tick(new_stop * 0.99, tick)
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
            persist_fn(order)
        except Exception as e:
            err_msg = str(e)
            if "modifications exceeded" in err_msg.lower():
                log.warning("TRAIL %s: max modifications hit — replacing SL order", ticker)
                try:
                    kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_order_id)
                    order["sl_order_id"] = None
                    order["current_stop"] = new_stop
                    qty = order.get("filled_qty", 0)
                    place_sl(kite, order, qty, new_stop, tick_fn)
                    log.info("TRAIL %s: replaced SL -> %.2f (LTP=%.2f)",
                             ticker, new_stop, last_price)
                    persist_fn(order)
                except Exception as e2:
                    log.error("TRAIL %s: SL replacement failed: %s", ticker, e2)
            else:
                log.error("Trail modify failed for %s (SL %s): %s", ticker, sl_order_id, e)


def recover_unprotected(kite, today_orders, open_positions, tick_fn, lock, persist_fn):
    """Check UNPROTECTED positions — recover SL if deliverable, skip if T+1.

    Skips if market is closed — SL can only be placed during trading hours.
    """
    if not is_market_open():
        return

    with lock:
        unprotected = {t: o for t, o in {**today_orders, **open_positions}.items()
                       if o.get("status") == "UNPROTECTED"}

    if not unprotected:
        return

    try:
        kite_orders_list = kite.orders()
    except Exception:
        return
    kite_by_id = {str(o["order_id"]): o for o in kite_orders_list}

    held = get_held_stocks(kite)
    pending_sells = get_pending_sells(kite_orders_list)

    for ticker, order in unprotected.items():
        # Check if we already have a live SL
        sl_id = order.get("sl_order_id")
        if sl_id:
            kite_sl = kite_by_id.get(str(sl_id))
            if kite_sl and kite_sl.get("status") == STATUS_TRIGGER_PENDING:
                order["status"] = "protected"
                order["sl_status"] = STATUS_TRIGGER_PENDING
                persist_fn(order)
                log.info("RECOVERED: %s SL active on Kite (order %s) — status -> protected",
                         ticker, sl_id)
                continue

        # Scan all orders for a live SL on this ticker
        found_sl = False
        for oid, ko in kite_by_id.items():
            if (ko.get("tradingsymbol") == ticker
                    and ko.get("transaction_type") == "SELL"
                    and ko.get("status") == STATUS_TRIGGER_PENDING
                    and ko.get("product") == "CNC"):
                order["sl_order_id"] = oid
                order["sl_status"] = STATUS_TRIGGER_PENDING
                order["status"] = "protected"
                persist_fn(order)
                log.info("RECOVERED: %s SL found by scan (order %s) — status -> protected",
                         ticker, oid)
                found_sl = True
                break
        if found_sl:
            continue

        # No SL exists — check actual holdings
        sellable, deliverable, t1 = compute_sellable_qty(
            ticker, order.get("filled_qty", 0), held, pending_sells)

        if deliverable <= 0:
            if t1 > 0:
                log.info("RECOVER SKIP: %s — %d shares in T+1 settlement, SL deferred", ticker, t1)
            else:
                log.warning("RECOVER SKIP: %s — 0 deliverable holdings, cannot place SL", ticker)
            continue

        stop = order.get("current_stop") or order.get("initial_stop") or 0
        if sellable > 0 and stop > 0:
            log.info("RECOVER: re-placing SL for %s qty=%d stop=%.2f", ticker, sellable, stop)
            place_sl(kite, order, sellable, stop, tick_fn)
            persist_fn(order)


def morning_sl_refresh(kite, open_positions, tick_fn, lock, persist_fn) -> list[dict]:
    """Re-place SL for all open positions. Called at market open each day.

    Handles:
      - T+1 settlement: skip SL, defer to next cycle
      - Partial holdings: cap SL qty to deliverable
      - Pending sells: deduct from available qty
      - Ghost positions: 0 in Kite after >1 day → mark closed
      - Max hold expiry: sell at market
      - Trailing stop: trail up using LTP
      - Existing SL detection: don't double-place
    """
    results = []

    # ── MARKET HOURS GATE ──
    if not is_market_open():
        log.info("Morning SL refresh: market closed — skipping all operations")
        return [{"skipped": "market_closed"}]

    with lock:
        open_orders = list(open_positions.items())

    if not open_orders:
        log.info("Morning SL refresh: no open positions")
        return results

    # ── Fetch Kite state ──
    net_positions = get_held_stocks(kite)

    if not net_positions and open_orders:
        log.warning(
            "Morning SL refresh: Kite returned 0 net positions but we track %d "
            "— likely holiday. Skipping.", len(open_orders))
        return [{"skipped": "holiday_or_no_data", "open_count": len(open_orders)}]

    live_orders = [(t, o) for t, o in open_orders
                   if o.get("status") not in ("closed", "cancelled", "error")]

    if live_orders:
        missing = [t for t, _ in live_orders if t not in net_positions]
        if len(missing) > 0 and len(missing) == len(live_orders):
            log.warning(
                "Morning SL refresh: ALL %d live positions missing from Kite (%s). "
                "Likely API error — skipping.", len(missing), ", ".join(missing))
            return [{"skipped": "all_positions_missing", "missing": missing}]

    # Fetch Kite orders once
    kite_orders_list: list[dict] = []
    kite_by_id: dict[str, dict] = {}
    try:
        kite_orders_list = kite.orders()
        kite_by_id = {str(o["order_id"]): o for o in kite_orders_list}
    except Exception as e:
        log.warning("Morning SL refresh: failed to fetch Kite orders: %s", e)

    pending_sells = get_pending_sells(kite_orders_list)

    for ticker, order in open_orders:
        if order.get("status") in ("closed", "cancelled", "error"):
            with lock:
                open_positions.pop(ticker, None)
            continue

        # ── Determine actual sellable quantity ──
        db_qty = order.get("filled_qty", 0)
        sellable, deliverable, t1_qty = compute_sellable_qty(
            ticker, db_qty, net_positions, pending_sells)
        pos = net_positions.get(ticker)
        pending_sell = pending_sells.get(ticker, 0)

        # ── Position not deliverable ──
        if deliverable <= 0:
            if t1_qty > 0:
                log.info("T+1 PENDING: %s — %d shares settling, SL deferred", ticker, t1_qty)
                results.append({"ticker": ticker, "action": "t1_pending", "t1_qty": t1_qty})
                continue

            if (order.get("buy_status") == STATUS_COMPLETE
                    and not order.get("exit_order_id")):
                order_date = order.get("time", "")[:10]
                days_ago = 99
                if order_date:
                    try:
                        days_ago = (date.today() - date.fromisoformat(order_date)).days
                    except ValueError:
                        pass

                if days_ago <= 1:
                    log.info("T+1 PENDING: %s — bought %s, not yet in holdings", ticker, order_date)
                    results.append({"ticker": ticker, "action": "t1_pending"})
                    continue
                else:
                    log.warning(
                        "GHOST POSITION: %s — bought %s (%dd ago), 0 in Kite. Marking closed.",
                        ticker, order_date, days_ago)
                    with lock:
                        open_positions.pop(ticker, None)
                    order["status"] = "closed"
                    order["errors"].append(f"Ghost: 0 holdings after {days_ago}d")
                    persist_fn(order)
                    results.append({"ticker": ticker, "action": "ghost_closed", "days_ago": days_ago})
                    continue

            log.info("Position %s no longer held — removing", ticker)
            with lock:
                open_positions.pop(ticker, None)
            order["status"] = "closed"
            persist_fn(order)
            continue

        # ── All shares covered by pending sell orders ──
        if sellable <= 0 and pending_sell > 0:
            log.info("SL SKIP: %s — all %d deliverable shares covered by %d pending sell qty",
                     ticker, deliverable, pending_sell)
            results.append({"ticker": ticker, "action": "pending_sell_covers",
                            "deliverable": deliverable, "pending_sell": pending_sell})
            continue

        # ── Cap SL qty ──
        sl_qty = sellable
        if sl_qty < db_qty and sl_qty > 0:
            log.warning("SL QTY CAPPED: %s %d → %d (deliverable=%d, pending_sell=%d)",
                        ticker, db_qty, sl_qty, deliverable, pending_sell)
        if sl_qty <= 0:
            log.warning("SL SKIP: %s — 0 sellable qty (db=%d, deliverable=%d, pending_sell=%d)",
                        ticker, db_qty, deliverable, pending_sell)
            results.append({"ticker": ticker, "action": "nothing_sellable"})
            continue

        # ── Max hold expiry ──
        exit_date = order.get("exit_date", "")
        if exit_date and date.today().isoformat() >= exit_date:
            log.info("MAX HOLD reached for %s — placing market sell (qty=%d)", ticker, sl_qty)
            try:
                sell_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=ticker,
                    transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=sl_qty,
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
                results.append({"ticker": ticker, "action": "max_hold_exit", "error": str(e)})
            with lock:
                open_positions.pop(ticker, None)
            persist_fn(order)
            continue

        # ── Trail stop ──
        entry = order.get("avg_fill_price", 0)
        trail_pct = order.get("trail_pct", 0.02)
        current_stop = order.get("current_stop", 0)
        last_price = pos.get("last_price", 0) if pos else 0

        if last_price > entry > 0:
            tick = tick_fn(ticker)
            new_stop = round_to_tick(last_price * (1 - trail_pct), tick)
            if new_stop > current_stop:
                current_stop = new_stop
                order["current_stop"] = current_stop
                log.info("TRAILING UP: %s stop -> %.2f", ticker, current_stop)

        # ── Check existing SL on Kite ──
        existing_sl = order.get("sl_order_id")
        if existing_sl and kite_by_id:
            kite_sl = kite_by_id.get(str(existing_sl))
            if kite_sl and kite_sl.get("status") == STATUS_TRIGGER_PENDING:
                log.info("SL already active: %s order_id=%s trigger=%.2f",
                         ticker, existing_sl, kite_sl.get("trigger_price", 0))
                order["sl_status"] = STATUS_TRIGGER_PENDING
                if order.get("status") in ("bought", "UNPROTECTED"):
                    order["status"] = "protected"
                persist_fn(order)
                results.append({"ticker": ticker, "action": "sl_already_active",
                                "stop": current_stop, "sl_order_id": existing_sl})
                continue

        # ── Fallback: scan Kite orders for TRIGGER PENDING sell ──
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
                    persist_fn(order)
                    results.append({"ticker": ticker, "action": "sl_found_by_scan",
                                    "stop": current_stop, "sl_order_id": oid})
                    break
            else:
                place_sl(kite, order, sl_qty, current_stop, tick_fn)
                persist_fn(order)
                results.append({"ticker": ticker, "action": "sl_refreshed",
                                "stop": current_stop, "qty": sl_qty})
            continue

        place_sl(kite, order, sl_qty, current_stop, tick_fn)
        persist_fn(order)
        results.append({"ticker": ticker, "action": "sl_refreshed",
                        "stop": current_stop, "qty": sl_qty})

    return results


def _poll_exit_fill(kite, order_id: str, max_wait: float = 5.0) -> float | None:
    """Poll Kite for actual fill price of an exit order."""
    end = _time.monotonic() + max_wait
    while _time.monotonic() < end:
        try:
            hist = kite.order_history(order_id)
            for h in reversed(hist):
                if h.get("status") == "COMPLETE" and h.get("average_price", 0) > 0:
                    return h["average_price"]
        except Exception:
            pass
        _time.sleep(1)
    return None
