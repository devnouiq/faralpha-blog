"""Routes: /api/orders/* — auto-trade order management"""

from __future__ import annotations

from fastapi import APIRouter

from faralpha.api import state
from faralpha.api.state import broadcast_from_thread
from faralpha.utils.db import get_conn

router = APIRouter()


@router.get("/api/orders/logbook")
async def orders_logbook():
    """Trade logbook — all orders with P&L summary."""
    try:
        from faralpha.kite.kite_orders import _ORDER_COLUMNS, _row_to_dict
        con = get_conn()
        rows = con.execute(f"""
            SELECT {_ORDER_COLUMNS}
            FROM orders
            ORDER BY order_date DESC, ticker
        """).fetchall()
        con.close()
    except Exception:
        return {"trades": [], "summary": {}}

    trades = []
    total_pnl = 0.0
    total_invested = 0.0
    wins = 0
    losses = 0
    open_count = 0
    for r in rows:
        d = _row_to_dict(r)
        status = d["status"] or ""

        # Auto-repair breached trades missing exit_price/pnl
        if (status == "exit_sl_breached"
                and d.get("exit_price") is None
                and d.get("avg_fill_price")):
            entry = d["avg_fill_price"]
            qty = d.get("filled_qty") or d.get("quantity") or 0
            # Try to get actual fill from Kite via sl/exit order
            exit_price = None
            for oid_key in ("exit_order_id", "sl_order_id"):
                oid = d.get(oid_key)
                if not oid:
                    continue
                try:
                    from faralpha.kite.kite_orders import order_manager
                    kite = order_manager._get_kite()
                    hist = kite.order_history(str(oid))
                    for h in reversed(hist):
                        if (h.get("status") == "COMPLETE"
                                and h.get("average_price", 0) > 0):
                            exit_price = h["average_price"]
                            break
                except Exception:
                    pass
                if exit_price:
                    break
            # Fallback to stop price if Kite lookup fails
            if not exit_price:
                exit_price = (d.get("current_stop")
                              or d.get("initial_stop") or 0)
            if exit_price and qty:
                d["exit_price"] = exit_price
                d["pnl"] = round((exit_price - entry) * qty, 2)
                d["pnl_pct"] = round(
                    (exit_price / entry - 1) * 100, 2)
                try:
                    rc = get_conn()
                    rc.execute(
                        "UPDATE orders SET exit_price=?, pnl=?, "
                        "pnl_pct=? WHERE ticker=? AND order_date=?"
                        " AND status='exit_sl_breached'",
                        [d["exit_price"], d["pnl"], d["pnl_pct"],
                         d["ticker"], d["time"]])
                    rc.close()
                except Exception:
                    pass

        pnl = d["pnl"] or 0
        status = d["status"] or ""
        trade = {
            "ticker": d["ticker"],
            "order_date": d["time"],
            "signal_price": d["signal_price"],
            "avg_fill_price": d["avg_fill_price"] or 0,
            "exit_price": d["exit_price"],
            "quantity": d["quantity"],
            "filled_qty": d["filled_qty"] or 0,
            "invest_amount": d["invest_amount"] or 0,
            "risk_amount": d["risk_amount"] or 0,
            "initial_stop": d["initial_stop"],
            "current_stop": d["current_stop"],
            "status": status,
            "pnl": round(pnl, 2) if pnl is not None else None,
            "pnl_pct": round(d["pnl_pct"], 2) if d["pnl_pct"] is not None else None,
            "exit_date": d["exit_date"],
            "max_hold_days": d["max_hold_days"],
            "trail_pct": d["trail_pct"],
        }
        trades.append(trade)
        is_closed = status in ("closed", "exit_sl_breached")
        if is_closed:
            total_pnl += pnl
            total_invested += (d["invest_amount"] or 0)
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
            # pnl == 0 counts as closed but neither win nor loss
        elif status in ("bought", "protected", "UNPROTECTED"):
            open_count += 1

    closed = wins + losses
    summary = {
        "total_trades": len(trades),
        "closed": closed,
        "open": open_count,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / closed * 100, 1) if closed > 0 else 0,
        "total_pnl": round(total_pnl, 2),
        "total_invested": round(total_invested, 2),
        "return_pct": round(
            total_pnl / total_invested * 100, 2
        ) if total_invested > 0 else 0,
    }
    return {"trades": trades, "summary": summary}


@router.get("/api/orders/status")
async def orders_status():
    """Get auto-trade status and today's orders."""
    from faralpha.kite.kite_orders import order_manager
    return order_manager.status()


@router.post("/api/orders/enable")
async def orders_enable():
    """Enable auto-trading. Retroactively process any pending signals."""
    from faralpha.kite.kite_orders import order_manager
    result = order_manager.enable()
    if state.live_engine is not None:
        st = state.live_engine.get_status()
        pending = st.get("signal_history", [])
        if pending:
            order_manager.set_on_order_event(
                lambda evt, data: broadcast_from_thread(evt, data)
            )
            placed = order_manager.process_pending_signals(pending)
            result["retroactive_orders"] = len(placed)
    return result


@router.post("/api/orders/disable")
async def orders_disable():
    """Disable auto-trading."""
    from faralpha.kite.kite_orders import order_manager
    return order_manager.disable()


@router.post("/api/orders/update-sl")
async def orders_update_sl(req: dict):
    """Update trailing stop for a ticker. Body: {ticker, new_stop}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    new_stop = req.get("new_stop", 0)
    if not ticker or new_stop <= 0:
        return {"status": "error", "message": "ticker and new_stop required"}
    return order_manager.update_trailing_stop(ticker, new_stop)


@router.post("/api/orders/cancel-buy")
async def orders_cancel_buy(req: dict):
    """Cancel unfilled BUY order. Body: {ticker}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    return order_manager.cancel_buy(ticker)


@router.post("/api/orders/force-exit")
async def orders_force_exit(req: dict):
    """Force-exit a position: cancel SL + sell at market. Body: {ticker}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    return order_manager.force_exit(ticker)


@router.post("/api/orders/mark-exited")
async def orders_mark_exited(req: dict):
    """Mark a DB record as closed using Kite tradebook data.

    Use when shares were sold (manually or by old server) but DB
    still says protected.  Body: {ticker, order_date?, exit_price?}
    """
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "").replace(".NS", "")
    order_date = req.get("order_date")
    manual_exit = req.get("exit_price")
    if not ticker:
        return {"status": "error", "message": "ticker required"}

    # Find the DB record
    con = get_conn()
    where = "ticker = ? AND status IN ('protected','bought','UNPROTECTED')"
    params = [ticker]
    if order_date:
        where += " AND order_date = ?"
        params.append(order_date)
    row = con.execute(
        f"SELECT avg_fill_price, filled_qty, sl_order_id, "
        f"exit_order_id, order_date FROM orders "
        f"WHERE {where} ORDER BY order_date ASC LIMIT 1",
        params).fetchone()
    if not row:
        con.close()
        return {"status": "error",
                "message": f"No open record for {ticker}"}
    entry, qty, sl_oid, exit_oid, odate = row

    # Try to get exit price from Kite
    exit_price = manual_exit
    source = "manual" if manual_exit else "unknown"
    if not exit_price:
        try:
            kite = order_manager._get_kite()
            # Check SL order (may have been triggered)
            for oid in (exit_oid, sl_oid):
                if not oid:
                    continue
                try:
                    hist = kite.order_history(str(oid))
                    for h in reversed(hist):
                        if (h.get("status") == "COMPLETE"
                                and h.get("average_price", 0) > 0):
                            exit_price = h["average_price"]
                            source = "kite_order"
                            break
                except Exception:
                    pass
                if exit_price:
                    break
            # Fallback: search today's tradebook
            if not exit_price:
                trades = kite.trades()
                for tr in trades:
                    if (tr.get("tradingsymbol") == ticker
                            and tr.get("transaction_type") == "SELL"
                            and tr.get("fill_timestamp")):
                        exit_price = tr["average_price"]
                        source = "kite_tradebook"
                        break
        except Exception:
            pass

    if not exit_price:
        con.close()
        return {"status": "error",
                "message": f"Could not find exit price for {ticker}"
                           " — pass exit_price manually"}

    pnl = round((exit_price - entry) * qty, 2) if entry else 0
    pnl_pct = round(
        (exit_price / entry - 1) * 100, 2) if entry else 0
    con.execute(
        "UPDATE orders SET status='closed', exit_price=?, "
        "pnl=?, pnl_pct=? WHERE ticker=? AND order_date=?",
        [exit_price, pnl, pnl_pct, ticker, str(odate)])
    con.close()

    # Cancel any pending SL orders on Kite for this ticker
    try:
        kite = order_manager._get_kite()
        order_manager._cancel_pending_sls(kite, ticker)
    except Exception:
        pass

    # Remove from live tracking
    with order_manager._lock:
        order_manager._open_positions.pop(ticker, None)
        order_manager._today_orders.pop(ticker, None)

    broadcast_from_thread("orders_update", order_manager.status())
    return {
        "status": "ok", "ticker": ticker,
        "order_date": str(odate),
        "exit_price": exit_price,
        "pnl": pnl, "pnl_pct": pnl_pct,
        "source": source,
    }


@router.post("/api/orders/retry-sl")
async def orders_retry_sl(req: dict):
    """Retry SL placement for UNPROTECTED position. Body: {ticker}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "").replace(".NS", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    order = (order_manager._today_orders.get(ticker)
             or order_manager._open_positions.get(ticker))
    if not order:
        return {"status": "error", "message": f"No position for {ticker}"}
    qty = order.get("filled_qty", 0)
    stop = order.get("current_stop") or order.get("initial_stop")
    if qty <= 0 or not stop:
        return {"status": "error", "message": "No filled qty or stop price"}
    try:
        kite = order_manager._get_kite()
        order_manager._place_sl(kite, order, qty, stop)
        order_manager._persist_order(order)
        return {"status": "ok", "new_status": order.get("status")}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/api/orders/morning-refresh")
async def orders_morning_refresh():
    """Re-place SL-M orders for all open positions (call at 9:16)."""
    from faralpha.kite.kite_orders import order_manager
    return {"results": order_manager.morning_sl_refresh()}


@router.post("/api/orders/restore")
async def orders_restore(req: dict):
    """Restore a wrongly-closed position (no exit_order_id in DB).

    Re-opens the DB record, adds it to _open_positions, and places SL.
    Body: {ticker}
    """
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "").replace(".NS", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    restored = order_manager._restore_closed_order(ticker)
    if not restored:
        return {"status": "error", "message": f"No restorable record for {ticker}"}
    if restored.get("exit_order_id"):
        return {"status": "error",
                "message": f"{ticker} has exit_order_id — genuinely closed"}
    with order_manager._lock:
        order_manager._open_positions[ticker] = restored
    # Place SL
    try:
        kite = order_manager._get_kite()
        qty = restored.get("filled_qty", 0)
        stop = restored.get("current_stop") or restored.get("initial_stop")
        if qty > 0 and stop:
            order_manager._place_sl(kite, restored, qty, stop)
        order_manager._persist_order(restored)
    except Exception as e:
        return {"status": "partial", "message": f"Restored but SL failed: {e}",
                "ticker": ticker}
    broadcast_from_thread("orders_update", order_manager.status())
    return {"status": "ok", "ticker": ticker,
            "new_status": restored.get("status"),
            "stop": restored.get("current_stop")}


@router.post("/api/orders/sync")
async def orders_sync():
    """Cross-check local state against Kite positions/orders."""
    from faralpha.kite.kite_orders import order_manager
    return order_manager.sync_from_kite()


@router.get("/api/orders/pending")
async def orders_pending():
    """Get pending signals awaiting manual approval."""
    from faralpha.kite.kite_orders import order_manager
    return {"pending": list(order_manager._pending_queue)}


@router.post("/api/orders/repair-breached")
async def orders_repair_breached():
    """Backfill exit_price/pnl for exit_sl_breached rows missing data.

    Fetches actual fill prices from Kite order history when available.
    """
    from faralpha.kite.kite_orders import order_manager
    con = get_conn()
    rows = con.execute("""
        SELECT ticker, order_date, avg_fill_price, filled_qty,
               current_stop, initial_stop, sl_order_id, exit_order_id
        FROM orders
        WHERE status = 'exit_sl_breached'
          AND (exit_price IS NULL OR pnl IS NULL)
    """).fetchall()
    repaired = []
    kite = None
    try:
        kite = order_manager._get_kite()
    except Exception:
        pass
    for (ticker, odate, entry, qty,
         stop, istop, sl_oid, exit_oid) in rows:
        exit_price = None
        source = "stop"
        # Try Kite order history for real fill price
        if kite:
            for oid in (exit_oid, sl_oid):
                if not oid:
                    continue
                try:
                    hist = kite.order_history(str(oid))
                    for h in reversed(hist):
                        if (h.get("status") == "COMPLETE"
                                and h.get("average_price", 0) > 0):
                            exit_price = h["average_price"]
                            source = "kite"
                            break
                except Exception:
                    pass
                if exit_price:
                    break
        if not exit_price:
            exit_price = stop or istop or 0
        if entry and qty and exit_price:
            pnl = round((exit_price - entry) * qty, 2)
            pnl_pct = round((exit_price / entry - 1) * 100, 2)
            con.execute("""
                UPDATE orders
                SET exit_price = ?, pnl = ?, pnl_pct = ?
                WHERE ticker = ? AND order_date = ?
                  AND status = 'exit_sl_breached'
            """, [exit_price, pnl, pnl_pct, ticker, odate])
            repaired.append({
                "ticker": ticker,
                "exit_price": exit_price,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "source": source,
            })
    con.close()
    return {"repaired": repaired, "count": len(repaired)}


@router.post("/api/orders/approve")
async def orders_approve(req: dict):
    """Approve a pending signal — place the order. Body: {ticker}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    result = order_manager.approve_pending(ticker)
    if result.get("status") == "ok":
        broadcast_from_thread("orders_update", order_manager.status())
    return result


@router.post("/api/orders/dismiss")
async def orders_dismiss(req: dict):
    """Dismiss a pending signal without acting. Body: {ticker}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    return order_manager.dismiss_pending(ticker)


@router.post("/api/orders/retry-failed")
async def orders_retry_failed(req: dict):
    """Retry a failed (error) order. Body: {ticker}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    if not ticker:
        return {"status": "error", "message": "ticker required"}
    result = order_manager.retry_failed_order(ticker)
    if result.get("status") == "ok":
        broadcast_from_thread("orders_update", order_manager.status())
    return result


@router.post("/api/orders/place")
async def orders_place(req: dict):
    """Manually place an order from a signal. Body: {ticker, price, trailing_stop_pct?, max_hold_days?}"""
    from faralpha.kite.kite_orders import order_manager
    ticker = req.get("ticker", "")
    price = req.get("price", 0)
    if not ticker or not price:
        return {"status": "error", "message": "ticker and price required"}
    signal = {
        "ticker": ticker,
        "price": price,
        "trailing_stop_pct": req.get("trailing_stop_pct", 0.02),
        "max_hold_days": req.get("max_hold_days", 10),
    }
    result = order_manager.place_manual_order(signal)
    if result.get("status") == "ok":
        broadcast_from_thread("orders_update", order_manager.status())
    return result
