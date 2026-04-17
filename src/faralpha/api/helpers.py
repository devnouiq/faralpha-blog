"""Pure-logic helpers: market status, position sizing, trading status, stop checks."""

from __future__ import annotations

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.api.state import table_exists


def get_market_status(market: str) -> dict:
    try:
        con = get_conn(read_only=True)
        row = con.execute(
            "SELECT MAX(date), COUNT(DISTINCT ticker), COUNT(*) FROM prices WHERE market = ?",
            [market],
        ).fetchone()

        tc = con.execute("""
            SELECT
                SUM(CASE WHEN delisting_date IS NULL OR delisting_date > CURRENT_DATE
                         THEN 1 ELSE 0 END),
                SUM(CASE WHEN delisting_date IS NOT NULL AND delisting_date <= CURRENT_DATE
                         THEN 1 ELSE 0 END)
            FROM stocks WHERE market = ?
        """, [market]).fetchone()
        n_active = int(tc[0] or 0) if tc else 0
        n_delisted = int(tc[1] or 0) if tc else 0

        regime = None
        if table_exists(con, "regime"):
            rr = con.execute(
                "SELECT date, is_bull, is_recovery, regime_strength, breadth_pct "
                "FROM regime WHERE market = ? ORDER BY date DESC LIMIT 1",
                [market],
            ).fetchone()
            if rr:
                label = "bull" if rr[1] else ("recovery" if rr[2] else "bear")
                regime = {
                    "date": str(rr[0])[:10] if rr[0] else None,
                    "regime": label,
                    "is_bull": rr[1],
                    "strength": rr[3],
                    "breadth_pct": round(rr[4] * 100, 1) if rr[4] is not None else None,
                }

        n_candidates = 0
        if table_exists(con, "candidates"):
            cr = con.execute(
                "SELECT COUNT(*) FROM candidates WHERE market = ? "
                "AND date = (SELECT MAX(date) FROM candidates WHERE market = ?)",
                [market, market],
            ).fetchone()
            n_candidates = cr[0] if cr else 0

        con.close()
        return {
            "last_price_date": str(row[0])[:10] if row[0] else None,
            "n_tickers": n_active,
            "n_active_tickers": n_active,
            "n_delisted_tickers": n_delisted,
            "n_price_rows": row[2],
            "regime": regime,
            "n_candidates_today": n_candidates,
        }
    except Exception as e:
        return {"error": str(e)}


def position_sizing(capital: float, pf: dict, entry_price: float,
                    stop_price: float, scale: float = 1.0) -> dict:
    """Risk-based position sizing (Minervini Ch.12)."""
    if capital <= 0 or entry_price <= 0 or stop_price <= 0:
        return {}
    risk_pct = pf.get("risk_per_trade_pct", 0.01)
    stop_pct = pf.get("stop_loss_pct", 0.07)
    max_pos_pct = pf.get("max_position_pct", 0.25)

    position_value = (capital * risk_pct / stop_pct) * scale
    position_value = min(position_value, capital * max_pos_pct)
    shares = int(position_value / entry_price)
    risk_amount = round(capital * risk_pct * scale, 0)

    return {
        "position_value": round(position_value, 0),
        "shares": shares,
        "risk_amount": risk_amount,
        "capital_pct": round(position_value / capital * 100, 1) if capital > 0 else 0,
    }


def compute_trading_status(
    pf: dict,
    regime: str,
    positions: list[dict],
    market: str,
    capital: float,
) -> dict:
    """Build a trading status dict for the UI."""
    held = [p for p in positions if p["market"] == market]
    n_held = len(held)

    # ── Equity drawdown status ──
    dd_enabled = pf.get("equity_dd_enabled", False)
    dd_status = {"active": False, "level": "normal"}
    if dd_enabled and capital > 0 and n_held > 0:
        total_value = sum(
            (p.get("current_price", p["entry_price"]) * p["shares"])
            for p in held
        )
        invested = sum(p["entry_price"] * p["shares"] for p in held)
        remaining_cash = max(0, capital - invested)
        portfolio_value = total_value + remaining_cash

        if portfolio_value < capital:
            dd_pct = (portfolio_value - capital) / capital
            threshold = pf.get("equity_dd_threshold", -0.08)
            floor = pf.get("equity_dd_floor", -0.20)
            if dd_pct <= threshold:
                t = min(1.0, (dd_pct - threshold) / (floor - threshold)) \
                    if floor != threshold else 1.0
                max_pos_normal = pf["max_positions"]
                max_pos_dd = pf.get("equity_dd_max_positions", 3)
                scale_dd = pf.get("equity_dd_position_scale", 0.50)
                effective_max = round(
                    max_pos_normal + t * (max_pos_dd - max_pos_normal)
                )
                effective_scale = round(
                    1.0 + t * (scale_dd - 1.0), 2
                )
                dd_status = {
                    "active": True,
                    "level": "floor" if dd_pct <= floor else "reducing",
                    "current_dd_pct": round(dd_pct * 100, 1),
                    "threshold_pct": round(threshold * 100, 1),
                    "floor_pct": round(floor * 100, 1),
                    "effective_max_positions": effective_max,
                    "effective_position_scale": effective_scale,
                    "message": (
                        f"Portfolio down {abs(dd_pct)*100:.1f}% — "
                        f"max {effective_max} positions at "
                        f"{effective_scale*100:.0f}% size"
                    ),
                }

    # ── Circuit breaker ──
    cb_enabled = pf.get("circuit_breaker_enabled", True)
    cb_status = {"active": False}
    if cb_enabled:
        closed = sorted(
            [p for p in held if p.get("pnl_pct") is not None],
            key=lambda p: p.get("entry_date", ""),
            reverse=True,
        )
        consec_losses = 0
        for p in closed:
            if (p.get("pnl_pct") or 0) < 0:
                consec_losses += 1
            else:
                break
        cb_threshold = pf.get("circuit_breaker_losses", 3)
        if consec_losses >= cb_threshold:
            pause_days = pf.get("circuit_breaker_pause_days", 20)
            cb_status = {
                "active": True,
                "consecutive_losses": consec_losses,
                "threshold": cb_threshold,
                "pause_days": pause_days,
                "message": (
                    f"{consec_losses} consecutive losses — "
                    f"pause new entries for {pause_days} trading days"
                ),
            }

    # ── Per-position guidance ──
    position_guidance = []
    for p in held:
        ticker = p["ticker"]
        entry = p["entry_price"]
        current = p.get("current_price", entry)
        highest = p.get("highest_price", entry)
        gain_pct = ((current - entry) / entry * 100) if entry > 0 else 0
        trail_pct = pf["trailing_stop_pct"]
        stop_pct = pf["stop_loss_pct"]

        hard_stop = round(entry * (1 - stop_pct), 2)
        trail_stop = round(highest * (1 - trail_pct), 2)
        active_stop = max(hard_stop, trail_stop)

        if current <= active_stop:
            action = "SELL"
            urgency = "critical"
            instruction = (
                f"SELL {ticker} at next open — "
                f"price {current:.2f} below stop {active_stop:.2f}"
            )
        elif gain_pct > 50:
            action = "HOLD"
            urgency = "info"
            instruction = (
                f"Winner +{gain_pct:.0f}% — hold with trail stop "
                f"at {trail_stop:.2f}"
            )
        elif gain_pct < -5:
            action = "HOLD"
            urgency = "warning"
            instruction = (
                f"Down {gain_pct:.1f}% — hard stop at "
                f"{hard_stop:.2f}, monitor closely"
            )
        else:
            action = "HOLD"
            urgency = "info"
            instruction = (
                f"Hold — trail stop at {trail_stop:.2f}, "
                f"hard stop at {hard_stop:.2f}"
            )

        position_guidance.append({
            "ticker": ticker,
            "action": action,
            "urgency": urgency,
            "instruction": instruction,
            "current_price": round(current, 2),
            "entry_price": round(entry, 2),
            "gain_pct": round(gain_pct, 1),
            "hard_stop": hard_stop,
            "trail_stop": trail_stop,
            "active_stop": active_stop,
        })

    urgency_order = {"critical": 0, "warning": 1, "info": 2}
    position_guidance.sort(
        key=lambda g: (urgency_order.get(g["urgency"], 9), -abs(g["gain_pct"]))
    )

    # ── Overall recommendation ──
    if regime == "bear":
        overall = "NO_TRADE"
        overall_message = (
            "Bear market — hold cash, no new positions. "
            "Monitor existing stops."
        )
    elif cb_status.get("active"):
        overall = "PAUSED"
        overall_message = cb_status["message"]
    elif dd_status.get("active"):
        overall = "REDUCED"
        overall_message = dd_status["message"]
    elif n_held >= pf["max_positions"]:
        overall = "FULL"
        overall_message = (
            f"All {pf['max_positions']} slots filled — "
            "manage existing positions"
        )
    else:
        overall = "ACTIVE"
        slots = pf["max_positions"] - n_held
        overall_message = (
            f"{slots} open slot{'s' if slots != 1 else ''} — "
            "buy top-ranked candidates at next open"
        )

    return {
        "overall": overall,
        "overall_message": overall_message,
        "regime": regime,
        "equity_dd": dd_status,
        "circuit_breaker": cb_status,
        "positions_held": n_held,
        "max_positions": pf["max_positions"],
        "position_guidance": position_guidance,
    }


def check_stops(positions: list[dict]) -> list[dict]:
    """Check tracked positions against current prices for stop breaches."""
    if not positions:
        return []
    alerts: list[dict] = []
    try:
        con = get_conn(read_only=True)
    except Exception:
        return []

    for pos in positions:
        ticker, market = pos["ticker"], pos["market"]
        entry_price = pos["entry_price"]
        highest = pos.get("highest_price", entry_price)

        try:
            row = con.execute(
                "SELECT close, high, date FROM prices "
                "WHERE ticker = ? AND market = ? ORDER BY date DESC LIMIT 1",
                [ticker, market],
            ).fetchone()
        except Exception:
            continue
        if not row:
            continue

        current_price, current_high, price_date = row[0], row[1], row[2]
        if current_high and current_high > highest:
            pos["highest_price"] = current_high
            highest = current_high

        pf = config.get_portfolio(market)
        entry_stop = entry_price * (1 - pf["stop_loss_pct"])
        trail_stop = highest * (1 - pf["trailing_stop_pct"])
        active_stop = max(entry_stop, trail_stop)

        pos["current_price"] = round(current_price, 2)
        pos["price_date"] = str(price_date)[:10]
        pos["entry_stop"] = round(entry_stop, 2)
        pos["trail_stop"] = round(trail_stop, 2)
        pos["active_stop"] = round(active_stop, 2)
        pos["pnl_pct"] = round((current_price / entry_price - 1) * 100, 2)
        pos["stop_type"] = "trailing" if trail_stop > entry_stop else "entry"
        pos["stop_distance_pct"] = round((current_price / active_stop - 1) * 100, 2) if active_stop > 0 else 0
        pos["gain_from_entry_pct"] = round((highest / entry_price - 1) * 100, 2) if entry_price > 0 else 0
        pos["trailing_stop_pct"] = pf["trailing_stop_pct"]
        pos["stop_loss_pct"] = pf["stop_loss_pct"]

        if current_price <= active_stop:
            alerts.append({
                "ticker": ticker,
                "market": market,
                "current_price": current_price,
                "stop_price": round(active_stop, 2),
                "stop_type": "trailing" if trail_stop > entry_stop else "entry",
                "loss_pct": pos["pnl_pct"],
                "action": "SELL",
            })

    con.close()
    return alerts
