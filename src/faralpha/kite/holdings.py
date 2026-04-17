"""Kite holdings & position state — single source of truth for what's sellable.

Merges kite.holdings() + kite.positions() into a unified view with:
  - deliverable_qty: settled shares that can be sold right now
  - t1_quantity: shares in T+1 settlement (visible but not sellable)
  - pending_sell_qty: shares already reserved by open SELL orders

Key principle: NEVER trust the DB alone. Always cross-check Kite before any sell.
"""

from __future__ import annotations

from faralpha.utils.logger import get_logger

log = get_logger("holdings")

# Kite order statuses
STATUS_OPEN = "OPEN"
STATUS_TRIGGER_PENDING = "TRIGGER PENDING"


def get_held_stocks(kite) -> dict[str, dict]:
    """Merge kite.positions() + kite.holdings() into {symbol: info} for CNC.

    Each entry contains:
      quantity        — total qty visible in Kite (may include unsettled T+1)
      deliverable_qty — settled shares that can actually be sold
      t1_quantity     — shares bought yesterday, pending T+1 settlement
    """
    held: dict[str, dict] = {}
    _hdetail: dict[str, dict] = {}

    # 1) holdings first — gives us settled vs T+1 breakdown
    try:
        for h in kite.holdings():
            sym = h.get("tradingsymbol", "")
            settled = h.get("quantity", 0)
            t1 = h.get("t1_quantity", 0)
            if sym and (settled > 0 or t1 > 0):
                _hdetail[sym] = {"deliverable_qty": settled, "t1_quantity": t1}
                if settled > 0:
                    held[sym] = {
                        "quantity": settled,
                        "deliverable_qty": settled,
                        "t1_quantity": t1,
                        "average_price": h.get("average_price", 0),
                        "last_price": h.get("last_price", h.get("average_price", 0)),
                        "product": "CNC",
                    }
    except Exception as e:
        log.warning("get_held: holdings() failed: %s", e)

    # 2) positions — same-day T+0 trades (supplement / update LTP)
    try:
        positions = kite.positions()
        for p in positions.get("net", []):
            if p["quantity"] > 0 and p.get("product") == "CNC":
                sym = p["tradingsymbol"]
                detail = _hdetail.get(sym, {})
                deliverable = detail.get("deliverable_qty", 0)
                t1 = detail.get("t1_quantity", 0)
                if sym in held:
                    held[sym]["last_price"] = p.get("last_price", held[sym]["last_price"])
                else:
                    held[sym] = {
                        "quantity": p["quantity"],
                        "deliverable_qty": deliverable,
                        "t1_quantity": t1,
                        "average_price": p["average_price"],
                        "last_price": p.get("last_price", p["average_price"]),
                        "product": "CNC",
                    }
    except Exception as e:
        log.warning("get_held: positions() failed: %s", e)

    return held


def get_pending_sells(kite_orders_list: list[dict]) -> dict[str, int]:
    """Compute pending sell qty per ticker from Kite orders.

    Counts OPEN / TRIGGER PENDING CNC sell orders so we know
    how many deliverable shares are already reserved.
    """
    pending: dict[str, int] = {}
    for ko in kite_orders_list:
        if (ko.get("transaction_type") == "SELL"
                and ko.get("status") in (STATUS_OPEN, STATUS_TRIGGER_PENDING)
                and ko.get("product") == "CNC"):
            sym = ko.get("tradingsymbol", "")
            pending[sym] = (
                pending.get(sym, 0)
                + ko.get("pending_quantity", ko.get("quantity", 0))
            )
    return pending


def compute_sellable_qty(
    ticker: str,
    db_filled_qty: int,
    held: dict[str, dict],
    pending_sells: dict[str, int],
) -> tuple[int, int, int]:
    """Compute how many shares we can actually sell for a ticker.

    Returns: (sellable_qty, deliverable_qty, t1_qty)
      - sellable_qty: shares we can place a sell order for right now
      - deliverable_qty: settled shares in Kite
      - t1_qty: shares in T+1 settlement
    """
    pos = held.get(ticker)
    deliverable = pos.get("deliverable_qty", pos.get("quantity", 0)) if pos else 0
    t1 = pos.get("t1_quantity", 0) if pos else 0
    pending = pending_sells.get(ticker, 0)
    sellable = max(min(db_filled_qty, deliverable - pending), 0)
    return sellable, deliverable, t1
