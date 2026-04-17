"""DuckDB persistence for the orders table.

Single source of truth for order schema, upsert, load, and row conversion.
"""

from __future__ import annotations

import json
from datetime import date

from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("db_store")

# Column list for all order queries (single source of truth)
ORDER_COLUMNS = """
    ticker, order_date, signal_price, max_entry_price,
    initial_stop, current_stop, trail_pct, max_hold_days,
    exit_date, quantity, filled_qty, avg_fill_price,
    invest_amount, risk_amount, risk_pct,
    buy_order_id, sl_order_id, exit_order_id,
    buy_status, sl_status, status,
    exit_price, pnl, pnl_pct, errors, fills
""".strip()


def ensure_table() -> None:
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


def persist_order(order_info: dict) -> None:
    """Upsert order into DuckDB orders table."""
    try:
        con = get_conn()
        order_date = order_info.get("time", "")[:10]
        ticker = order_info.get("ticker", "")
        exit_date_str = order_info.get("exit_date")
        exit_date_val = exit_date_str if exit_date_str else None

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


def load_today_orders() -> tuple[dict[str, dict], dict[str, dict]]:
    """Load today's orders + open positions from DuckDB.

    Returns: (today_orders, open_positions) both keyed by ticker.
    """
    today_orders: dict[str, dict] = {}
    open_positions: dict[str, dict] = {}

    try:
        con = get_conn()

        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'"
        ).fetchall()]
        if "orders" not in tables:
            con.close()
            return today_orders, open_positions

        today = str(date.today())

        rows = con.execute(f"""
            SELECT {ORDER_COLUMNS}
            FROM orders WHERE order_date = ?
        """, [today]).fetchall()

        for r in rows:
            order = row_to_dict(r)
            today_orders[order["ticker"]] = order

        rows = con.execute(f"""
            SELECT {ORDER_COLUMNS}
            FROM orders
            WHERE order_date < ? AND status IN ('bought', 'protected', 'UNPROTECTED')
        """, [today]).fetchall()

        for r in rows:
            order = row_to_dict(r)
            open_positions[order["ticker"]] = order

        con.close()

        if today_orders:
            log.info("Loaded %d orders from today", len(today_orders))
        if open_positions:
            log.info("Loaded %d open positions from prior days", len(open_positions))
    except Exception as e:
        log.warning("Failed to load orders from DB: %s", e)

    return today_orders, open_positions


def restore_closed_order(ticker: str) -> dict | None:
    """Find the most recent DB record for ticker with buy_order_id
    and flip it back to 'protected'. Clears SL/exit fields."""
    try:
        con = get_conn()
        row = con.execute(f"""
            SELECT {ORDER_COLUMNS}
            FROM orders
            WHERE ticker = ?
              AND buy_order_id IS NOT NULL
            ORDER BY order_date DESC LIMIT 1
        """, [ticker]).fetchone()
        if not row:
            con.close()
            return None

        order = row_to_dict(row)
        order["status"] = "protected"
        order["sl_order_id"] = None
        order["sl_status"] = None
        order["exit_order_id"] = None
        order["exit_price"] = None
        order["pnl"] = None
        order["pnl_pct"] = None

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


def row_to_dict(r) -> dict:
    """Convert a DB row tuple to the order dict format."""
    exit_date_val = str(r[8]) if r[8] else None
    return {
        "ticker": r[0],
        "time": str(r[1]),
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
